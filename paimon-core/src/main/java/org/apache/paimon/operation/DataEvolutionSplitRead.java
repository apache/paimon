/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.operation;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.append.ForceSingleBatchReader;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.format.FileFormatDiscover;
import org.apache.paimon.format.FormatKey;
import org.apache.paimon.format.FormatReaderContext;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.globalindex.IndexedSplit;
import org.apache.paimon.globalindex.IndexedSplitRecordReader;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataFilePathFactory;
import org.apache.paimon.io.DataFileRecordReader;
import org.apache.paimon.mergetree.compact.ConcatRecordReader;
import org.apache.paimon.partition.PartitionUtils;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.reader.DataEvolutionFileReader;
import org.apache.paimon.reader.DataEvolutionRow;
import org.apache.paimon.reader.FileRecordReader;
import org.apache.paimon.reader.ReaderSupplier;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypeRoot;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.FileStorePathFactory;
import org.apache.paimon.utils.FormatReaderMapping;
import org.apache.paimon.utils.FormatReaderMapping.Builder;
import org.apache.paimon.utils.Preconditions;
import org.apache.paimon.utils.Range;
import org.apache.paimon.utils.RangeHelper;
import org.apache.paimon.utils.RoaringBitmap32;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.function.ToLongFunction;

import static java.lang.String.format;
import static java.util.Collections.reverseOrder;
import static java.util.Comparator.comparingLong;
import static org.apache.paimon.format.blob.BlobFileFormat.isBlobFile;
import static org.apache.paimon.table.SpecialFields.rowTypeWithRowTracking;
import static org.apache.paimon.types.VectorType.isVectorStoreFile;
import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.paimon.utils.Preconditions.checkNotNull;

/**
 * A union {@link SplitRead} to read multiple inner files to merge columns, note that this class
 * does not support filtering push down and deletion vectors, as they can interfere with the process
 * of merging columns.
 *
 * <p>TODO: Optimize implementation of this class.
 */
public class DataEvolutionSplitRead implements SplitRead<InternalRow> {

    private final FileIO fileIO;
    private final TableSchema schema;
    private final FileFormatDiscover formatDiscover;
    private final FileStorePathFactory pathFactory;
    private final Map<FormatKey, FormatReaderMapping> formatReaderMappings;
    private final Function<Long, TableSchema> schemaFetcher;
    private final CoreOptions coreOptions;

    protected RowType readRowType;

    public DataEvolutionSplitRead(
            FileIO fileIO,
            SchemaManager schemaManager,
            TableSchema schema,
            RowType rowType,
            CoreOptions coreOptions,
            FileStorePathFactory pathFactory) {
        this.fileIO = fileIO;
        final Map<Long, TableSchema> cache = new HashMap<>();
        this.schemaFetcher =
                schemaId -> cache.computeIfAbsent(schemaId, key -> schemaManager.schema(schemaId));
        this.schema = schema;
        this.formatDiscover = FileFormatDiscover.of(coreOptions);
        this.coreOptions = coreOptions;
        this.pathFactory = pathFactory;
        this.formatReaderMappings = new HashMap<>();
        this.readRowType = rowType;
    }

    @Override
    public SplitRead<InternalRow> forceKeepDelete() {
        return this;
    }

    @Override
    public SplitRead<InternalRow> withIOManager(@Nullable IOManager ioManager) {
        return this;
    }

    @Override
    public SplitRead<InternalRow> withReadType(RowType readRowType) {
        this.readRowType = readRowType;
        return this;
    }

    @Override
    public SplitRead<InternalRow> withFilter(@Nullable Predicate predicate) {
        // TODO: Support File index push down (all conditions) and Predicate push down (only if no
        // column merge)
        return this;
    }

    @Override
    public RecordReader<InternalRow> createReader(Split split) throws IOException {
        if (split instanceof DataSplit) {
            return createReader((DataSplit) split);
        } else {
            return createReader((IndexedSplit) split);
        }
    }

    private RecordReader<InternalRow> createReader(DataSplit dataSplit) throws IOException {
        return createReader(dataSplit, null, this.readRowType);
    }

    private RecordReader<InternalRow> createReader(
            DataSplit dataSplit, List<Range> rowRanges, RowType readRowType) throws IOException {
        List<DataFileMeta> files = dataSplit.dataFiles();
        BinaryRow partition = dataSplit.partition();
        DataFilePathFactory dataFilePathFactory =
                pathFactory.createDataFilePathFactory(partition, dataSplit.bucket());
        List<ReaderSupplier<InternalRow>> suppliers = new ArrayList<>();

        Builder formatBuilder =
                new Builder(
                        formatDiscover,
                        readRowType.getFields(),
                        // file has no row id and sequence number, they are in manifest entry
                        schema ->
                                rowTypeWithRowTracking(schema.logicalRowType(), true, true)
                                        .getFields(),
                        null,
                        null,
                        null);

        List<List<DataFileMeta>> splitByRowId = mergeRangesAndSort(files);
        for (List<DataFileMeta> needMergeFiles : splitByRowId) {
            if (needMergeFiles.size() == 1 || readRowType.getFields().isEmpty()) {
                // No need to merge fields, just create a single file reader
                suppliers.add(
                        () ->
                                createFileReader(
                                        partition,
                                        dataFilePathFactory,
                                        needMergeFiles.get(0),
                                        formatBuilder,
                                        rowRanges,
                                        readRowType));

            } else {
                suppliers.add(
                        () ->
                                createUnionReader(
                                        needMergeFiles,
                                        partition,
                                        dataFilePathFactory,
                                        formatBuilder,
                                        rowRanges,
                                        readRowType));
            }
        }

        return ConcatRecordReader.create(suppliers);
    }

    private RecordReader<InternalRow> createReader(IndexedSplit indexedSplit) throws IOException {
        DataSplit dataSplit = indexedSplit.dataSplit();
        List<Range> rowRanges = indexedSplit.rowRanges();
        IndexedSplitRecordReader.Info info =
                IndexedSplitRecordReader.readInfo(this.readRowType, indexedSplit);
        return new IndexedSplitRecordReader(
                createReader(dataSplit, rowRanges, info.actualReadType), info);
    }

    private DataEvolutionFileReader createUnionReader(
            List<DataFileMeta> needMergeFiles,
            BinaryRow partition,
            DataFilePathFactory dataFilePathFactory,
            Builder formatBuilder,
            List<Range> rowRanges,
            RowType readRowType)
            throws IOException {
        List<FieldBunch> fieldsFiles =
                splitFieldBunches(
                        needMergeFiles,
                        file -> {
                            checkArgument(
                                    isBlobFile(file.fileName())
                                            || isVectorStoreFile(file.fileName()),
                                    "Only blob/vector-store files need to call this method.");
                            return schemaFetcher.apply(file.schemaId()).logicalRowType();
                        },
                        rowRanges != null);

        long rowCount = fieldsFiles.get(0).rowCount();
        long firstRowId = fieldsFiles.get(0).files().get(0).nonNullFirstRowId();

        if (rowRanges == null) {
            for (FieldBunch bunch : fieldsFiles) {
                checkArgument(
                        bunch.rowCount() == rowCount,
                        "All files in a field merge split should have the same row count.");
                checkArgument(
                        bunch.files().get(0).nonNullFirstRowId() == firstRowId,
                        "All files in a field merge split should have the same first row id and could not be null.");
            }
        }

        // Init all we need to create a compound reader
        List<DataField> allReadFields = readRowType.getFields();
        int numFields = allReadFields.size();
        int numBunches = fieldsFiles.size();
        RecordReader<InternalRow>[] fileRecordReaders = new RecordReader[numBunches];

        // Step 1: for each bunch (file), gather its (possibly nested) data row type and the set of
        // leaf field ids it physically provides. writeCols may carry nested dotted paths.
        long[] bunchSchemaId = new long[numBunches];
        List<Set<Integer>> bunchLeaves = new ArrayList<>();
        for (int i = 0; i < numBunches; i++) {
            DataFileMeta firstFile = fieldsFiles.get(i).files().get(0);
            long schemaId = firstFile.schemaId();
            bunchSchemaId[i] = schemaId;
            TableSchema dataSchema = schemaFetcher.apply(schemaId).project(firstFile.writeCols());
            RowType avail = rowTypeWithRowTracking(dataSchema.logicalRowType());
            Set<Integer> leaves = new HashSet<>();
            collectLeafIds(avail.getFields(), leaves);
            bunchLeaves.add(leaves);
        }

        // Step 2: decide, per read field, whether it is taken whole from one file or composed from
        // several files at sub-field granularity. Files are already sorted latest-first, so the
        // first bunch providing a leaf wins (latest-wins semantics, now at sub-field level).
        // selection per bunch: topFieldId -> null (whole) or set of selected sub-field ids
        List<Map<Integer, Set<Integer>>> bunchSelection = new ArrayList<>();
        for (int i = 0; i < numBunches; i++) {
            bunchSelection.add(new LinkedHashMap<>());
        }

        int[] rowOffsets = new int[numFields];
        int[] fieldOffsets = new int[numFields];
        Arrays.fill(rowOffsets, -1);
        Arrays.fill(fieldOffsets, -1);
        DataEvolutionRow.NestedField[] nested = new DataEvolutionRow.NestedField[numFields];
        boolean[] composite = new boolean[numFields];
        int[] wholeBunch = new int[numFields];
        Arrays.fill(wholeBunch, -1);

        for (int j = 0; j < numFields; j++) {
            DataField rf = allReadFields.get(j);
            List<Integer> leaves = leafIdsOf(rf);
            Map<Integer, Integer> leafProvider = new HashMap<>();
            Set<Integer> providers = new HashSet<>();
            for (int leaf : leaves) {
                int p = providerOf(leaf, bunchLeaves);
                if (p >= 0) {
                    leafProvider.put(leaf, p);
                    providers.add(p);
                }
            }
            if (providers.isEmpty()) {
                // no file provides this field; it stays null (nullability checked below)
                continue;
            }
            // Only read a field whole from a single file when that file covers ALL of its leaves.
            // If a single file provides only some leaves of a struct (the rest absent everywhere),
            // we must prune to the provided sub-fields so the reader is not asked for sub-fields
            // the
            // file does not physically contain; the missing ones stay null via the composite plan.
            boolean allLeavesCovered = leafProvider.size() == leaves.size();
            if (providers.size() == 1 && allLeavesCovered) {
                int b = providers.iterator().next();
                bunchSelection.get(b).put(rf.id(), null);
                wholeBunch[j] = b;
            } else {
                checkArgument(
                        rf.type() instanceof RowType,
                        "Field %s is split across files but is not a struct.",
                        rf.name());
                composite[j] = true;
                for (DataField sub : ((RowType) rf.type()).getFields()) {
                    List<Integer> subLeaves = leafIdsOf(sub);
                    Set<Integer> subProviders = new HashSet<>();
                    int coveredSubLeaves = 0;
                    for (int leaf : subLeaves) {
                        int p = leafProvider.getOrDefault(leaf, -1);
                        if (p >= 0) {
                            subProviders.add(p);
                            coveredSubLeaves++;
                        }
                    }
                    if (subProviders.size() > 1) {
                        throw new UnsupportedOperationException(
                                "Sub-field-level data evolution does not yet support splitting a "
                                        + "nested sub-field ("
                                        + rf.name()
                                        + "."
                                        + sub.name()
                                        + ") across multiple files.");
                    }
                    if (subProviders.size() == 1) {
                        if (sub.type() instanceof RowType && coveredSubLeaves < subLeaves.size()) {
                            // the single provider holds only part of this nested sub-struct;
                            // reading
                            // it whole would request leaves it lacks, and one-level composition
                            // cannot prune deeper than this level yet
                            throw new UnsupportedOperationException(
                                    "Sub-field-level data evolution does not yet support reading a "
                                            + "partially-written nested sub-field ("
                                            + rf.name()
                                            + "."
                                            + sub.name()
                                            + ") deeper than one level.");
                        }
                        int b = subProviders.iterator().next();
                        bunchSelection
                                .get(b)
                                .computeIfAbsent(rf.id(), k -> new LinkedHashSet<>())
                                .add(sub.id());
                    }
                    // else: sub-field absent everywhere -> stays null
                }
            }
        }

        // Step 3: materialize each bunch's partial read row type and the offset maps.
        List<List<DataField>> bunchReadFields = new ArrayList<>();
        List<Map<Integer, Integer>> bunchTopOffset = new ArrayList<>();
        List<Map<Integer, Map<Integer, Integer>>> bunchSubOffset = new ArrayList<>();
        for (int i = 0; i < numBunches; i++) {
            Map<Integer, Set<Integer>> sel = bunchSelection.get(i);
            List<DataField> readFields = new ArrayList<>();
            Map<Integer, Integer> topOffset = new HashMap<>();
            Map<Integer, Map<Integer, Integer>> subOffset = new HashMap<>();
            for (Map.Entry<Integer, Set<Integer>> e : sel.entrySet()) {
                int topId = e.getKey();
                Set<Integer> subs = e.getValue();
                DataField readTop = readRowType.getField(topId);
                if (subs == null) {
                    readFields.add(readTop);
                    topOffset.put(topId, readFields.size() - 1);
                } else {
                    RowType readStruct = (RowType) readTop.type();
                    List<DataField> chosen = new ArrayList<>();
                    Map<Integer, Integer> subToIdx = new HashMap<>();
                    for (DataField s : readStruct.getFields()) {
                        if (subs.contains(s.id())) {
                            subToIdx.put(s.id(), chosen.size());
                            chosen.add(s);
                        }
                    }
                    RowType partial = new RowType(readStruct.isNullable(), chosen);
                    readFields.add(readTop.newType(partial));
                    topOffset.put(topId, readFields.size() - 1);
                    subOffset.put(topId, subToIdx);
                }
            }
            bunchReadFields.add(readFields);
            bunchTopOffset.add(topOffset);
            bunchSubOffset.add(subOffset);
        }

        // Step 4: wire output offsets (whole fields) and nested composition plans (split structs).
        for (int j = 0; j < numFields; j++) {
            DataField rf = allReadFields.get(j);
            if (composite[j]) {
                List<DataField> subFields = ((RowType) rf.type()).getFields();
                int subCount = subFields.size();
                int[] subRowOffsets = new int[subCount];
                int[] subFieldOffsets = new int[subCount];
                Arrays.fill(subRowOffsets, -1);
                Arrays.fill(subFieldOffsets, -1);
                Map<Integer, Integer> bunchToPartial = new LinkedHashMap<>();
                List<int[]> partials = new ArrayList<>();
                for (int s = 0; s < subCount; s++) {
                    int subId = subFields.get(s).id();
                    int b = findSubProvider(rf.id(), subId, bunchSubOffset);
                    if (b < 0) {
                        // no file provides this sub-field; it stays null, so it must be nullable
                        checkArgument(
                                subFields.get(s).type().isNullable(),
                                "Sub-field %s.%s is not null but can't find any file contains it.",
                                rf.name(),
                                subFields.get(s).name());
                        continue;
                    }
                    Integer pIdx = bunchToPartial.get(b);
                    if (pIdx == null) {
                        int topOff = bunchTopOffset.get(b).get(rf.id());
                        int size = bunchSubOffset.get(b).get(rf.id()).size();
                        pIdx = partials.size();
                        bunchToPartial.put(b, pIdx);
                        partials.add(new int[] {b, topOff, size});
                    }
                    subRowOffsets[s] = pIdx;
                    subFieldOffsets[s] = bunchSubOffset.get(b).get(rf.id()).get(subId);
                }
                int p = partials.size();
                int[] pr = new int[p];
                int[] po = new int[p];
                int[] ps = new int[p];
                for (int k = 0; k < p; k++) {
                    pr[k] = partials.get(k)[0];
                    po[k] = partials.get(k)[1];
                    ps[k] = partials.get(k)[2];
                }
                nested[j] =
                        new DataEvolutionRow.NestedField(
                                pr, po, ps, subRowOffsets, subFieldOffsets);
            } else if (wholeBunch[j] >= 0) {
                int b = wholeBunch[j];
                rowOffsets[j] = b;
                fieldOffsets[j] = bunchTopOffset.get(b).get(rf.id());
            }
        }

        // Step 5: build the per-bunch readers from the materialized partial read row types.
        for (int i = 0; i < numBunches; i++) {
            List<DataField> readFields = bunchReadFields.get(i);
            if (readFields.isEmpty()) {
                fileRecordReaders[i] = null;
                continue;
            }
            FieldBunch bunch = fieldsFiles.get(i);
            DataFileMeta firstFile = bunch.files().get(0);
            String formatIdentifier = DataFilePathFactory.formatIdentifier(firstFile.fileName());
            long schemaId = bunchSchemaId[i];
            TableSchema dataSchema = schemaFetcher.apply(schemaId).project(firstFile.writeCols());
            RowType partialReadRowType = new RowType(readFields);
            // cache key must use paths relative to the full schema so that two files reading
            // different sub-fields of the same struct (e.g. nest.a vs nest.b) do not collide
            RowType fullRef =
                    rowTypeWithRowTracking(schemaFetcher.apply(schemaId).logicalRowType());
            List<String> readFieldNames = partialReadRowType.leafPaths(fullRef);
            FormatReaderMapping formatReaderMapping =
                    formatReaderMappings.computeIfAbsent(
                            new FormatKey(schemaId, formatIdentifier, readFieldNames),
                            key ->
                                    formatBuilder.build(
                                            formatIdentifier,
                                            schema,
                                            dataSchema,
                                            readFields,
                                            false));
            fileRecordReaders[i] =
                    new ForceSingleBatchReader(
                            createFieldBunchReader(
                                    partition,
                                    bunch,
                                    dataFilePathFactory,
                                    formatReaderMapping,
                                    rowRanges,
                                    partialReadRowType));
        }

        for (int j = 0; j < numFields; j++) {
            if (rowOffsets[j] == -1 && nested[j] == null) {
                checkArgument(
                        allReadFields.get(j).type().isNullable(),
                        format(
                                "Field %s is not null but can't find any file contains it.",
                                allReadFields.get(j)));
            }
        }

        return new DataEvolutionFileReader(rowOffsets, fieldOffsets, fileRecordReaders, nested);
    }

    /** Collect (recursively) the leaf field ids of {@code fields}; only ROW types recurse. */
    private static void collectLeafIds(List<DataField> fields, java.util.Collection<Integer> out) {
        for (DataField f : fields) {
            if (f.type() instanceof RowType) {
                collectLeafIds(((RowType) f.type()).getFields(), out);
            } else {
                out.add(f.id());
            }
        }
    }

    private static List<Integer> leafIdsOf(DataField field) {
        List<Integer> result = new ArrayList<>();
        collectLeafIds(Collections.singletonList(field), result);
        return result;
    }

    private static int providerOf(int leafId, List<Set<Integer>> bunchLeaves) {
        for (int i = 0; i < bunchLeaves.size(); i++) {
            if (bunchLeaves.get(i).contains(leafId)) {
                return i;
            }
        }
        return -1;
    }

    private static int findSubProvider(
            int topId, int subId, List<Map<Integer, Map<Integer, Integer>>> bunchSubOffset) {
        for (int b = 0; b < bunchSubOffset.size(); b++) {
            Map<Integer, Integer> sm = bunchSubOffset.get(b).get(topId);
            if (sm != null && sm.containsKey(subId)) {
                return b;
            }
        }
        return -1;
    }

    private RecordReader<InternalRow> createFieldBunchReader(
            BinaryRow partition,
            FieldBunch bunch,
            DataFilePathFactory dataFilePathFactory,
            FormatReaderMapping formatReaderMapping,
            List<Range> rowRanges,
            RowType readRowType)
            throws IOException {
        if (bunch instanceof DataBunch) {
            // for data bunch, directly read the single file
            return createFileReader(
                    partition,
                    bunch.files().get(0),
                    dataFilePathFactory,
                    formatReaderMapping,
                    rowRanges,
                    readRowType);
        } else if (bunch instanceof VectorFileBunch) {
            // for vector bunch, sequential read all data files and concat them
            return sequentialReadFiles(
                    bunch.files(), partition, dataFilePathFactory, formatReaderMapping, rowRanges);
        } else if (bunch instanceof BlobFileBunch) {
            // for blob bunch, fallback on placeholders

            // fast path: only contains one max_seq group
            if (((BlobFileBunch) bunch).sequentialReadOptimize()) {
                return sequentialReadFiles(
                        bunch.files(),
                        partition,
                        dataFilePathFactory,
                        formatReaderMapping,
                        rowRanges);
            }
            int blobIndex = findBlobFieldIndex(readRowType);
            checkArgument(blobIndex >= 0, "Blob bunch read type should contain a blob field.");
            return new BlobFallbackRecordReader(
                    bunch.files(),
                    file ->
                            createFileReader(
                                    partition,
                                    file,
                                    dataFilePathFactory,
                                    formatReaderMapping,
                                    rowRanges,
                                    readRowType),
                    rowRanges,
                    readRowType,
                    blobIndex);
        } else {
            throw new UnsupportedOperationException("Unsupported bunch type: " + bunch);
        }
    }

    private RecordReader<InternalRow> sequentialReadFiles(
            List<DataFileMeta> files,
            BinaryRow partition,
            DataFilePathFactory dataFilePathFactory,
            FormatReaderMapping formatReaderMapping,
            List<Range> rowRanges)
            throws IOException {
        List<ReaderSupplier<InternalRow>> readerSuppliers = new ArrayList<>();
        for (DataFileMeta file : files) {
            RoaringBitmap32 selection = file.toFileSelection(rowRanges);
            FormatReaderContext formatReaderContext =
                    new FormatReaderContext(
                            fileIO, dataFilePathFactory.toPath(file), file.fileSize(), selection);
            readerSuppliers.add(
                    () ->
                            new DataFileRecordReader(
                                    readRowType,
                                    formatReaderMapping.getReaderFactory(),
                                    formatReaderContext,
                                    coreOptions.scanIgnoreCorruptFile(),
                                    coreOptions.scanIgnoreLostFile(),
                                    formatReaderMapping.getIndexMapping(),
                                    formatReaderMapping.getCastMapping(),
                                    PartitionUtils.create(
                                            formatReaderMapping.getPartitionPair(), partition),
                                    true,
                                    file.firstRowId(),
                                    file.maxSequenceNumber(),
                                    formatReaderMapping.getSystemFields()));
        }
        return ConcatRecordReader.create(readerSuppliers);
    }

    private static int findBlobFieldIndex(RowType rowType) {
        for (int i = 0; i < rowType.getFieldCount(); i++) {
            if (rowType.getTypeAt(i).getTypeRoot() == DataTypeRoot.BLOB) {
                return i;
            }
        }
        return -1;
    }

    private FileRecordReader<InternalRow> createFileReader(
            BinaryRow partition,
            DataFilePathFactory dataFilePathFactory,
            DataFileMeta file,
            Builder formatBuilder,
            List<Range> rowRanges,
            RowType readRowType)
            throws IOException {
        String formatIdentifier = DataFilePathFactory.formatIdentifier(file.fileName());
        long schemaId = file.schemaId();
        FormatReaderMapping formatReaderMapping =
                formatReaderMappings.computeIfAbsent(
                        new FormatKey(file.schemaId(), formatIdentifier),
                        key ->
                                formatBuilder.build(
                                        formatIdentifier,
                                        schema,
                                        schemaId == schema.id()
                                                ? schema
                                                : schemaFetcher.apply(schemaId)));
        return createFileReader(
                partition, file, dataFilePathFactory, formatReaderMapping, rowRanges, readRowType);
    }

    private FileRecordReader<InternalRow> createFileReader(
            BinaryRow partition,
            DataFileMeta file,
            DataFilePathFactory dataFilePathFactory,
            FormatReaderMapping formatReaderMapping,
            List<Range> rowRanges,
            RowType readRowType)
            throws IOException {
        RoaringBitmap32 selection = file.toFileSelection(rowRanges);
        FormatReaderContext formatReaderContext =
                new FormatReaderContext(
                        fileIO, dataFilePathFactory.toPath(file), file.fileSize(), selection);
        return new DataFileRecordReader(
                readRowType,
                formatReaderMapping.getReaderFactory(),
                formatReaderContext,
                coreOptions.scanIgnoreCorruptFile(),
                coreOptions.scanIgnoreLostFile(),
                formatReaderMapping.getIndexMapping(),
                formatReaderMapping.getCastMapping(),
                PartitionUtils.create(formatReaderMapping.getPartitionPair(), partition),
                true,
                file.firstRowId(),
                file.maxSequenceNumber(),
                formatReaderMapping.getSystemFields());
    }

    @VisibleForTesting
    public static List<FieldBunch> splitFieldBunches(
            List<DataFileMeta> needMergeFiles, Function<DataFileMeta, RowType> fileToRowType) {
        return splitFieldBunches(needMergeFiles, fileToRowType, false);
    }

    @VisibleForTesting
    public static List<FieldBunch> splitFieldBunches(
            List<DataFileMeta> needMergeFiles,
            Function<DataFileMeta, RowType> fileToRowType,
            boolean rowIdPushDown) {
        List<FieldBunch> fieldsFiles = new ArrayList<>();
        Map<Integer, BlobFileBunch> blobBunchMap = new HashMap<>();
        Map<VectorStoreBunchKey, VectorFileBunch> vectorStoreBunchMap = new TreeMap<>();
        long rowCount = -1;
        for (DataFileMeta file : needMergeFiles) {
            if (isBlobFile(file.fileName())) {
                RowType rowType = fileToRowType.apply(file);
                int fieldId = rowType.getField(file.writeCols().get(0)).id();
                final long expectedRowCount = rowCount;
                blobBunchMap
                        .computeIfAbsent(
                                fieldId, key -> new BlobFileBunch(expectedRowCount, rowIdPushDown))
                        .add(file);
            } else if (isVectorStoreFile(file.fileName())) {
                RowType rowType = fileToRowType.apply(file);
                String fileFormat = DataFilePathFactory.formatIdentifier(file.fileName());
                VectorStoreBunchKey vectorStoreKey =
                        new VectorStoreBunchKey(
                                file.schemaId(), fileFormat, file.writeCols(), rowType);
                final long expectedRowCount = rowCount;
                vectorStoreBunchMap
                        .computeIfAbsent(
                                vectorStoreKey,
                                key -> new VectorFileBunch(expectedRowCount, rowIdPushDown))
                        .add(file);
            } else {
                // Normal file, just add it to the current merge split
                fieldsFiles.add(new DataBunch(file));
                rowCount = file.rowCount();
            }
        }
        fieldsFiles.addAll(blobBunchMap.values());
        fieldsFiles.addAll(vectorStoreBunchMap.values());
        return fieldsFiles;
    }

    /** Files for partial field. */
    public interface FieldBunch {

        long rowCount();

        List<DataFileMeta> files();
    }

    private static class DataBunch implements FieldBunch {

        private final DataFileMeta dataFile;

        private DataBunch(DataFileMeta dataFile) {
            this.dataFile = dataFile;
        }

        @Override
        public long rowCount() {
            return dataFile.rowCount();
        }

        @Override
        public List<DataFileMeta> files() {
            return Collections.singletonList(dataFile);
        }
    }

    /**
     * The {@link FieldBunch} for blobs. Compared to {@link VectorFileBunch} which only contains
     * data files of the max max_seq number, {@link BlobFileBunch} contains all blob files.
     */
    @VisibleForTesting
    static class BlobFileBunch implements FieldBunch {

        final List<DataFileMeta> files;
        final List<Range> ranges;
        final long expectedRowCount;
        final boolean rowIdPushdown;

        BlobFileBunch(long expectedRowCount, boolean rowIdPushdown) {
            this.files = new ArrayList<>();
            this.expectedRowCount = expectedRowCount;
            this.ranges = new ArrayList<>();
            this.rowIdPushdown = rowIdPushdown;
        }

        void add(DataFileMeta file) {
            if (!isBlobFile(file.fileName())) {
                throw new IllegalArgumentException("Only blob file can be added to this bunch.");
            }
            if (!files.isEmpty()) {
                checkArgument(
                        file.writeCols().equals(files.get(0).writeCols()),
                        "All files in this bunch should have the same write columns.");
            }

            files.add(file);
            ranges.add(file.nonNullRowIdRange());
        }

        @Override
        public long rowCount() {
            List<Range> merged = Range.sortAndMergeOverlap(ranges, true);
            if (!rowIdPushdown) {
                Preconditions.checkState(
                        merged.size() == 1,
                        "Blob file bunch should always contain a contiguous row range.");

                long rowCount = merged.get(0).count();
                if (expectedRowCount >= 0) {
                    Preconditions.checkState(
                            rowCount == expectedRowCount,
                            "The merged rowCount %s of blob file bunch should be aligned with normal files %s.",
                            rowCount,
                            expectedRowCount);
                }
            }

            return merged.stream().mapToLong(Range::count).sum();
        }

        public boolean sequentialReadOptimize() {
            Preconditions.checkState(!files.isEmpty(), "Blob file bunch should not be empty.");

            // If blob files share the same max_seq_num, we could sequentially read them.
            // Files have already been sorted by first_row_id
            long maxSeq = files.get(0).maxSequenceNumber();
            for (int i = 1; i < files.size(); i++) {
                if (files.get(i).maxSequenceNumber() != maxSeq) {
                    return false;
                }
            }

            return true;
        }

        @Override
        public List<DataFileMeta> files() {
            return files;
        }
    }

    /** {@link FieldBunch} for vector-store files. */
    @VisibleForTesting
    static class VectorFileBunch implements FieldBunch {

        final List<DataFileMeta> files;
        final long expectedRowCount;
        final boolean rowIdPushDown;

        long latestFistRowId = -1;
        long expectedNextFirstRowId = -1;
        long latestMaxSequenceNumber = -1;
        long rowCount;

        VectorFileBunch(long expectedRowCount, boolean rowIdPushDown) {
            this.files = new ArrayList<>();
            this.expectedRowCount = expectedRowCount;
            this.rowIdPushDown = rowIdPushDown;
        }

        void add(DataFileMeta file) {
            if (!isVectorStoreFile(file.fileName())) {
                throw new IllegalArgumentException(
                        "Only vector-store file can be added to this bunch.");
            }
            if (file.nonNullFirstRowId() == latestFistRowId) {
                if (file.maxSequenceNumber() >= latestMaxSequenceNumber) {
                    throw new IllegalArgumentException(
                            "Vector file with same first row id should have decreasing sequence number.");
                }
                return;
            }

            if (!files.isEmpty()) {
                long firstRowId = file.nonNullFirstRowId();
                if (rowIdPushDown && firstRowId < expectedNextFirstRowId) {
                    if (file.maxSequenceNumber() > latestMaxSequenceNumber) {
                        DataFileMeta lastFile = files.remove(files.size() - 1);
                        rowCount -= lastFile.rowCount();
                    } else {
                        return;
                    }
                } else if (firstRowId < expectedNextFirstRowId) {
                    checkArgument(
                            file.maxSequenceNumber() < latestMaxSequenceNumber,
                            "Vector file with overlapping row id should have decreasing sequence number.");
                    return;
                } else if (!rowIdPushDown && firstRowId > expectedNextFirstRowId) {
                    throw new IllegalArgumentException(
                            "Vector file first row id should be continuous, expect "
                                    + expectedNextFirstRowId
                                    + " but got "
                                    + firstRowId);
                }

                if (!files.isEmpty()) {
                    checkArgument(
                            file.schemaId() == files.get(0).schemaId(),
                            "All files in this bunch should have the same schema id.");
                    checkArgument(
                            file.writeCols().equals(files.get(0).writeCols()),
                            "All files in this bunch should have the same write columns.");
                }
            }

            files.add(file);
            rowCount += file.rowCount();
            if (expectedRowCount > 0) {
                checkArgument(
                        rowCount <= expectedRowCount,
                        "Vector files row count exceed the expect " + expectedRowCount);
            }
            latestMaxSequenceNumber = file.maxSequenceNumber();
            latestFistRowId = file.nonNullFirstRowId();
            expectedNextFirstRowId = latestFistRowId + file.rowCount();
        }

        @Override
        public long rowCount() {
            return rowCount;
        }

        @Override
        public List<DataFileMeta> files() {
            return files;
        }
    }

    public static List<List<DataFileMeta>> mergeRangesAndSort(List<DataFileMeta> files) {
        // group by row id range
        ToLongFunction<DataFileMeta> maxSeqF = DataFileMeta::maxSequenceNumber;
        RangeHelper<DataFileMeta> rangeHelper = new RangeHelper<>(DataFileMeta::nonNullRowIdRange);
        List<List<DataFileMeta>> result = rangeHelper.mergeOverlappingRanges(files);

        // in group, sort by blob/vector-store file and max_seq
        for (List<DataFileMeta> group : result) {
            // split to data files, blob files, vector-store files
            List<DataFileMeta> dataFiles = new ArrayList<>();
            List<DataFileMeta> blobFiles = new ArrayList<>();
            List<DataFileMeta> vectorStoreFiles = new ArrayList<>();
            for (DataFileMeta f : group) {
                if (isBlobFile(f.fileName())) {
                    blobFiles.add(f);
                } else if (isVectorStoreFile(f.fileName())) {
                    vectorStoreFiles.add(f);
                } else {
                    dataFiles.add(f);
                }
            }

            // data files sort by reversed max sequence number
            dataFiles.sort(comparingLong(maxSeqF).reversed());
            checkArgument(
                    rangeHelper.areAllRangesSame(dataFiles),
                    "Data files %s should be all row id ranges same.",
                    dataFiles);

            // blob files sort by first row id then by reversed max sequence number
            blobFiles.sort(
                    comparingLong(DataFileMeta::nonNullFirstRowId)
                            .thenComparing(reverseOrder(comparingLong(maxSeqF))));

            // vector-store files sort by first row id then by reversed max sequence number
            vectorStoreFiles.sort(
                    comparingLong(DataFileMeta::nonNullFirstRowId)
                            .thenComparing(reverseOrder(comparingLong(maxSeqF))));

            // concat data files, blob files, vector-store files
            group.clear();
            group.addAll(dataFiles);
            group.addAll(blobFiles);
            group.addAll(vectorStoreFiles);
        }

        return result;
    }

    static final class VectorStoreBunchKey implements Comparable<VectorStoreBunchKey> {
        public final long schemaId;
        public final String formatIdentifier;
        public final List<String> writeCols;

        public VectorStoreBunchKey(
                long schemaId,
                String formatIdentifier,
                List<String> writeCols,
                RowType preferredColOrder) {
            this.schemaId = schemaId;
            this.formatIdentifier = checkNotNull(formatIdentifier, "formatIdentifier");
            this.writeCols = normalizeWriteCols(writeCols, preferredColOrder);
        }

        @Override
        public int compareTo(VectorStoreBunchKey o) {
            int c = Long.compare(this.schemaId, o.schemaId);
            if (c != 0) {
                return c;
            }

            c = this.formatIdentifier.compareTo(o.formatIdentifier);
            if (c != 0) {
                return c;
            }

            int n = Math.min(this.writeCols.size(), o.writeCols.size());
            for (int i = 0; i < n; i++) {
                c = this.writeCols.get(i).compareTo(o.writeCols.get(i));
                if (c != 0) {
                    return c;
                }
            }
            return Integer.compare(this.writeCols.size(), o.writeCols.size());
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof VectorStoreBunchKey)) {
                return false;
            }
            VectorStoreBunchKey that = (VectorStoreBunchKey) o;
            return schemaId == that.schemaId
                    && formatIdentifier.equals(that.formatIdentifier)
                    && writeCols.equals(that.writeCols);
        }

        @Override
        public int hashCode() {
            return Objects.hash(schemaId, formatIdentifier, writeCols);
        }

        @Override
        public String toString() {
            return "VectorStoreBunchKey{schemaId="
                    + schemaId
                    + ", format="
                    + formatIdentifier
                    + ", writeCols="
                    + writeCols
                    + "}";
        }

        private static List<String> normalizeWriteCols(List<String> writeCols, RowType rowType) {
            if (writeCols == null || writeCols.isEmpty()) {
                return Collections.emptyList();
            }

            Map<String, Integer> colPosMap = new HashMap<>();
            List<String> namesInRowType = rowType.getFieldNames();
            for (int i = 0; i < namesInRowType.size(); i++) {
                colPosMap.putIfAbsent(namesInRowType.get(i), i);
            }

            ArrayList<String> sorted = new ArrayList<>(writeCols);
            sorted.sort(
                    (a, b) -> {
                        int ia = colPosMap.getOrDefault(a, Integer.MAX_VALUE);
                        int ib = colPosMap.getOrDefault(b, Integer.MAX_VALUE);
                        if (ia != ib) {
                            return Integer.compare(ia, ib);
                        }
                        return a.compareTo(b);
                    });

            return sorted;
        }
    }
}
