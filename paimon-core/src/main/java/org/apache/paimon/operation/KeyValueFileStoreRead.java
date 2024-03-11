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
import org.apache.paimon.KeyValue;
import org.apache.paimon.KeyValueFileStore;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.deletionvectors.ApplyDeletionVectorReader;
import org.apache.paimon.deletionvectors.DeletionVector;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.KeyValueFileReaderFactory;
import org.apache.paimon.mergetree.DropDeleteReader;
import org.apache.paimon.mergetree.MergeSorter;
import org.apache.paimon.mergetree.MergeTreeReaders;
import org.apache.paimon.mergetree.SortedRun;
import org.apache.paimon.mergetree.compact.ConcatRecordReader;
import org.apache.paimon.mergetree.compact.ConcatRecordReader.ReaderSupplier;
import org.apache.paimon.mergetree.compact.IntervalPartition;
import org.apache.paimon.mergetree.compact.MergeFunctionFactory;
import org.apache.paimon.mergetree.compact.MergeFunctionFactory.AdjustedProjection;
import org.apache.paimon.mergetree.compact.MergeFunctionWrapper;
import org.apache.paimon.mergetree.compact.ReducerMergeFunctionWrapper;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.DeletionFile;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.FieldsComparator;
import org.apache.paimon.utils.ProjectedRow;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.paimon.io.DataFilePathFactory.CHANGELOG_FILE_PREFIX;
import static org.apache.paimon.predicate.PredicateBuilder.containsFields;
import static org.apache.paimon.predicate.PredicateBuilder.splitAnd;

/** {@link FileStoreRead} implementation for {@link KeyValueFileStore}. */
public class KeyValueFileStoreRead implements FileStoreRead<KeyValue> {

    private final TableSchema tableSchema;
    private final FileIO fileIO;
    private final KeyValueFileReaderFactory.Builder readerFactoryBuilder;
    private final Comparator<InternalRow> keyComparator;
    private final MergeFunctionFactory<KeyValue> mfFactory;
    private final MergeSorter mergeSorter;
    @Nullable private final FieldsComparator userDefinedSeqComparator;

    @Nullable private int[][] keyProjectedFields;

    @Nullable private List<Predicate> filtersForKeys;

    @Nullable private List<Predicate> filtersForAll;

    @Nullable private int[][] pushdownProjection;
    @Nullable private int[][] outerProjection;

    private boolean forceKeepDelete = false;

    public KeyValueFileStoreRead(
            SchemaManager schemaManager,
            long schemaId,
            RowType keyType,
            RowType valueType,
            Comparator<InternalRow> keyComparator,
            @Nullable FieldsComparator userDefinedSeqComparator,
            MergeFunctionFactory<KeyValue> mfFactory,
            KeyValueFileReaderFactory.Builder readerFactoryBuilder) {
        this.tableSchema = schemaManager.schema(schemaId);
        this.readerFactoryBuilder = readerFactoryBuilder;
        this.fileIO = readerFactoryBuilder.fileIO();
        this.keyComparator = keyComparator;
        this.mfFactory = mfFactory;
        this.userDefinedSeqComparator = userDefinedSeqComparator;
        this.mergeSorter =
                new MergeSorter(
                        CoreOptions.fromMap(tableSchema.options()), keyType, valueType, null);
    }

    public KeyValueFileStoreRead withKeyProjection(int[][] projectedFields) {
        readerFactoryBuilder.withKeyProjection(projectedFields);
        this.keyProjectedFields = projectedFields;
        return this;
    }

    public KeyValueFileStoreRead withValueProjection(int[][] projectedFields) {
        AdjustedProjection projection = mfFactory.adjustProjection(projectedFields);
        this.pushdownProjection = projection.pushdownProjection;
        this.outerProjection = projection.outerProjection;
        if (pushdownProjection != null) {
            readerFactoryBuilder.withValueProjection(pushdownProjection);
            mergeSorter.setProjectedValueType(readerFactoryBuilder.projectedValueType());
        }
        return this;
    }

    public KeyValueFileStoreRead withIOManager(IOManager ioManager) {
        this.mergeSorter.setIOManager(ioManager);
        return this;
    }

    public KeyValueFileStoreRead forceKeepDelete() {
        this.forceKeepDelete = true;
        return this;
    }

    @Override
    public FileStoreRead<KeyValue> withFilter(Predicate predicate) {
        List<Predicate> allFilters = new ArrayList<>();
        List<Predicate> pkFilters = null;
        List<String> primaryKeys = tableSchema.trimmedPrimaryKeys();
        Set<String> nonPrimaryKeys =
                tableSchema.fieldNames().stream()
                        .filter(name -> !primaryKeys.contains(name))
                        .collect(Collectors.toSet());
        for (Predicate sub : splitAnd(predicate)) {
            allFilters.add(sub);
            if (!containsFields(sub, nonPrimaryKeys)) {
                if (pkFilters == null) {
                    pkFilters = new ArrayList<>();
                }
                // TODO Actually, the index is wrong, but it is OK.
                //  The orc filter just use name instead of index.
                pkFilters.add(sub);
            }
        }
        // Consider this case:
        // Denote (seqNumber, key, value) as a record. We have two overlapping runs in a section:
        //   * First run: (1, k1, 100), (2, k2, 200)
        //   * Second run: (3, k1, 10), (4, k2, 20)
        // If we push down filter "value >= 100" for this section, only the first run will be read,
        // and the second run is lost. This will produce incorrect results.
        //
        // So for sections with overlapping runs, we only push down key filters.
        // For sections with only one run, as each key only appears once, it is OK to push down
        // value filters.
        filtersForAll = allFilters;
        filtersForKeys = pkFilters;
        return this;
    }

    @Override
    public RecordReader<KeyValue> createReader(DataSplit split) throws IOException {
        RecordReader<KeyValue> reader = createReaderWithoutOuterProjection(split);
        if (outerProjection != null) {
            ProjectedRow projectedRow = ProjectedRow.from(outerProjection);
            reader = reader.transform(kv -> kv.replaceValue(projectedRow.replaceRow(kv.value())));
        }
        return reader;
    }

    private RecordReader<KeyValue> createReaderWithoutOuterProjection(DataSplit split)
            throws IOException {
        ReaderSupplier<KeyValue> beforeSupplier = null;
        if (split.beforeFiles().size() > 0) {
            if (split.isStreaming() || split.beforeDeletionFiles().isPresent()) {
                beforeSupplier =
                        () ->
                                new ReverseReader(
                                        noMergeRead(
                                                split.partition(),
                                                split.bucket(),
                                                split.beforeFiles(),
                                                split.beforeDeletionFiles().orElse(null),
                                                split.isStreaming()));
            } else {
                beforeSupplier =
                        () ->
                                mergeRead(
                                        split.partition(),
                                        split.bucket(),
                                        split.beforeFiles(),
                                        false);
            }
        }

        ReaderSupplier<KeyValue> dataSupplier;
        if (split.isStreaming() || split.deletionFiles().isPresent()) {
            dataSupplier =
                    () ->
                            noMergeRead(
                                    split.partition(),
                                    split.bucket(),
                                    split.dataFiles(),
                                    split.deletionFiles().orElse(null),
                                    split.isStreaming());
        } else {
            dataSupplier =
                    () ->
                            mergeRead(
                                    split.partition(),
                                    split.bucket(),
                                    split.dataFiles(),
                                    split.beforeFiles().isEmpty() && forceKeepDelete);
        }

        if (split.isStreaming()) {
            return beforeSupplier == null
                    ? dataSupplier.get()
                    : ConcatRecordReader.create(Arrays.asList(beforeSupplier, dataSupplier));
        } else {
            return beforeSupplier == null
                    ? dataSupplier.get()
                    : DiffReader.readDiff(
                            beforeSupplier.get(),
                            dataSupplier.get(),
                            keyComparator,
                            userDefinedSeqComparator,
                            mergeSorter,
                            forceKeepDelete);
        }
    }

    private RecordReader<KeyValue> mergeRead(
            BinaryRow partition, int bucket, List<DataFileMeta> files, boolean keepDelete)
            throws IOException {
        // Sections are read by SortMergeReader, which sorts and merges records by keys.
        // So we cannot project keys or else the sorting will be incorrect.
        KeyValueFileReaderFactory overlappedSectionFactory =
                readerFactoryBuilder.build(partition, bucket, false, filtersForKeys);
        KeyValueFileReaderFactory nonOverlappedSectionFactory =
                readerFactoryBuilder.build(partition, bucket, false, filtersForAll);

        List<ReaderSupplier<KeyValue>> sectionReaders = new ArrayList<>();
        MergeFunctionWrapper<KeyValue> mergeFuncWrapper =
                new ReducerMergeFunctionWrapper(mfFactory.create(pushdownProjection));
        for (List<SortedRun> section : new IntervalPartition(files, keyComparator).partition()) {
            sectionReaders.add(
                    () ->
                            MergeTreeReaders.readerForSection(
                                    section,
                                    section.size() > 1
                                            ? overlappedSectionFactory
                                            : nonOverlappedSectionFactory,
                                    keyComparator,
                                    userDefinedSeqComparator,
                                    mergeFuncWrapper,
                                    mergeSorter));
        }
        RecordReader<KeyValue> reader = ConcatRecordReader.create(sectionReaders);

        if (!keepDelete) {
            reader = new DropDeleteReader(reader);
        }

        // Project results from SortMergeReader using ProjectKeyRecordReader.
        return keyProjectedFields == null ? reader : projectKey(reader, keyProjectedFields);
    }

    private RecordReader<KeyValue> noMergeRead(
            BinaryRow partition,
            int bucket,
            List<DataFileMeta> files,
            @Nullable List<DeletionFile> deletionFiles,
            boolean onlyFilterKey)
            throws IOException {
        KeyValueFileReaderFactory readerFactory =
                readerFactoryBuilder.build(
                        partition, bucket, true, onlyFilterKey ? filtersForKeys : filtersForAll);
        List<ReaderSupplier<KeyValue>> suppliers = new ArrayList<>();
        for (int i = 0; i < files.size(); i++) {
            DataFileMeta file = files.get(i);
            DeletionFile deletionFile = deletionFiles == null ? null : deletionFiles.get(i);
            suppliers.add(
                    () -> {
                        // We need to check extraFiles to be compatible with Paimon 0.2.
                        // See comments on DataFileMeta#extraFiles.
                        String fileName = changelogFile(file).orElse(file.fileName());
                        RecordReader<KeyValue> reader =
                                readerFactory.createRecordReader(
                                        file.schemaId(), fileName, file.fileSize(), file.level());
                        if (deletionFile != null) {
                            DeletionVector deletionVector =
                                    DeletionVector.read(fileIO, deletionFile);
                            reader = ApplyDeletionVectorReader.create(reader, deletionVector);
                        }
                        return reader;
                    });
        }
        return ConcatRecordReader.create(suppliers);
    }

    private Optional<String> changelogFile(DataFileMeta fileMeta) {
        for (String file : fileMeta.extraFiles()) {
            if (file.startsWith(CHANGELOG_FILE_PREFIX)) {
                return Optional.of(file);
            }
        }
        return Optional.empty();
    }

    private RecordReader<KeyValue> projectKey(
            RecordReader<KeyValue> reader, int[][] keyProjectedFields) {
        ProjectedRow projectedRow = ProjectedRow.from(keyProjectedFields);
        return reader.transform(kv -> kv.replaceKey(projectedRow.replaceRow(kv.key())));
    }
}
