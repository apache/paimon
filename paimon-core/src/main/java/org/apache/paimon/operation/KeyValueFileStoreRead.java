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
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.format.FileFormatDiscover;
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
import org.apache.paimon.mergetree.compact.ReorderFunction;
import org.apache.paimon.mergetree.compact.ReorderFunctionFactory;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.KeyValueFieldsExtractor;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.FileStorePathFactory;
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
    private final KeyValueFileReaderFactory.Builder readerFactoryBuilder;
    private final Comparator<InternalRow> keyComparator;
    private final MergeFunctionFactory<KeyValue> mfFactory;
    private final MergeSorter mergeSorter;
    private final ReorderFunctionFactory<KeyValue> rfFactory;

    @Nullable private int[][] keyProjectedFields;

    @Nullable private List<Predicate> filtersForOverlappedSection;

    @Nullable private List<Predicate> filtersForNonOverlappedSection;

    @Nullable private int[][] pushdownProjection;
    @Nullable private int[][] outerProjection;

    private boolean forceKeepDelete = false;

    public KeyValueFileStoreRead(
            FileIO fileIO,
            SchemaManager schemaManager,
            long schemaId,
            RowType keyType,
            RowType valueType,
            Comparator<InternalRow> keyComparator,
            MergeFunctionFactory<KeyValue> mfFactory,
            FileFormatDiscover formatDiscover,
            FileStorePathFactory pathFactory,
            KeyValueFieldsExtractor extractor,
            CoreOptions options,
            ReorderFunctionFactory<KeyValue> rfFactory) {
        this(
                schemaManager,
                schemaId,
                keyType,
                valueType,
                keyComparator,
                mfFactory,
                KeyValueFileReaderFactory.builder(
                        fileIO,
                        schemaManager,
                        schemaId,
                        keyType,
                        valueType,
                        formatDiscover,
                        pathFactory,
                        extractor,
                        options),
                rfFactory);
    }

    public KeyValueFileStoreRead(
            SchemaManager schemaManager,
            long schemaId,
            RowType keyType,
            RowType valueType,
            Comparator<InternalRow> keyComparator,
            MergeFunctionFactory<KeyValue> mfFactory,
            KeyValueFileReaderFactory.Builder readerFactoryBuilder,
            ReorderFunctionFactory<KeyValue> rfFactory) {
        this.tableSchema = schemaManager.schema(schemaId);
        this.readerFactoryBuilder = readerFactoryBuilder;
        this.keyComparator = keyComparator;
        this.mfFactory = mfFactory;
        this.mergeSorter =
                new MergeSorter(
                        CoreOptions.fromMap(tableSchema.options()), keyType, valueType, null);
        this.rfFactory = rfFactory;
    }

    public KeyValueFileStoreRead withKeyProjection(int[][] projectedFields) {
        readerFactoryBuilder.withKeyProjection(projectedFields);
        this.keyProjectedFields = projectedFields;
        return this;
    }

    public KeyValueFileStoreRead withValueProjection(int[][] projectedFields) {
        AdjustedProjection mfProjection = mfFactory.adjustProjection(projectedFields);
        AdjustedProjection projection = rfFactory.adjustProjection(mfProjection.pushdownProjection);
        this.pushdownProjection = projection.pushdownProjection;
        // keep the first outer projection.
        this.outerProjection = mfProjection.outerProjection;
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
        filtersForNonOverlappedSection = allFilters;
        filtersForOverlappedSection = pkFilters;
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
        if (split.isStreaming()) {
            KeyValueFileReaderFactory readerFactory =
                    readerFactoryBuilder.build(
                            split.partition(), split.bucket(), true, filtersForOverlappedSection);
            ReaderSupplier<KeyValue> beforeSupplier =
                    () -> new ReverseReader(streamingConcat(split.beforeFiles(), readerFactory));
            ReaderSupplier<KeyValue> dataSupplier =
                    () -> streamingConcat(split.dataFiles(), readerFactory);
            return split.beforeFiles().isEmpty()
                    ? dataSupplier.get()
                    : ConcatRecordReader.create(Arrays.asList(beforeSupplier, dataSupplier));
        } else {
            return split.beforeFiles().isEmpty()
                    ? batchMergeRead(
                            split.partition(), split.bucket(), split.dataFiles(), forceKeepDelete)
                    : DiffReader.readDiff(
                            batchMergeRead(
                                    split.partition(), split.bucket(), split.beforeFiles(), false),
                            batchMergeRead(
                                    split.partition(), split.bucket(), split.dataFiles(), false),
                            keyComparator,
                            mergeSorter,
                            forceKeepDelete,
                            p -> null);
        }
    }

    private RecordReader<KeyValue> batchMergeRead(
            BinaryRow partition, int bucket, List<DataFileMeta> files, boolean keepDelete)
            throws IOException {
        // Sections are read by SortMergeReader, which sorts and merges records by keys.
        // So we cannot project keys or else the sorting will be incorrect.
        KeyValueFileReaderFactory overlappedSectionFactory =
                readerFactoryBuilder.build(partition, bucket, false, filtersForOverlappedSection);
        KeyValueFileReaderFactory nonOverlappedSectionFactory =
                readerFactoryBuilder.build(
                        partition, bucket, false, filtersForNonOverlappedSection);

        List<ReaderSupplier<KeyValue>> sectionReaders = new ArrayList<>();
        MergeFunctionWrapper<KeyValue> mergeFuncWrapper =
                new ReducerMergeFunctionWrapper(mfFactory.create(pushdownProjection));
        ReorderFunction<KeyValue> reorderFunction = rfFactory.create(pushdownProjection);
        for (List<SortedRun> section : new IntervalPartition(files, keyComparator).partition()) {
            sectionReaders.add(
                    () ->
                            MergeTreeReaders.readerForSection(
                                    section,
                                    section.size() > 1
                                            ? overlappedSectionFactory
                                            : nonOverlappedSectionFactory,
                                    keyComparator,
                                    mergeFuncWrapper,
                                    mergeSorter,
                                    reorderFunction));
        }

        RecordReader<KeyValue> reader = ConcatRecordReader.create(sectionReaders);
        if (!keepDelete) {
            reader = new DropDeleteReader(reader);
        }

        // Project results from SortMergeReader using ProjectKeyRecordReader.
        return keyProjectedFields == null ? reader : projectKey(reader, keyProjectedFields);
    }

    private RecordReader<KeyValue> streamingConcat(
            List<DataFileMeta> files, KeyValueFileReaderFactory readerFactory) throws IOException {
        List<ReaderSupplier<KeyValue>> suppliers = new ArrayList<>();
        for (DataFileMeta file : files) {
            suppliers.add(
                    () -> {
                        // We need to check extraFiles to be compatible with Paimon 0.2.
                        // See comments on DataFileMeta#extraFiles.
                        String fileName = changelogFile(file).orElse(file.fileName());
                        return readerFactory.createRecordReader(
                                file.schemaId(), fileName, file.fileSize(), file.level());
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
