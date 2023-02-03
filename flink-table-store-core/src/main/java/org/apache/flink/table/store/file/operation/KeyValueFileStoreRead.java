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

package org.apache.flink.table.store.file.operation;

import org.apache.flink.table.store.data.InternalRow;
import org.apache.flink.table.store.file.KeyValue;
import org.apache.flink.table.store.file.io.DataFileMeta;
import org.apache.flink.table.store.file.io.KeyValueFileReaderFactory;
import org.apache.flink.table.store.file.mergetree.DropDeleteReader;
import org.apache.flink.table.store.file.mergetree.MergeTreeReaders;
import org.apache.flink.table.store.file.mergetree.SortedRun;
import org.apache.flink.table.store.file.mergetree.compact.ConcatRecordReader;
import org.apache.flink.table.store.file.mergetree.compact.IntervalPartition;
import org.apache.flink.table.store.file.mergetree.compact.MergeFunctionFactory;
import org.apache.flink.table.store.file.mergetree.compact.MergeFunctionWrapper;
import org.apache.flink.table.store.file.mergetree.compact.ReducerMergeFunctionWrapper;
import org.apache.flink.table.store.file.predicate.Predicate;
import org.apache.flink.table.store.file.schema.KeyValueFieldsExtractor;
import org.apache.flink.table.store.file.schema.SchemaManager;
import org.apache.flink.table.store.file.schema.TableSchema;
import org.apache.flink.table.store.file.utils.FileStorePathFactory;
import org.apache.flink.table.store.file.utils.RecordReader;
import org.apache.flink.table.store.file.utils.RecordReaderUtils;
import org.apache.flink.table.store.format.FileFormatDiscover;
import org.apache.flink.table.store.fs.FileIO;
import org.apache.flink.table.store.table.source.DataSplit;
import org.apache.flink.table.store.types.RowType;
import org.apache.flink.table.store.utils.ProjectedRow;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.flink.table.store.file.io.DataFilePathFactory.CHANGELOG_FILE_PREFIX;
import static org.apache.flink.table.store.file.predicate.PredicateBuilder.containsFields;
import static org.apache.flink.table.store.file.predicate.PredicateBuilder.splitAnd;

/**
 * {@link FileStoreRead} implementation for {@link
 * org.apache.flink.table.store.file.KeyValueFileStore}.
 */
public class KeyValueFileStoreRead implements FileStoreRead<KeyValue> {

    private final TableSchema tableSchema;
    private final KeyValueFileReaderFactory.Builder readerFactoryBuilder;
    private final Comparator<InternalRow> keyComparator;
    private final MergeFunctionFactory<KeyValue> mfFactory;
    private final boolean valueCountMode;

    @Nullable private int[][] keyProjectedFields;

    @Nullable private List<Predicate> filtersForOverlappedSection;

    @Nullable private List<Predicate> filtersForNonOverlappedSection;

    @Nullable private int[][] valueProjection;

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
            KeyValueFieldsExtractor extractor) {
        this.tableSchema = schemaManager.schema(schemaId);
        this.readerFactoryBuilder =
                KeyValueFileReaderFactory.builder(
                        fileIO,
                        schemaManager,
                        schemaId,
                        keyType,
                        valueType,
                        formatDiscover,
                        pathFactory,
                        extractor);
        this.keyComparator = keyComparator;
        this.mfFactory = mfFactory;
        this.valueCountMode = tableSchema.trimmedPrimaryKeys().isEmpty();
    }

    public KeyValueFileStoreRead withKeyProjection(int[][] projectedFields) {
        readerFactoryBuilder.withKeyProjection(projectedFields);
        this.keyProjectedFields = projectedFields;
        return this;
    }

    public KeyValueFileStoreRead withValueProjection(int[][] projectedFields) {
        this.valueProjection = projectedFields;
        readerFactoryBuilder.withValueProjection(projectedFields);
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
        filtersForOverlappedSection = valueCountMode ? allFilters : pkFilters;
        return this;
    }

    @Override
    public RecordReader<KeyValue> createReader(DataSplit split) throws IOException {
        if (split.isIncremental()) {
            KeyValueFileReaderFactory readerFactory =
                    readerFactoryBuilder.build(
                            split.partition(), split.bucket(), true, filtersForOverlappedSection);
            // Return the raw file contents without merging
            List<ConcatRecordReader.ReaderSupplier<KeyValue>> suppliers = new ArrayList<>();
            for (DataFileMeta file : split.files()) {
                suppliers.add(
                        () -> {
                            // We need to check extraFiles to be compatible with Table Store 0.2.
                            // See comments on DataFileMeta#extraFiles.
                            String fileName = changelogFile(file).orElse(file.fileName());
                            return readerFactory.createRecordReader(
                                    file.schemaId(), fileName, file.level());
                        });
            }
            RecordReader<KeyValue> concatRecordReader = ConcatRecordReader.create(suppliers);
            return split.reverseRowKind()
                    ? new ReverseReader(concatRecordReader)
                    : concatRecordReader;
        } else {
            // Sections are read by SortMergeReader, which sorts and merges records by keys.
            // So we cannot project keys or else the sorting will be incorrect.
            KeyValueFileReaderFactory overlappedSectionFactory =
                    readerFactoryBuilder.build(
                            split.partition(), split.bucket(), false, filtersForOverlappedSection);
            KeyValueFileReaderFactory nonOverlappedSectionFactory =
                    readerFactoryBuilder.build(
                            split.partition(),
                            split.bucket(),
                            false,
                            filtersForNonOverlappedSection);

            List<ConcatRecordReader.ReaderSupplier<KeyValue>> sectionReaders = new ArrayList<>();
            MergeFunctionWrapper<KeyValue> mergeFuncWrapper =
                    new ReducerMergeFunctionWrapper(mfFactory.create(valueProjection));
            for (List<SortedRun> section :
                    new IntervalPartition(split.files(), keyComparator).partition()) {
                sectionReaders.add(
                        () ->
                                MergeTreeReaders.readerForSection(
                                        section,
                                        section.size() > 1
                                                ? overlappedSectionFactory
                                                : nonOverlappedSectionFactory,
                                        keyComparator,
                                        mergeFuncWrapper));
            }
            DropDeleteReader reader =
                    new DropDeleteReader(ConcatRecordReader.create(sectionReaders));

            // Project results from SortMergeReader using ProjectKeyRecordReader.
            return keyProjectedFields == null ? reader : projectKey(reader, keyProjectedFields);
        }
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
        return RecordReaderUtils.transform(
                reader, kv -> kv.replaceKey(projectedRow.replaceRow(kv.key())));
    }
}
