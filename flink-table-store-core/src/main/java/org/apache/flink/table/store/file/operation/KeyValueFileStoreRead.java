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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.store.file.KeyValue;
import org.apache.flink.table.store.file.data.DataFileMeta;
import org.apache.flink.table.store.file.data.DataFileReader;
import org.apache.flink.table.store.file.mergetree.MergeTreeReader;
import org.apache.flink.table.store.file.mergetree.SortedRun;
import org.apache.flink.table.store.file.mergetree.compact.ConcatRecordReader;
import org.apache.flink.table.store.file.mergetree.compact.IntervalPartition;
import org.apache.flink.table.store.file.mergetree.compact.MergeFunction;
import org.apache.flink.table.store.file.predicate.Predicate;
import org.apache.flink.table.store.file.schema.SchemaManager;
import org.apache.flink.table.store.file.schema.TableSchema;
import org.apache.flink.table.store.file.stats.FieldStatsArraySerializer;
import org.apache.flink.table.store.file.utils.FileStorePathFactory;
import org.apache.flink.table.store.file.utils.ProjectKeyRecordReader;
import org.apache.flink.table.store.file.utils.RecordReader;
import org.apache.flink.table.store.format.FileFormat;
import org.apache.flink.table.store.table.source.Split;
import org.apache.flink.table.types.logical.RowType;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.flink.table.store.file.data.DataFilePathFactory.CHANGELOG_FILE_PREFIX;
import static org.apache.flink.table.store.file.predicate.PredicateBuilder.and;
import static org.apache.flink.table.store.file.predicate.PredicateBuilder.containsFields;
import static org.apache.flink.table.store.file.predicate.PredicateBuilder.splitAnd;

/**
 * {@link FileStoreRead} implementation for {@link
 * org.apache.flink.table.store.file.KeyValueFileStore}.
 */
public class KeyValueFileStoreRead implements FileStoreRead<KeyValue> {

    private final TableSchema tableSchema;
    private final DataFileReader.Factory dataFileReaderFactory;
    private final Comparator<RowData> keyComparator;
    private final MergeFunction mergeFunction;
    private final boolean valueCountMode;
    private final FieldStatsArraySerializer rowStatsConverter;

    @Nullable private int[][] keyProjectedFields;

    @Nullable private List<Predicate> keyFilters;

    @Nullable private List<Predicate> allFilters;

    public KeyValueFileStoreRead(
            SchemaManager schemaManager,
            long schemaId,
            RowType keyType,
            RowType valueType,
            Comparator<RowData> keyComparator,
            MergeFunction mergeFunction,
            FileFormat fileFormat,
            FileStorePathFactory pathFactory) {
        this.tableSchema = schemaManager.schema(schemaId);
        this.dataFileReaderFactory =
                new DataFileReader.Factory(
                        schemaManager, schemaId, keyType, valueType, fileFormat, pathFactory);
        this.keyComparator = keyComparator;
        this.mergeFunction = mergeFunction;
        this.valueCountMode = tableSchema.trimmedPrimaryKeys().isEmpty();
        this.rowStatsConverter =
                valueCountMode
                        ? new FieldStatsArraySerializer(keyType)
                        : new FieldStatsArraySerializer(valueType);
    }

    public KeyValueFileStoreRead withKeyProjection(int[][] projectedFields) {
        dataFileReaderFactory.withKeyProjection(projectedFields);
        this.keyProjectedFields = projectedFields;
        return this;
    }

    public KeyValueFileStoreRead withValueProjection(int[][] projectedFields) {
        dataFileReaderFactory.withValueProjection(projectedFields);
        return this;
    }

    @Override
    public FileStoreRead<KeyValue> withFilter(Predicate predicate) {
        allFilters = new ArrayList<>();
        List<String> primaryKeys = tableSchema.trimmedPrimaryKeys();
        Set<String> nonPrimaryKeys =
                tableSchema.fieldNames().stream()
                        .filter(name -> !primaryKeys.contains(name))
                        .collect(Collectors.toSet());
        for (Predicate sub : splitAnd(predicate)) {
            allFilters.add(sub);
            if (!containsFields(sub, nonPrimaryKeys)) {
                if (keyFilters == null) {
                    keyFilters = new ArrayList<>();
                }
                // TODO Actually, the index is wrong, but it is OK.
                //  The orc filter just use name instead of index.
                keyFilters.add(sub);
            }
        }
        return this;
    }

    @Override
    public RecordReader<KeyValue> createReader(Split split) throws IOException {
        if (split.isIncremental()) {
            // incremental mode cannot push down value filters, because the update for the same key
            // may occur in the next split
            DataFileReader dataFileReader = createDataFileReader(split, true, false);
            // Return the raw file contents without merging
            List<ConcatRecordReader.ReaderSupplier<KeyValue>> suppliers = new ArrayList<>();
            for (DataFileMeta file : split.files()) {
                if (acceptFilter(false).test(file)) {
                    suppliers.add(
                            () -> dataFileReader.read(changelogFile(file).orElse(file.fileName())));
                }
            }
            return ConcatRecordReader.create(suppliers);
        } else {
            // in this case merge tree should merge records with same key
            // Do not project key in MergeTreeReader.
            List<List<SortedRun>> sections =
                    new IntervalPartition(split.files(), keyComparator).partition();
            DataFileReader dataFileReaderWithAllFilters = createDataFileReader(split, false, true);
            DataFileReader dataFileReaderWithKeyFilters = createDataFileReader(split, false, false);
            MergeFunction mergeFunc = mergeFunction.copy();
            List<ConcatRecordReader.ReaderSupplier<KeyValue>> readers = new ArrayList<>();
            for (List<SortedRun> section : sections) {
                // if key ranges do not have overlap, value filter can be pushed down as well
                boolean acceptAll = section.size() == 1;
                List<SortedRun> hitSection = new ArrayList<>();
                for (SortedRun run : section) {
                    List<DataFileMeta> hitFiles = new ArrayList<>();
                    for (DataFileMeta file : run.files()) {
                        if (acceptFilter(acceptAll).test(file)) {
                            hitFiles.add(file);
                        }
                    }
                    if (hitFiles.size() > 0) {
                        hitSection.add(SortedRun.fromSorted(hitFiles));
                    }
                }
                if (hitSection.size() > 0) {
                    readers.add(
                            () ->
                                    MergeTreeReader.readerForSection(
                                            hitSection,
                                            acceptAll
                                                    ? dataFileReaderWithAllFilters
                                                    : dataFileReaderWithKeyFilters,
                                            keyComparator,
                                            mergeFunc));
                }
            }
            MergeTreeReader reader = new MergeTreeReader(true, readers);

            // project key using ProjectKeyRecordReader
            return keyProjectedFields == null
                    ? reader
                    : new ProjectKeyRecordReader(reader, keyProjectedFields);
        }
    }

    @VisibleForTesting
    java.util.function.Predicate<DataFileMeta> acceptFilter(boolean acceptAllFilter) {
        Predicate keyFilter = keyFilters == null ? null : and(keyFilters);
        Predicate allFilter = allFilters == null ? null : and(allFilters);
        return file -> {
            if (valueCountMode) {
                return allFilter == null
                        || allFilter.test(
                                file.rowCount(),
                                file.keyStats().fields(rowStatsConverter, file.rowCount()));
            } else {
                Predicate filter = acceptAllFilter ? allFilter : keyFilter;
                return filter == null
                        || filter.test(
                                file.rowCount(),
                                file.valueStats().fields(rowStatsConverter, file.rowCount()));
            }
        };
    }

    private Optional<String> changelogFile(DataFileMeta fileMeta) {
        for (String file : fileMeta.extraFiles()) {
            if (file.startsWith(CHANGELOG_FILE_PREFIX)) {
                return Optional.of(file);
            }
        }
        return Optional.empty();
    }

    private DataFileReader createDataFileReader(
            Split split, boolean projectKeys, boolean acceptAllFilter) {
        return dataFileReaderFactory.create(
                split.partition(),
                split.bucket(),
                projectKeys,
                acceptAllFilter ? allFilters : keyFilters);
    }
}
