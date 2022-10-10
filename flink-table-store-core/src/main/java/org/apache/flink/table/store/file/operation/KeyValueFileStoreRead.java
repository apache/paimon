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

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.store.file.KeyValue;
import org.apache.flink.table.store.file.io.DataFileMeta;
import org.apache.flink.table.store.file.io.KeyValueFileReaderFactory;
import org.apache.flink.table.store.file.mergetree.MergeTreeReader;
import org.apache.flink.table.store.file.mergetree.SortedRun;
import org.apache.flink.table.store.file.mergetree.compact.ConcatRecordReader;
import org.apache.flink.table.store.file.mergetree.compact.IntervalPartition;
import org.apache.flink.table.store.file.mergetree.compact.MergeFunction;
import org.apache.flink.table.store.file.predicate.Predicate;
import org.apache.flink.table.store.file.schema.SchemaManager;
import org.apache.flink.table.store.file.schema.TableSchema;
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
    private final Comparator<RowData> keyComparator;
    private final MergeFunction<KeyValue> mergeFunction;
    private final boolean valueCountMode;

    @Nullable private int[][] keyProjectedFields;

    @Nullable private List<Predicate> filtersForOverlappedSection;

    @Nullable private List<Predicate> filtersForNonOverlappedSection;

    public KeyValueFileStoreRead(
            SchemaManager schemaManager,
            long schemaId,
            RowType keyType,
            RowType valueType,
            Comparator<RowData> keyComparator,
            MergeFunction<KeyValue> mergeFunction,
            FileFormat fileFormat,
            FileStorePathFactory pathFactory) {
        this.tableSchema = schemaManager.schema(schemaId);
        this.readerFactoryBuilder =
                KeyValueFileReaderFactory.builder(
                        schemaManager, schemaId, keyType, valueType, fileFormat, pathFactory);
        this.keyComparator = keyComparator;
        this.mergeFunction = mergeFunction;
        this.valueCountMode = tableSchema.trimmedPrimaryKeys().isEmpty();
    }

    public KeyValueFileStoreRead withKeyProjection(int[][] projectedFields) {
        readerFactoryBuilder.withKeyProjection(projectedFields);
        this.keyProjectedFields = projectedFields;
        return this;
    }

    public KeyValueFileStoreRead withValueProjection(int[][] projectedFields) {
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
        // for section which does not have key range overlap, push down value filters too
        filtersForNonOverlappedSection = allFilters;
        filtersForOverlappedSection = valueCountMode ? allFilters : pkFilters;
        return this;
    }

    @Override
    public RecordReader<KeyValue> createReader(Split split) throws IOException {
        if (split.isIncremental()) {
            KeyValueFileReaderFactory readerFactory =
                    readerFactoryBuilder.build(
                            split.partition(), split.bucket(), true, filtersForOverlappedSection);
            // Return the raw file contents without merging
            List<ConcatRecordReader.ReaderSupplier<KeyValue>> suppliers = new ArrayList<>();
            for (DataFileMeta file : split.files()) {
                suppliers.add(
                        () ->
                                readerFactory.createRecordReader(
                                        changelogFile(file).orElse(file.fileName()), file.level()));
            }
            return ConcatRecordReader.create(suppliers);
        } else {
            // in this case merge tree should merge records with same key
            // Do not project key in MergeTreeReader.
            MergeTreeReader reader = new MergeTreeReader(true, createSectionReaders(split));

            // project key using ProjectKeyRecordReader
            return keyProjectedFields == null
                    ? reader
                    : new ProjectKeyRecordReader(reader, keyProjectedFields);
        }
    }

    private List<ConcatRecordReader.ReaderSupplier<KeyValue>> createSectionReaders(Split split) {
        List<ConcatRecordReader.ReaderSupplier<KeyValue>> sectionReaders = new ArrayList<>();
        MergeFunction<KeyValue> mergeFunc = mergeFunction.copy();
        for (List<SortedRun> section :
                new IntervalPartition(split.files(), keyComparator).partition()) {
            KeyValueFileReaderFactory readerFactory =
                    createReaderFactory(split, section.size() > 1);
            sectionReaders.add(
                    () ->
                            MergeTreeReader.readerForSection(
                                    section, readerFactory, keyComparator, mergeFunc));
        }
        return sectionReaders;
    }

    private KeyValueFileReaderFactory createReaderFactory(Split split, boolean overlapped) {
        return readerFactoryBuilder.build(
                split.partition(),
                split.bucket(),
                false,
                overlapped ? filtersForOverlappedSection : filtersForNonOverlappedSection);
    }

    private Optional<String> changelogFile(DataFileMeta fileMeta) {
        for (String file : fileMeta.extraFiles()) {
            if (file.startsWith(CHANGELOG_FILE_PREFIX)) {
                return Optional.of(file);
            }
        }
        return Optional.empty();
    }
}
