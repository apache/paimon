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
import org.apache.flink.table.store.file.data.DataFileMeta;
import org.apache.flink.table.store.file.data.DataFileReader;
import org.apache.flink.table.store.file.mergetree.MergeTreeReader;
import org.apache.flink.table.store.file.mergetree.compact.ConcatRecordReader;
import org.apache.flink.table.store.file.mergetree.compact.IntervalPartition;
import org.apache.flink.table.store.file.mergetree.compact.MergeFunction;
import org.apache.flink.table.store.file.predicate.Predicate;
import org.apache.flink.table.store.file.schema.SchemaManager;
import org.apache.flink.table.store.file.utils.FileStorePathFactory;
import org.apache.flink.table.store.file.utils.ProjectKeyRecordReader;
import org.apache.flink.table.store.file.utils.RecordReader;
import org.apache.flink.table.store.format.FileFormat;
import org.apache.flink.table.store.table.source.Split;
import org.apache.flink.table.types.logical.RowType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;

import static org.apache.flink.table.store.file.data.DataFilePathFactory.CHANGELOG_FILE_PREFIX;
import static org.apache.flink.table.store.file.predicate.PredicateBuilder.splitAnd;

/**
 * {@link FileStoreRead} implementation for {@link
 * org.apache.flink.table.store.file.KeyValueFileStore}.
 */
public class KeyValueFileStoreRead implements FileStoreRead<KeyValue> {

    private final DataFileReader.Factory dataFileReaderFactory;
    private final Comparator<RowData> keyComparator;
    private final MergeFunction mergeFunction;

    private int[][] keyProjectedFields;

    private List<Predicate> filters;

    public KeyValueFileStoreRead(
            SchemaManager schemaManager,
            long schemaId,
            RowType keyType,
            RowType valueType,
            Comparator<RowData> keyComparator,
            MergeFunction mergeFunction,
            FileFormat fileFormat,
            FileStorePathFactory pathFactory) {
        this.dataFileReaderFactory =
                new DataFileReader.Factory(
                        schemaManager, schemaId, keyType, valueType, fileFormat, pathFactory);
        this.keyComparator = keyComparator;
        this.mergeFunction = mergeFunction;
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
        this.filters = splitAnd(predicate);
        return this;
    }

    @Override
    public RecordReader<KeyValue> createReader(Split split) throws IOException {
        if (split.isIncremental()) {
            DataFileReader dataFileReader =
                    dataFileReaderFactory.create(split.partition(), split.bucket(), true, filters);
            // Return the raw file contents without merging
            List<ConcatRecordReader.ReaderSupplier<KeyValue>> suppliers = new ArrayList<>();
            for (DataFileMeta file : split.files()) {
                suppliers.add(
                        () -> dataFileReader.read(changelogFile(file).orElse(file.fileName())));
            }
            return ConcatRecordReader.create(suppliers);
        } else {
            // in this case merge tree should merge records with same key
            // Do not project key in MergeTreeReader.
            DataFileReader dataFileReader =
                    dataFileReaderFactory.create(split.partition(), split.bucket(), false, filters);
            MergeTreeReader reader =
                    new MergeTreeReader(
                            new IntervalPartition(split.files(), keyComparator).partition(),
                            true,
                            dataFileReader,
                            keyComparator,
                            mergeFunction.copy());

            // project key using ProjectKeyRecordReader
            return keyProjectedFields == null
                    ? reader
                    : new ProjectKeyRecordReader(reader, keyProjectedFields);
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
}
