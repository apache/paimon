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
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.store.file.KeyValue;
import org.apache.flink.table.store.file.WriteMode;
import org.apache.flink.table.store.file.data.DataFileMeta;
import org.apache.flink.table.store.file.data.DataFileReader;
import org.apache.flink.table.store.file.format.FileFormat;
import org.apache.flink.table.store.file.mergetree.MergeTreeReader;
import org.apache.flink.table.store.file.mergetree.compact.ConcatRecordReader;
import org.apache.flink.table.store.file.mergetree.compact.IntervalPartition;
import org.apache.flink.table.store.file.mergetree.compact.MergeFunction;
import org.apache.flink.table.store.file.schema.SchemaManager;
import org.apache.flink.table.store.file.utils.FileStorePathFactory;
import org.apache.flink.table.store.file.utils.RecordReader;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/** Default implementation of {@link FileStoreRead}. */
public class FileStoreReadImpl implements FileStoreRead {

    private final DataFileReader.Factory dataFileReaderFactory;
    private final WriteMode writeMode;
    private final Comparator<RowData> keyComparator;
    @Nullable private final MergeFunction mergeFunction;

    private boolean keyProjected;
    private boolean dropDelete = true;

    public FileStoreReadImpl(
            SchemaManager schemaManager,
            long schemaId,
            WriteMode writeMode,
            RowType keyType,
            RowType valueType,
            Comparator<RowData> keyComparator,
            @Nullable MergeFunction mergeFunction,
            FileFormat fileFormat,
            FileStorePathFactory pathFactory) {
        this.dataFileReaderFactory =
                new DataFileReader.Factory(
                        schemaManager, schemaId, keyType, valueType, fileFormat, pathFactory);
        this.writeMode = writeMode;
        this.keyComparator = keyComparator;
        this.mergeFunction = mergeFunction;

        this.keyProjected = false;
    }

    @Override
    public FileStoreRead withDropDelete(boolean dropDelete) {
        Preconditions.checkArgument(
                writeMode != WriteMode.APPEND_ONLY || !dropDelete,
                "Cannot drop delete message for append-only table.");
        this.dropDelete = dropDelete;
        return this;
    }

    @Override
    public FileStoreRead withKeyProjection(int[][] projectedFields) {
        dataFileReaderFactory.withKeyProjection(projectedFields);
        keyProjected = true;
        return this;
    }

    @Override
    public FileStoreRead withValueProjection(int[][] projectedFields) {
        dataFileReaderFactory.withValueProjection(projectedFields);
        return this;
    }

    @Override
    public RecordReader<KeyValue> createReader(
            BinaryRowData partition, int bucket, List<DataFileMeta> files) throws IOException {
        switch (writeMode) {
            case APPEND_ONLY:
                return createAppendOnlyReader(partition, bucket, files);

            case CHANGE_LOG:
                return createMergeTreeReader(partition, bucket, files);

            default:
                throw new UnsupportedOperationException("Unknown write mode: " + writeMode);
        }
    }

    private RecordReader<KeyValue> createAppendOnlyReader(
            BinaryRowData partition, int bucket, List<DataFileMeta> files) throws IOException {
        DataFileReader dataFileReader = dataFileReaderFactory.create(partition, bucket);
        List<ConcatRecordReader.ReaderSupplier> suppliers = new ArrayList<>();
        for (DataFileMeta file : files) {
            suppliers.add(() -> dataFileReader.read(file.fileName()));
        }

        return ConcatRecordReader.create(suppliers);
    }

    private RecordReader<KeyValue> createMergeTreeReader(
            BinaryRowData partition, int bucket, List<DataFileMeta> files) throws IOException {
        DataFileReader dataFileReader = dataFileReaderFactory.create(partition, bucket);
        if (keyProjected) {
            // key projection has been applied, so data file readers will not return key-values in
            // order,
            // we have to return the raw file contents without merging
            List<ConcatRecordReader.ReaderSupplier> suppliers = new ArrayList<>();
            for (DataFileMeta file : files) {
                suppliers.add(() -> dataFileReader.read(file.fileName()));
            }

            if (dropDelete) {
                throw new UnsupportedOperationException(
                        "The key is projected, there is no ability to merge records, so the deleted message cannot be dropped.");
            }
            return ConcatRecordReader.create(suppliers);
        } else {
            // key projection is not applied, so data file readers will return key-values in order,
            // in this case merge tree can merge records with same key for us
            return new MergeTreeReader(
                    new IntervalPartition(files, keyComparator).partition(),
                    dropDelete,
                    dataFileReader,
                    keyComparator,
                    mergeFunction.copy());
        }
    }
}
