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
import org.apache.flink.table.store.file.data.AppendOnlyReader;
import org.apache.flink.table.store.file.data.DataFileMeta;
import org.apache.flink.table.store.file.format.FileFormat;
import org.apache.flink.table.store.file.mergetree.compact.ConcatRecordReader;
import org.apache.flink.table.store.file.schema.SchemaManager;
import org.apache.flink.table.store.file.utils.FileStorePathFactory;
import org.apache.flink.table.store.file.utils.RecordReader;
import org.apache.flink.table.types.logical.RowType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/** {@link FileStoreRead} for {@link org.apache.flink.table.store.file.AppendOnlyFileStore}. */
public class AppendOnlyFileStoreRead implements FileStoreRead<RowData> {

    private final AppendOnlyReader.Factory appendOnlyReaderFactory;

    public AppendOnlyFileStoreRead(
            SchemaManager schemaManager,
            long schemaId,
            RowType rowType,
            FileFormat fileFormat,
            FileStorePathFactory pathFactory) {
        this.appendOnlyReaderFactory =
                new AppendOnlyReader.Factory(
                        schemaManager, schemaId, rowType, fileFormat, pathFactory);
    }

    public FileStoreRead<RowData> withProjection(int[][] projectedFields) {
        appendOnlyReaderFactory.withProjection(projectedFields);
        return this;
    }

    @Override
    public RecordReader<RowData> createReader(
            BinaryRowData partition, int bucket, List<DataFileMeta> files) throws IOException {
        AppendOnlyReader appendOnlyReader = appendOnlyReaderFactory.create(partition, bucket);
        List<ConcatRecordReader.ReaderSupplier<RowData>> suppliers = new ArrayList<>();
        for (DataFileMeta file : files) {
            suppliers.add(() -> appendOnlyReader.read(file.fileName()));
        }

        return ConcatRecordReader.create(suppliers);
    }
}
