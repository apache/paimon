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

package org.apache.flink.table.store.file;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.store.CoreOptions;
import org.apache.flink.table.store.file.operation.AppendOnlyFileStoreRead;
import org.apache.flink.table.store.file.operation.AppendOnlyFileStoreScan;
import org.apache.flink.table.store.file.operation.AppendOnlyFileStoreWrite;
import org.apache.flink.table.store.file.schema.SchemaManager;
import org.apache.flink.table.types.logical.RowType;

import java.util.Comparator;

/** {@link FileStore} for reading and writing {@link RowData}. */
public class AppendOnlyFileStore extends AbstractFileStore<RowData> {

    private final RowType bucketKeyType;
    private final RowType rowType;

    public AppendOnlyFileStore(
            SchemaManager schemaManager,
            long schemaId,
            CoreOptions options,
            RowType partitionType,
            RowType bucketKeyType,
            RowType rowType) {
        super(schemaManager, schemaId, options, partitionType);
        this.bucketKeyType = bucketKeyType;
        this.rowType = rowType;
    }

    @Override
    public AppendOnlyFileStoreScan newScan() {
        return newScan(false);
    }

    @Override
    public AppendOnlyFileStoreRead newRead() {
        return new AppendOnlyFileStoreRead(
                schemaManager, schemaId, rowType, options.fileFormat(), pathFactory());
    }

    @Override
    public AppendOnlyFileStoreWrite newWrite(String commitUser) {
        return new AppendOnlyFileStoreWrite(
                newRead(),
                schemaId,
                commitUser,
                rowType,
                pathFactory(),
                snapshotManager(),
                newScan(true),
                options);
    }

    private AppendOnlyFileStoreScan newScan(boolean checkNumOfBuckets) {
        return new AppendOnlyFileStoreScan(
                partitionType,
                bucketKeyType.getFieldCount() == 0 ? rowType : bucketKeyType,
                rowType,
                snapshotManager(),
                manifestFileFactory(),
                manifestListFactory(),
                options.bucket(),
                checkNumOfBuckets);
    }

    @Override
    public Comparator<RowData> newKeyComparator() {
        return null;
    }
}
