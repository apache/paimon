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

package org.apache.paimon.table;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.WriteMode;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.manifest.ManifestCacheFilter;
import org.apache.paimon.operation.AppendOnlyFileStoreWrite;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.sink.InternalRowKeyAndBucketExtractor;
import org.apache.paimon.table.sink.TableWriteImpl;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.utils.Preconditions;

/** {@link FileStoreTable} for {@link WriteMode#TABLE} write mode. */
public class BatchFileStoreTable extends AppendOnlyFileStoreTable {
    public static final int BATCH_TABLE_BUCKET = 0;

    BatchFileStoreTable(FileIO fileIO, Path path, TableSchema tableSchema) {
        super(fileIO, path, tableSchema);
        Preconditions.checkState(
                tableSchema.options().get(CoreOptions.BUCKET_KEY.key()) == null
                        && tableSchema.options().get(CoreOptions.BUCKET.key()) == null,
                "Table mode could not set properties 'bucket_key' and 'bucket'");
    }

    @Override
    public TableWriteImpl<InternalRow> newWrite(
            String commitUser, ManifestCacheFilter manifestFilter) {
        AppendOnlyFileStoreWrite writer = store().newWrite(commitUser);
        writer.skipCompaction();
        writer.fromEmptyWriter();
        writer.withOverwrite(true);
        return new TableWriteImpl<>(
                writer,
                new InternalRowKeyAndBucketExtractor(tableSchema) {
                    @Override
                    public int bucket() {
                        return BATCH_TABLE_BUCKET;
                    }
                },
                record -> {
                    Preconditions.checkState(
                            record.row().getRowKind() == RowKind.INSERT,
                            "Batch table writer can not accept row with RowKind %s",
                            record.row().getRowKind());
                    return record.row();
                });
    }

    @Override
    protected FileStoreTable copy(TableSchema newTableSchema) {
        return new BatchFileStoreTable(fileIO, path, newTableSchema);
    }
}
