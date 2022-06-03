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

package org.apache.flink.table.store.table;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.store.file.FileStore;
import org.apache.flink.table.store.file.FileStoreImpl;
import org.apache.flink.table.store.file.FileStoreOptions;
import org.apache.flink.table.store.file.KeyValue;
import org.apache.flink.table.store.file.WriteMode;
import org.apache.flink.table.store.file.operation.FileStoreScan;
import org.apache.flink.table.store.file.predicate.Predicate;
import org.apache.flink.table.store.file.schema.Schema;
import org.apache.flink.table.store.file.utils.RecordReader;
import org.apache.flink.table.store.table.source.TableRead;
import org.apache.flink.table.store.table.source.TableScan;
import org.apache.flink.table.store.table.source.ValueContentRowDataRecordIterator;
import org.apache.flink.table.types.logical.RowType;

/** {@link FileStoreTable} for {@link WriteMode#APPEND_ONLY} write mode. */
public class AppendOnlyFileStoreTable implements FileStoreTable {

    private final Schema schema;
    private final FileStoreImpl store;

    AppendOnlyFileStoreTable(Schema schema, Configuration conf, String user) {
        this.schema = schema;
        this.store =
                new FileStoreImpl(
                        schema.id(),
                        new FileStoreOptions(conf),
                        WriteMode.APPEND_ONLY,
                        user,
                        schema.logicalPartitionType(),
                        RowType.of(),
                        schema.logicalRowType(),
                        null);
    }

    @Override
    public TableScan newScan(boolean incremental) {
        FileStoreScan scan = store.newScan().withIncremental(incremental);
        return new TableScan(scan, schema, store.pathFactory()) {
            @Override
            protected void withNonPartitionFilter(Predicate predicate) {
                scan.withValueFilter(predicate);
            }
        };
    }

    @Override
    public TableRead newRead(boolean incremental) {
        return new TableRead(store.newRead()) {
            @Override
            public TableRead withProjection(int[][] projection) {
                read.withValueProjection(projection);
                return this;
            }

            @Override
            protected RecordReader.RecordIterator<RowData> rowDataRecordIteratorFromKv(
                    RecordReader.RecordIterator<KeyValue> kvRecordIterator) {
                return new ValueContentRowDataRecordIterator(kvRecordIterator);
            }
        };
    }

    @Override
    public FileStore fileStore() {
        return store;
    }
}
