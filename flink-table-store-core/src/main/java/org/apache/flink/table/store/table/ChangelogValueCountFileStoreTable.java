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
import org.apache.flink.table.store.file.mergetree.compact.MergeFunction;
import org.apache.flink.table.store.file.mergetree.compact.ValueCountMergeFunction;
import org.apache.flink.table.store.file.operation.FileStoreRead;
import org.apache.flink.table.store.file.predicate.Predicate;
import org.apache.flink.table.store.file.schema.Schema;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import java.util.Iterator;

/** {@link FileStoreTable} for {@link WriteMode#CHANGE_LOG} write mode without primary keys. */
public class ChangelogValueCountFileStoreTable implements FileStoreTable {

    private final Schema schema;
    private final boolean isStreaming;
    private final FileStoreImpl store;

    ChangelogValueCountFileStoreTable(
            Schema schema, boolean isStreaming, Configuration conf, String user) {
        this.schema = schema;
        this.isStreaming = isStreaming;

        RowType countType =
                RowType.of(
                        new LogicalType[] {new BigIntType(false)}, new String[] {"_VALUE_COUNT"});
        MergeFunction mergeFunction = new ValueCountMergeFunction();
        this.store =
                new FileStoreImpl(
                        schema.id(),
                        new FileStoreOptions(conf),
                        WriteMode.CHANGE_LOG,
                        user,
                        schema.logicalPartitionType(),
                        schema.logicalRowType(),
                        countType,
                        mergeFunction);
    }

    @Override
    public TableScan newScan() {
        return new TableScan(store.newScan(), schema, store.pathFactory()) {
            @Override
            protected void withNonPartitionFilter(Predicate predicate) {
                scan.withKeyFilter(predicate);
            }
        };
    }

    @Override
    public TableRead newRead() {
        FileStoreRead read = store.newRead();
        if (isStreaming) {
            read.withDropDelete(false);
        }

        return new TableRead(read) {
            private int[][] projection = null;

            @Override
            protected void withProjectionImpl(int[][] projection) {
                if (isStreaming) {
                    read.withKeyProjection(projection);
                } else {
                    this.projection = projection;
                }
            }

            @Override
            protected Iterator<RowData> rowDataIteratorFromKv(KeyValue kv) {
                return new ValueCountRowDataIterator(kv, projection);
            }
        };
    }

    @Override
    public FileStore fileStore() {
        return store;
    }
}
