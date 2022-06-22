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

package org.apache.flink.table.store.connector.sink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.store.file.FileStoreOptions;
import org.apache.flink.table.store.file.KeyValue;
import org.apache.flink.table.store.file.ValueKind;
import org.apache.flink.table.store.file.schema.TableSchema;
import org.apache.flink.table.store.file.utils.SnapshotManager;
import org.apache.flink.table.store.file.writer.RecordWriter;
import org.apache.flink.table.store.table.FileStoreTable;
import org.apache.flink.table.store.table.sink.AbstractTableWrite;
import org.apache.flink.table.store.table.sink.SinkRecord;
import org.apache.flink.table.store.table.sink.SinkRecordConverter;
import org.apache.flink.table.store.table.sink.TableCommit;
import org.apache.flink.table.store.table.sink.TableCompact;
import org.apache.flink.table.store.table.sink.TableWrite;
import org.apache.flink.table.store.table.source.TableRead;
import org.apache.flink.table.store.table.source.TableScan;
import org.apache.flink.types.RowKind;

/** {@link FileStoreTable} for tests. */
public class TestFileStoreTable implements FileStoreTable {

    private final TestFileStore store;
    private final TableSchema tableSchema;

    public TestFileStoreTable(TestFileStore store, TableSchema tableSchema) {
        this.store = store;
        this.tableSchema = tableSchema;
    }

    @Override
    public String name() {
        return "test";
    }

    @Override
    public TableSchema schema() {
        return tableSchema;
    }

    @Override
    public SnapshotManager snapshotManager() {
        throw new UnsupportedOperationException();
    }

    @Override
    public TableScan newScan() {
        throw new UnsupportedOperationException();
    }

    @Override
    public TableRead newRead() {
        throw new UnsupportedOperationException();
    }

    @Override
    public TableWrite newWrite() {
        return new AbstractTableWrite<KeyValue>(
                store.newWrite(),
                new SinkRecordConverter(2, tableSchema),
                new FileStoreOptions(new Configuration())) {
            @Override
            protected void writeSinkRecord(SinkRecord record, RecordWriter<KeyValue> writer)
                    throws Exception {
                boolean isInsert =
                        record.row().getRowKind() == RowKind.INSERT
                                || record.row().getRowKind() == RowKind.UPDATE_AFTER;
                KeyValue kv = new KeyValue();
                if (store.hasPk) {
                    kv.replace(
                            record.primaryKey(),
                            isInsert ? ValueKind.ADD : ValueKind.DELETE,
                            record.row());
                } else {
                    kv.replace(record.row(), ValueKind.ADD, GenericRowData.of(isInsert ? 1L : -1L));
                }
                writer.write(kv);
            }
        };
    }

    @Override
    public TableCommit newCommit() {
        return new TableCommit(store.newCommit(), store.newExpire());
    }

    @Override
    public TableCompact newCompact() {
        throw new UnsupportedOperationException();
    }
}
