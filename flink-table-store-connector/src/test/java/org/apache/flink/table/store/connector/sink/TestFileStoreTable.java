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

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.store.file.FileStore;
import org.apache.flink.table.store.file.ValueKind;
import org.apache.flink.table.store.file.schema.Schema;
import org.apache.flink.table.store.file.utils.SnapshotManager;
import org.apache.flink.table.store.file.writer.RecordWriter;
import org.apache.flink.table.store.table.FileStoreTable;
import org.apache.flink.table.store.table.sink.SinkRecord;
import org.apache.flink.table.store.table.sink.SinkRecordConverter;
import org.apache.flink.table.store.table.sink.TableCommit;
import org.apache.flink.table.store.table.sink.TableWrite;
import org.apache.flink.table.store.table.source.TableRead;
import org.apache.flink.table.store.table.source.TableScan;
import org.apache.flink.types.RowKind;

/** {@link FileStoreTable} for tests. */
public class TestFileStoreTable implements FileStoreTable {

    private final TestFileStore store;
    private final Schema schema;

    public TestFileStoreTable(TestFileStore store, Schema schema) {
        this.store = store;
        this.schema = schema;
    }

    @Override
    public String name() {
        return "test";
    }

    @Override
    public Schema schema() {
        return schema;
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
        return new TableWrite(store.newWrite(), new SinkRecordConverter(2, schema)) {
            @Override
            protected void writeSinkRecord(SinkRecord record, RecordWriter writer)
                    throws Exception {
                boolean isInsert =
                        record.row().getRowKind() == RowKind.INSERT
                                || record.row().getRowKind() == RowKind.UPDATE_AFTER;
                if (store.hasPk) {
                    writer.write(
                            isInsert ? ValueKind.ADD : ValueKind.DELETE,
                            record.primaryKey(),
                            record.row());
                } else {
                    writer.write(
                            ValueKind.ADD, record.row(), GenericRowData.of(isInsert ? 1L : -1L));
                }
            }
        };
    }

    @Override
    public TableCommit newCommit() {
        return new TableCommit(store.newCommit(), store.newExpire());
    }

    @Override
    public FileStore store() {
        return store;
    }
}
