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

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.store.file.FileStoreImpl;
import org.apache.flink.table.store.file.FileStoreOptions;
import org.apache.flink.table.store.file.KeyValue;
import org.apache.flink.table.store.file.ValueKind;
import org.apache.flink.table.store.file.WriteMode;
import org.apache.flink.table.store.file.mergetree.compact.MergeFunction;
import org.apache.flink.table.store.file.mergetree.compact.ValueCountMergeFunction;
import org.apache.flink.table.store.file.predicate.Predicate;
import org.apache.flink.table.store.file.schema.Schema;
import org.apache.flink.table.store.file.schema.SchemaManager;
import org.apache.flink.table.store.file.utils.RecordReader;
import org.apache.flink.table.store.file.writer.RecordWriter;
import org.apache.flink.table.store.table.sink.SinkRecord;
import org.apache.flink.table.store.table.sink.SinkRecordConverter;
import org.apache.flink.table.store.table.sink.TableWrite;
import org.apache.flink.table.store.table.source.TableRead;
import org.apache.flink.table.store.table.source.TableScan;
import org.apache.flink.table.store.table.source.ValueCountRowDataRecordIterator;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

/** {@link FileStoreTable} for {@link WriteMode#CHANGE_LOG} write mode without primary keys. */
public class ChangelogValueCountFileStoreTable extends AbstractFileStoreTable {

    private static final long serialVersionUID = 1L;

    private final FileStoreImpl store;

    ChangelogValueCountFileStoreTable(
            String name, SchemaManager schemaManager, Schema schema, String user) {
        super(name, schema);
        RowType countType =
                RowType.of(
                        new LogicalType[] {new BigIntType(false)}, new String[] {"_VALUE_COUNT"});
        MergeFunction mergeFunction = new ValueCountMergeFunction();
        this.store =
                new FileStoreImpl(
                        schemaManager,
                        schema.id(),
                        new FileStoreOptions(schema.options()),
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
        return new TableRead(store.newRead()) {
            private int[][] projection = null;
            private boolean isIncremental = false;

            @Override
            public TableRead withProjection(int[][] projection) {
                if (isIncremental) {
                    read.withKeyProjection(projection);
                } else {
                    this.projection = projection;
                }
                return this;
            }

            @Override
            public TableRead withIncremental(boolean isIncremental) {
                this.isIncremental = isIncremental;
                read.withDropDelete(!isIncremental);
                return this;
            }

            @Override
            protected RecordReader.RecordIterator<RowData> rowDataRecordIteratorFromKv(
                    RecordReader.RecordIterator<KeyValue> kvRecordIterator) {
                return new ValueCountRowDataRecordIterator(kvRecordIterator, projection);
            }
        };
    }

    @Override
    public TableWrite newWrite() {
        SinkRecordConverter recordConverter =
                new SinkRecordConverter(store.options().bucket(), schema);
        return new TableWrite(store.newWrite(), recordConverter) {
            @Override
            protected void writeSinkRecord(SinkRecord record, RecordWriter writer)
                    throws Exception {
                switch (record.row().getRowKind()) {
                    case INSERT:
                    case UPDATE_AFTER:
                        writer.write(ValueKind.ADD, record.row(), GenericRowData.of(1L));
                        break;
                    case UPDATE_BEFORE:
                    case DELETE:
                        writer.write(ValueKind.ADD, record.row(), GenericRowData.of(-1L));
                        break;
                    default:
                        throw new UnsupportedOperationException(
                                "Unknown row kind " + record.row().getRowKind());
                }
            }
        };
    }

    @Override
    public FileStoreImpl store() {
        return store;
    }
}
