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

import org.apache.flink.core.fs.Path;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.store.CoreOptions;
import org.apache.flink.table.store.file.KeyValue;
import org.apache.flink.table.store.file.KeyValueFileStore;
import org.apache.flink.table.store.file.WriteMode;
import org.apache.flink.table.store.file.mergetree.compact.ValueCountMergeFunction;
import org.apache.flink.table.store.file.operation.KeyValueFileStoreScan;
import org.apache.flink.table.store.file.predicate.Predicate;
import org.apache.flink.table.store.file.schema.DataField;
import org.apache.flink.table.store.file.schema.KeyFieldsExtractor;
import org.apache.flink.table.store.file.schema.RowDataType;
import org.apache.flink.table.store.file.schema.SchemaManager;
import org.apache.flink.table.store.file.schema.TableSchema;
import org.apache.flink.table.store.file.utils.FileStorePathFactory;
import org.apache.flink.table.store.file.utils.RecordReader;
import org.apache.flink.table.store.table.sink.SinkRecordConverter;
import org.apache.flink.table.store.table.sink.TableWrite;
import org.apache.flink.table.store.table.sink.TableWriteImpl;
import org.apache.flink.table.store.table.source.DataTableScan;
import org.apache.flink.table.store.table.source.KeyValueTableRead;
import org.apache.flink.table.store.table.source.MergeTreeSplitGenerator;
import org.apache.flink.table.store.table.source.SplitGenerator;
import org.apache.flink.table.store.table.source.TableRead;
import org.apache.flink.table.store.table.source.ValueCountRowDataRecordIterator;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;

import java.util.List;

/** {@link FileStoreTable} for {@link WriteMode#CHANGE_LOG} write mode without primary keys. */
public class ChangelogValueCountFileStoreTable extends AbstractFileStoreTable {

    private static final long serialVersionUID = 1L;

    private final KeyValueFileStore store;

    ChangelogValueCountFileStoreTable(
            Path path, SchemaManager schemaManager, TableSchema tableSchema) {
        super(path, tableSchema);
        RowType countType =
                RowType.of(
                        new LogicalType[] {new BigIntType(false)}, new String[] {"_VALUE_COUNT"});
        KeyFieldsExtractor extractor = ValueCountTableKeyFieldsExtractor.EXTRACTOR;
        this.store =
                new KeyValueFileStore(
                        schemaManager,
                        tableSchema.id(),
                        new CoreOptions(tableSchema.options()),
                        tableSchema.logicalPartitionType(),
                        tableSchema.logicalBucketKeyType(),
                        RowDataType.toRowType(false, extractor.keyFields(tableSchema)),
                        countType,
                        extractor,
                        ValueCountMergeFunction.factory());
    }

    @Override
    public DataTableScan newScan() {
        KeyValueFileStoreScan scan = store.newScan();
        return new DataTableScan(scan, tableSchema, store.pathFactory(), options()) {
            @Override
            protected SplitGenerator splitGenerator(FileStorePathFactory pathFactory) {
                return new MergeTreeSplitGenerator(
                        store.newKeyComparator(),
                        store.options().splitTargetSize(),
                        store.options().splitOpenFileCost());
            }

            @Override
            protected void withNonPartitionFilter(Predicate predicate) {
                scan.withKeyFilter(predicate);
            }
        };
    }

    @Override
    public TableRead newRead() {
        return new KeyValueTableRead(store.newRead()) {

            @Override
            public TableRead withFilter(Predicate predicate) {
                read.withFilter(predicate);
                return this;
            }

            @Override
            public TableRead withProjection(int[][] projection) {
                read.withKeyProjection(projection);
                return this;
            }

            @Override
            protected RecordReader.RecordIterator<RowData> rowDataRecordIteratorFromKv(
                    RecordReader.RecordIterator<KeyValue> kvRecordIterator) {
                return new ValueCountRowDataRecordIterator(kvRecordIterator);
            }
        };
    }

    @Override
    public TableWrite newWrite(String commitUser) {
        SinkRecordConverter recordConverter =
                new SinkRecordConverter(store.options().bucket(), tableSchema);
        final KeyValue kv = new KeyValue();
        return new TableWriteImpl<>(
                store.newWrite(commitUser),
                recordConverter,
                record -> {
                    switch (record.row().getRowKind()) {
                        case INSERT:
                        case UPDATE_AFTER:
                            kv.replace(record.row(), RowKind.INSERT, GenericRowData.of(1L));
                            break;
                        case UPDATE_BEFORE:
                        case DELETE:
                            kv.replace(record.row(), RowKind.INSERT, GenericRowData.of(-1L));
                            break;
                        default:
                            throw new UnsupportedOperationException(
                                    "Unknown row kind " + record.row().getRowKind());
                    }
                    return kv;
                });
    }

    @Override
    public KeyValueFileStore store() {
        return store;
    }

    /** {@link KeyFieldsExtractor} implementation for {@link ChangelogValueCountFileStoreTable}. */
    static class ValueCountTableKeyFieldsExtractor implements KeyFieldsExtractor {
        private static final long serialVersionUID = 1L;

        static final ValueCountTableKeyFieldsExtractor EXTRACTOR =
                new ValueCountTableKeyFieldsExtractor();

        private ValueCountTableKeyFieldsExtractor() {}

        @Override
        public List<DataField> keyFields(TableSchema schema) {
            return schema.fields();
        }
    }
}
