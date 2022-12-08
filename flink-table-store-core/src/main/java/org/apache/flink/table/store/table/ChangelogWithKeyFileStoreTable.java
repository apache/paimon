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
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.store.CoreOptions;
import org.apache.flink.table.store.file.KeyValue;
import org.apache.flink.table.store.file.KeyValueFileStore;
import org.apache.flink.table.store.file.WriteMode;
import org.apache.flink.table.store.file.mergetree.compact.DeduplicateMergeFunction;
import org.apache.flink.table.store.file.mergetree.compact.MergeFunctionFactory;
import org.apache.flink.table.store.file.mergetree.compact.PartialUpdateMergeFunction;
import org.apache.flink.table.store.file.mergetree.compact.aggregate.AggregateMergeFunction;
import org.apache.flink.table.store.file.operation.KeyValueFileStoreScan;
import org.apache.flink.table.store.file.predicate.Predicate;
import org.apache.flink.table.store.file.schema.DataField;
import org.apache.flink.table.store.file.schema.KeyValueFieldsExtractor;
import org.apache.flink.table.store.file.schema.RowDataType;
import org.apache.flink.table.store.file.schema.TableSchema;
import org.apache.flink.table.store.file.utils.FileStorePathFactory;
import org.apache.flink.table.store.file.utils.RecordReader;
import org.apache.flink.table.store.table.sink.SequenceGenerator;
import org.apache.flink.table.store.table.sink.SinkRecordConverter;
import org.apache.flink.table.store.table.sink.TableWrite;
import org.apache.flink.table.store.table.sink.TableWriteImpl;
import org.apache.flink.table.store.table.source.DataTableScan;
import org.apache.flink.table.store.table.source.KeyValueTableRead;
import org.apache.flink.table.store.table.source.MergeTreeSplitGenerator;
import org.apache.flink.table.store.table.source.SplitGenerator;
import org.apache.flink.table.store.table.source.TableRead;
import org.apache.flink.table.store.table.source.ValueContentRowDataRecordIterator;
import org.apache.flink.table.types.logical.RowType;

import java.util.List;
import java.util.stream.Collectors;

import static org.apache.flink.table.store.file.predicate.PredicateBuilder.and;
import static org.apache.flink.table.store.file.predicate.PredicateBuilder.pickTransformFieldMapping;
import static org.apache.flink.table.store.file.predicate.PredicateBuilder.splitAnd;
import static org.apache.flink.table.store.file.schema.TableSchema.KEY_FIELD_PREFIX;

/** {@link FileStoreTable} for {@link WriteMode#CHANGE_LOG} write mode with primary keys. */
public class ChangelogWithKeyFileStoreTable extends AbstractFileStoreTable {

    private static final long serialVersionUID = 1L;

    private transient KeyValueFileStore lazyStore;

    ChangelogWithKeyFileStoreTable(Path path, TableSchema tableSchema) {
        super(path, tableSchema);
    }

    @Override
    protected FileStoreTable copy(TableSchema newTableSchema) {
        return new ChangelogWithKeyFileStoreTable(path, newTableSchema);
    }

    @Override
    public KeyValueFileStore store() {
        if (lazyStore == null) {
            RowType rowType = tableSchema.logicalRowType();
            Configuration conf = Configuration.fromMap(tableSchema.options());
            CoreOptions.MergeEngine mergeEngine = conf.get(CoreOptions.MERGE_ENGINE);
            MergeFunctionFactory<KeyValue> mfFactory;
            switch (mergeEngine) {
                case DEDUPLICATE:
                    mfFactory = DeduplicateMergeFunction.factory();
                    break;
                case PARTIAL_UPDATE:
                    mfFactory =
                            PartialUpdateMergeFunction.factory(
                                    conf.get(CoreOptions.PARTIAL_UPDATE_IGNORE_DELETE),
                                    rowType.getChildren());
                    break;
                case AGGREGATE:
                    mfFactory =
                            AggregateMergeFunction.factory(
                                    conf,
                                    tableSchema.fieldNames(),
                                    rowType.getChildren(),
                                    tableSchema.primaryKeys());
                    break;
                default:
                    throw new UnsupportedOperationException(
                            "Unsupported merge engine: " + mergeEngine);
            }

            CoreOptions options = new CoreOptions(conf);
            KeyValueFieldsExtractor extractor = ChangelogWithKeyKeyValueFieldsExtractor.EXTRACTOR;
            lazyStore =
                    new KeyValueFileStore(
                            schemaManager(),
                            tableSchema.id(),
                            options,
                            tableSchema.logicalPartitionType(),
                            addKeyNamePrefix(tableSchema.logicalBucketKeyType()),
                            RowDataType.toRowType(false, extractor.keyFields(tableSchema)),
                            rowType,
                            extractor,
                            mfFactory);
        }
        return lazyStore;
    }

    private static RowType addKeyNamePrefix(RowType type) {
        // add prefix to avoid conflict with value
        return new RowType(
                type.getFields().stream()
                        .map(
                                f ->
                                        new RowType.RowField(
                                                KEY_FIELD_PREFIX + f.getName(),
                                                f.getType(),
                                                f.getDescription().orElse(null)))
                        .collect(Collectors.toList()));
    }

    private static List<DataField> addKeyNamePrefix(List<DataField> keyFields) {
        return keyFields.stream()
                .map(
                        f ->
                                new DataField(
                                        f.id(),
                                        KEY_FIELD_PREFIX + f.name(),
                                        f.type(),
                                        f.description()))
                .collect(Collectors.toList());
    }

    @Override
    public DataTableScan newScan() {
        KeyValueFileStoreScan scan = store().newScan();
        return new DataTableScan(scan, tableSchema, store().pathFactory(), options()) {
            @Override
            protected SplitGenerator splitGenerator(FileStorePathFactory pathFactory) {
                return new MergeTreeSplitGenerator(
                        store().newKeyComparator(),
                        store().options().splitTargetSize(),
                        store().options().splitOpenFileCost());
            }

            @Override
            protected void withNonPartitionFilter(Predicate predicate) {
                // currently we can only perform filter push down on keys
                // consider this case:
                //   data file 1: insert key = a, value = 1
                //   data file 2: update key = a, value = 2
                //   filter: value = 1
                // if we perform filter push down on values, data file 1 will be chosen, but data
                // file 2 will be ignored, and the final result will be key = a, value = 1 while the
                // correct result is an empty set
                // TODO support value filter
                List<Predicate> keyFilters =
                        pickTransformFieldMapping(
                                splitAnd(predicate),
                                tableSchema.fieldNames(),
                                tableSchema.trimmedPrimaryKeys());
                if (keyFilters.size() > 0) {
                    scan.withKeyFilter(and(keyFilters));
                }
            }
        };
    }

    @Override
    public TableRead newRead() {
        return new KeyValueTableRead(store().newRead()) {

            @Override
            public TableRead withFilter(Predicate predicate) {
                read.withFilter(predicate);
                return this;
            }

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
    public TableWrite newWrite(String commitUser) {
        final SequenceGenerator sequenceGenerator =
                store().options()
                        .sequenceField()
                        .map(field -> new SequenceGenerator(field, schema().logicalRowType()))
                        .orElse(null);
        final KeyValue kv = new KeyValue();
        return new TableWriteImpl<>(
                store().newWrite(commitUser),
                new SinkRecordConverter(tableSchema),
                record -> {
                    long sequenceNumber =
                            sequenceGenerator == null
                                    ? KeyValue.UNKNOWN_SEQUENCE
                                    : sequenceGenerator.generate(record.row());
                    return kv.replace(
                            record.primaryKey(),
                            sequenceNumber,
                            record.row().getRowKind(),
                            record.row());
                });
    }

    static class ChangelogWithKeyKeyValueFieldsExtractor implements KeyValueFieldsExtractor {
        private static final long serialVersionUID = 1L;

        static final ChangelogWithKeyKeyValueFieldsExtractor EXTRACTOR =
                new ChangelogWithKeyKeyValueFieldsExtractor();

        private ChangelogWithKeyKeyValueFieldsExtractor() {}

        @Override
        public List<DataField> keyFields(TableSchema schema) {
            return addKeyNamePrefix(schema.trimmedPrimaryKeysFields());
        }

        @Override
        public List<DataField> valueFields(TableSchema schema) {
            return schema.fields();
        }
    }
}
