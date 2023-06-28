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
import org.apache.paimon.KeyValue;
import org.apache.paimon.KeyValueFileStore;
import org.apache.paimon.WriteMode;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.ManifestCacheFilter;
import org.apache.paimon.mergetree.compact.ValueCountMergeFunction;
import org.apache.paimon.operation.FileStoreScan;
import org.apache.paimon.operation.KeyValueFileStoreScan;
import org.apache.paimon.operation.Lock;
import org.apache.paimon.operation.ReverseReader;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.KeyValueFieldsExtractor;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.stats.BinaryTableStats;
import org.apache.paimon.table.sink.TableWriteImpl;
import org.apache.paimon.table.source.InnerTableRead;
import org.apache.paimon.table.source.KeyValueTableRead;
import org.apache.paimon.table.source.MergeTreeSplitGenerator;
import org.apache.paimon.table.source.SplitGenerator;
import org.apache.paimon.table.source.ValueCountRowDataRecordIterator;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;

import java.util.Collections;
import java.util.List;
import java.util.function.BiConsumer;

import static org.apache.paimon.schema.SystemColumns.VALUE_COUNT;

/** {@link FileStoreTable} for {@link WriteMode#CHANGE_LOG} write mode without primary keys. */
public class ChangelogValueCountFileStoreTable extends AbstractFileStoreTable {

    private static final long serialVersionUID = 1L;
    private transient KeyValueFileStore lazyStore;

    ChangelogValueCountFileStoreTable(FileIO fileIO, Path path, TableSchema tableSchema) {
        this(fileIO, path, tableSchema, Lock.emptyFactory());
    }

    ChangelogValueCountFileStoreTable(
            FileIO fileIO, Path path, TableSchema tableSchema, Lock.Factory lockFactory) {
        super(fileIO, path, tableSchema, lockFactory);
    }

    @Override
    protected FileStoreTable copy(TableSchema newTableSchema) {
        return new ChangelogValueCountFileStoreTable(fileIO, path, newTableSchema, lockFactory);
    }

    @Override
    public KeyValueFileStore store() {
        if (lazyStore == null) {
            KeyValueFieldsExtractor extractor = ValueCountTableKeyValueFieldsExtractor.EXTRACTOR;
            RowType countType = new RowType(extractor.valueFields(tableSchema));
            lazyStore =
                    new KeyValueFileStore(
                            fileIO,
                            schemaManager(),
                            tableSchema.id(),
                            new CoreOptions(tableSchema.options()),
                            tableSchema.logicalPartitionType(),
                            tableSchema.logicalBucketKeyType(),
                            new RowType(extractor.keyFields(tableSchema)),
                            countType,
                            extractor,
                            ValueCountMergeFunction.factory());
        }
        return lazyStore;
    }

    @Override
    public SplitGenerator splitGenerator() {
        return new MergeTreeSplitGenerator(
                store().newKeyComparator(),
                store().options().splitTargetSize(),
                store().options().splitOpenFileCost());
    }

    /**
     * Currently, the streaming read of overwrite is implemented by reversing the {@link RowKind} of
     * overwrote records to {@link RowKind#DELETE}, so only tables that have primary key support it.
     *
     * @see ReverseReader
     */
    @Override
    public boolean supportStreamingReadOverwrite() {
        return false;
    }

    @Override
    public BiConsumer<FileStoreScan, Predicate> nonPartitionFilterConsumer() {
        return (scan, predicate) -> ((KeyValueFileStoreScan) scan).withKeyFilter(predicate);
    }

    @Override
    public InnerTableRead newRead() {
        return new KeyValueTableRead(store().newRead()) {

            @Override
            public InnerTableRead withFilter(Predicate predicate) {
                read.withFilter(predicate);
                return this;
            }

            @Override
            public InnerTableRead withProjection(int[][] projection) {
                read.withKeyProjection(projection);
                return this;
            }

            @Override
            protected RecordReader.RecordIterator<InternalRow> rowDataRecordIteratorFromKv(
                    RecordReader.RecordIterator<KeyValue> kvRecordIterator) {
                return new ValueCountRowDataRecordIterator(kvRecordIterator);
            }

            @Override
            public InnerTableRead forceKeepDelete() {
                read.forceKeepDelete();
                return this;
            }
        };
    }

    @Override
    public TableWriteImpl<KeyValue> newWrite(String commitUser) {
        return newWrite(commitUser, null);
    }

    @Override
    public TableWriteImpl<KeyValue> newWrite(
            String commitUser, ManifestCacheFilter manifestFilter) {
        final KeyValue kv = new KeyValue();
        return new TableWriteImpl<>(
                store().newWrite(commitUser, manifestFilter),
                createRowKeyExtractor(),
                record -> {
                    switch (record.row().getRowKind()) {
                        case INSERT:
                        case UPDATE_AFTER:
                            kv.replace(record.row(), RowKind.INSERT, GenericRow.of(1L));
                            break;
                        case UPDATE_BEFORE:
                        case DELETE:
                            kv.replace(record.row(), RowKind.INSERT, GenericRow.of(-1L));
                            break;
                        default:
                            throw new UnsupportedOperationException(
                                    "Unknown row kind " + record.row().getRowKind());
                    }
                    return kv;
                });
    }

    @Override
    public BinaryTableStats getSchemaFieldStats(DataFileMeta dataFileMeta) {
        return dataFileMeta.keyStats();
    }

    /**
     * {@link KeyValueFieldsExtractor} implementation for {@link ChangelogValueCountFileStoreTable}.
     */
    static class ValueCountTableKeyValueFieldsExtractor implements KeyValueFieldsExtractor {
        private static final long serialVersionUID = 1L;

        static final ValueCountTableKeyValueFieldsExtractor EXTRACTOR =
                new ValueCountTableKeyValueFieldsExtractor();

        private ValueCountTableKeyValueFieldsExtractor() {}

        @Override
        public List<DataField> keyFields(TableSchema schema) {
            return schema.fields();
        }

        @Override
        public List<DataField> valueFields(TableSchema schema) {
            return Collections.singletonList(new DataField(0, VALUE_COUNT, new BigIntType(false)));
        }
    }
}
