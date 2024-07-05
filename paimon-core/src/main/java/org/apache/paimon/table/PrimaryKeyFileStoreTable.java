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
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.manifest.ManifestCacheFilter;
import org.apache.paimon.mergetree.compact.LookupMergeFunction;
import org.apache.paimon.mergetree.compact.MergeFunctionFactory;
import org.apache.paimon.operation.FileStoreScan;
import org.apache.paimon.operation.KeyValueFileStoreScan;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.schema.KeyValueFieldsExtractor;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.query.LocalTableQuery;
import org.apache.paimon.table.sink.TableWriteImpl;
import org.apache.paimon.table.source.InnerTableRead;
import org.apache.paimon.table.source.KeyValueTableRead;
import org.apache.paimon.table.source.MergeTreeSplitGenerator;
import org.apache.paimon.table.source.SplitGenerator;
import org.apache.paimon.types.RowType;

import java.util.List;
import java.util.function.BiConsumer;

import static org.apache.paimon.predicate.PredicateBuilder.and;
import static org.apache.paimon.predicate.PredicateBuilder.pickTransformFieldMapping;
import static org.apache.paimon.predicate.PredicateBuilder.splitAnd;

/** {@link FileStoreTable} for primary key table. */
class PrimaryKeyFileStoreTable extends AbstractFileStoreTable {

    private static final long serialVersionUID = 1L;

    private transient KeyValueFileStore lazyStore;

    PrimaryKeyFileStoreTable(FileIO fileIO, Path path, TableSchema tableSchema) {
        this(fileIO, path, tableSchema, CatalogEnvironment.empty());
    }

    PrimaryKeyFileStoreTable(
            FileIO fileIO,
            Path path,
            TableSchema tableSchema,
            CatalogEnvironment catalogEnvironment) {
        super(fileIO, path, tableSchema, catalogEnvironment);
    }

    @Override
    public FileStoreTable copy(TableSchema newTableSchema) {
        return new PrimaryKeyFileStoreTable(fileIO, path, newTableSchema, catalogEnvironment);
    }

    @Override
    public KeyValueFileStore store() {
        if (lazyStore == null) {
            RowType rowType = tableSchema.logicalRowType();
            Options conf = Options.fromMap(tableSchema.options());
            CoreOptions options = new CoreOptions(conf);
            KeyValueFieldsExtractor extractor =
                    PrimaryKeyTableUtils.PrimaryKeyFieldsExtractor.EXTRACTOR;

            MergeFunctionFactory<KeyValue> mfFactory =
                    PrimaryKeyTableUtils.createMergeFunctionFactory(tableSchema, extractor);
            if (options.needLookup()) {
                mfFactory =
                        LookupMergeFunction.wrap(
                                mfFactory, new RowType(extractor.keyFields(tableSchema)), rowType);
            }

            lazyStore =
                    new KeyValueFileStore(
                            fileIO(),
                            schemaManager(),
                            tableSchema,
                            tableSchema.crossPartitionUpdate(),
                            options,
                            tableSchema.logicalPartitionType(),
                            PrimaryKeyTableUtils.addKeyNamePrefix(
                                    tableSchema.logicalBucketKeyType()),
                            new RowType(extractor.keyFields(tableSchema)),
                            rowType,
                            extractor,
                            mfFactory,
                            name(),
                            catalogEnvironment);
        }
        return lazyStore;
    }

    @Override
    protected SplitGenerator splitGenerator() {
        CoreOptions options = store().options();
        return new MergeTreeSplitGenerator(
                store().newKeyComparator(),
                options.splitTargetSize(),
                options.splitOpenFileCost(),
                options.deletionVectorsEnabled(),
                options.mergeEngine());
    }

    @Override
    public boolean supportStreamingReadOverwrite() {
        return new CoreOptions(tableSchema.options()).streamingReadOverwrite();
    }

    @Override
    protected BiConsumer<FileStoreScan, Predicate> nonPartitionFilterConsumer() {
        return (scan, predicate) -> {
            // currently we can only perform filter push down on keys
            // consider this case:
            //   data file 1: insert key = a, value = 1
            //   data file 2: update key = a, value = 2
            //   filter: value = 1
            // if we perform filter push down on values, data file 1 will be chosen, but data
            // file 2 will be ignored, and the final result will be key = a, value = 1 while the
            // correct result is an empty set
            List<Predicate> keyFilters =
                    pickTransformFieldMapping(
                            splitAnd(predicate),
                            tableSchema.fieldNames(),
                            tableSchema.trimmedPrimaryKeys());
            if (keyFilters.size() > 0) {
                ((KeyValueFileStoreScan) scan).withKeyFilter(and(keyFilters));
            }

            // support value filter in bucket level
            ((KeyValueFileStoreScan) scan).withValueFilter(predicate);
        };
    }

    @Override
    public InnerTableRead newRead() {
        return new KeyValueTableRead(
                () -> store().newRead(), () -> store().newBatchRawFileRead(), schema());
    }

    @Override
    public TableWriteImpl<KeyValue> newWrite(String commitUser) {
        return newWrite(commitUser, null);
    }

    @Override
    public TableWriteImpl<KeyValue> newWrite(
            String commitUser, ManifestCacheFilter manifestFilter) {
        KeyValue kv = new KeyValue();
        return new TableWriteImpl<>(
                store().newWrite(commitUser, manifestFilter),
                createRowKeyExtractor(),
                (record, rowKind) ->
                        kv.replace(
                                record.primaryKey(),
                                KeyValue.UNKNOWN_SEQUENCE,
                                rowKind,
                                record.row()),
                rowKindGenerator(),
                CoreOptions.fromMap(tableSchema.options()).ignoreDelete());
    }

    @Override
    public LocalTableQuery newLocalTableQuery() {
        return new LocalTableQuery(this);
    }
}
