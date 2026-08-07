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

package org.apache.paimon.table.query;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.FileStore;
import org.apache.paimon.KeyValue;
import org.apache.paimon.KeyValueFileStore;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.data.serializer.InternalSerializers;
import org.apache.paimon.deletionvectors.DeletionVector;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.KeyValueFileReaderFactory;
import org.apache.paimon.mergetree.Levels;
import org.apache.paimon.mergetree.LookupUtils;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Filter;
import org.apache.paimon.utils.KeyComparatorSupplier;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.paimon.CoreOptions.MergeEngine.DEDUPLICATE;

/**
 * Implementation for {@link TableQuery} which looks up records directly from data files.
 *
 * <p>This query pushes primary key predicates down to the file format reader and does not build or
 * cache local lookup files. Callers are responsible for keeping the data file view up to date with
 * {@link #refreshFiles(BinaryRow, int, List, List)}.
 *
 * <p>For tables which do not enable lookup, this query only supports the deduplicate merge engine
 * without sequence fields. Deletion vectors are not supported.
 */
public class DataFileTableQuery implements TableQuery {

    private final Map<BinaryRow, Map<Integer, BucketQueryState>> tableView;
    private final CoreOptions options;
    private final Comparator<InternalRow> keyComparator;
    private final KeyValueFileReaderFactory.Builder readerFactoryBuilder;
    private final RowType rowType;
    private final List<Integer> primaryKeyFieldIndexes;
    private final InternalRow.FieldGetter[] keyFieldGetters;
    private final int startLevel;

    @Nullable private Filter<InternalRow> rowFilter;

    public DataFileTableQuery(FileStoreTable table) {
        this.options = table.coreOptions();
        this.tableView = new ConcurrentHashMap<>();

        if (options.deletionVectorsEnabled()) {
            throw new UnsupportedOperationException(
                    "Data file table query does not support deletion vectors.");
        }
        if (!options.needLookup() && options.mergeEngine() != DEDUPLICATE) {
            throw new UnsupportedOperationException(
                    "Data file table query only supports deduplicate merge engine when lookup is "
                            + "disabled, but merge engine is: "
                            + options.mergeEngine());
        }
        if (options.mergeEngine() == DEDUPLICATE && !options.sequenceField().isEmpty()) {
            throw new UnsupportedOperationException(
                    "Data file table query does not support sequence fields for deduplicate merge "
                            + "engine, but sequence fields are: "
                            + options.sequenceField());
        }

        FileStore<?> tableStore = table.store();
        if (!(tableStore instanceof KeyValueFileStore)) {
            throw new UnsupportedOperationException(
                    "Table Query only supports table with primary key.");
        }
        KeyValueFileStore store = (KeyValueFileStore) tableStore;

        this.readerFactoryBuilder = store.newReaderFactoryBuilder();
        this.keyComparator = new KeyComparatorSupplier(readerFactoryBuilder.keyType()).get();
        this.rowType = table.schema().logicalRowType();
        List<DataField> primaryKeyFields = table.schema().trimmedPrimaryKeysFields();
        this.primaryKeyFieldIndexes = new ArrayList<>(primaryKeyFields.size());
        this.keyFieldGetters = new InternalRow.FieldGetter[primaryKeyFields.size()];
        for (int i = 0; i < primaryKeyFields.size(); i++) {
            DataField field = primaryKeyFields.get(i);
            primaryKeyFieldIndexes.add(rowType.getFieldNames().indexOf(field.name()));
            keyFieldGetters[i] = InternalRow.createFieldGetter(field.type(), i);
        }
        this.startLevel = options.needLookup() ? 1 : 0;
    }

    /** Refreshes the data files of a partition and bucket. */
    public void refreshFiles(
            BinaryRow partition,
            int bucket,
            List<DataFileMeta> beforeFiles,
            List<DataFileMeta> dataFiles) {
        BucketQueryState state =
                tableView
                        .computeIfAbsent(partition, k -> new ConcurrentHashMap<>())
                        .computeIfAbsent(bucket, k -> new BucketQueryState());
        synchronized (state) {
            if (state.levels == null) {
                // Initial phase: ignore beforeFiles as they represent deletions from previous state
                state.levels = new Levels(keyComparator, dataFiles, options.numLevels());
            } else {
                // Publish a new immutable view so that lookups do not hold locks during file IO.
                Levels levels =
                        new Levels(keyComparator, state.levels.allFiles(), options.numLevels());
                levels.update(beforeFiles, dataFiles);
                state.levels = levels;
            }
        }
    }

    @Nullable
    @Override
    public InternalRow lookup(BinaryRow partition, int bucket, InternalRow key) throws IOException {
        Map<Integer, BucketQueryState> buckets = tableView.get(partition);
        if (buckets == null || buckets.isEmpty()) {
            return null;
        }
        BucketQueryState state = buckets.get(bucket);
        if (state == null) {
            return null;
        }

        Levels levels = state.levels;
        if (levels == null) {
            return null;
        }

        KeyValue kv = lookup(partition, bucket, levels, key);
        if (kv == null
                || kv.valueKind().isRetract()
                || (rowFilter != null && !rowFilter.test(kv.value()))) {
            return null;
        }
        return kv.value();
    }

    @Nullable
    private KeyValue lookup(BinaryRow partition, int bucket, Levels levels, InternalRow key)
            throws IOException {
        return LookupUtils.lookup(
                levels,
                key,
                startLevel,
                (target, level) ->
                        LookupUtils.lookup(
                                keyComparator,
                                target,
                                level,
                                (lookupKey, file) -> lookup(partition, bucket, lookupKey, file)),
                (target, level0) ->
                        LookupUtils.lookupLevel0(
                                keyComparator,
                                target,
                                level0,
                                (lookupKey, file) -> lookup(partition, bucket, lookupKey, file)));
    }

    @Nullable
    private KeyValue lookup(BinaryRow partition, int bucket, InternalRow key, DataFileMeta file)
            throws IOException {
        KeyValueFileReaderFactory readerFactory =
                readerFactoryBuilder.build(
                        partition,
                        bucket,
                        DeletionVector.emptyFactory(),
                        true,
                        primaryKeyPredicates(key));
        InternalRowSerializer valueSerializer = createValueSerializer();
        try (RecordReader<KeyValue> reader = readerFactory.createRecordReader(file)) {
            RecordReader.RecordIterator<KeyValue> batch;
            while ((batch = reader.readBatch()) != null) {
                KeyValue result = null;
                try {
                    KeyValue kv;
                    while ((kv = batch.next()) != null) {
                        if (keyComparator.compare(key, kv.key()) == 0) {
                            result =
                                    new KeyValue()
                                            .replace(
                                                    key,
                                                    kv.sequenceNumber(),
                                                    kv.valueKind(),
                                                    valueSerializer.copy(kv.value()))
                                            .setLevel(kv.level());
                            break;
                        }
                    }
                } finally {
                    batch.releaseBatch();
                }
                if (result != null) {
                    return result;
                }
            }
        }
        return null;
    }

    private List<Predicate> primaryKeyPredicates(InternalRow key) {
        PredicateBuilder builder = new PredicateBuilder(rowType);
        List<Predicate> predicates = new ArrayList<>(primaryKeyFieldIndexes.size());
        for (int i = 0; i < primaryKeyFieldIndexes.size(); i++) {
            int fieldIndex = primaryKeyFieldIndexes.get(i);
            Object literal = keyFieldGetters[i].getFieldOrNull(key);
            predicates.add(
                    literal == null
                            ? builder.isNull(fieldIndex)
                            : builder.equal(fieldIndex, literal));
        }
        return predicates;
    }

    @Override
    public DataFileTableQuery withValueProjection(int[] projection) {
        this.readerFactoryBuilder.withReadValueType(rowType.project(projection));
        return this;
    }

    public DataFileTableQuery withRowFilter(Filter<InternalRow> rowFilter) {
        this.rowFilter = rowFilter;
        return this;
    }

    @Override
    public InternalRowSerializer createValueSerializer() {
        return InternalSerializers.create(readerFactoryBuilder.readValueType());
    }

    @Override
    public void close() {
        tableView.clear();
    }

    private static class BucketQueryState {

        @Nullable private volatile Levels levels;
    }
}
