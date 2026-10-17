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

package org.apache.paimon.flink.query;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.data.serializer.InternalSerializers;
import org.apache.paimon.flink.lookup.FullCacheLookupTable;
import org.apache.paimon.flink.lookup.LookupFileStoreTable;
import org.apache.paimon.flink.lookup.LookupStreamingReader;
import org.apache.paimon.flink.lookup.PrimaryKeyLookupTable;
import org.apache.paimon.flink.lookup.ReopenException;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.query.TableQuery;
import org.apache.paimon.table.sink.ChannelComputer;
import org.apache.paimon.table.source.OutOfRangeException;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.FileIOUtils;
import org.apache.paimon.utils.ProjectedRow;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.IntStream;

import static org.apache.paimon.flink.FlinkConnectorOptions.LOOKUP_CACHE_MODE;
import static org.apache.paimon.flink.FlinkConnectorOptions.LOOKUP_REFRESH_ASYNC;
import static org.apache.paimon.flink.FlinkConnectorOptions.LookupCacheMode.FULL;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * A materialized primary-key cache for one query-service executor.
 *
 * <p>Data reads use the same partition/bucket assignment as remote requests. Cache refresh runs on
 * the Flink operator thread; requests run on the query server thread. Both are serialized here
 * because the cache and its serializers are not thread-safe.
 */
public class FullCacheTableQuery implements TableQuery {

    private final FileStoreTable table;
    private final File directory;
    private final int executorId;
    private final int numExecutors;
    private final InternalRow.FieldGetter[] keyGetters;
    private final boolean[] partitionKey;
    @Nullable private FullCacheLookupTable cache;
    @Nullable private IOException failure;
    private boolean closed;
    @Nullable private ProjectedRow valueProjection;
    private RowType valueType;
    private InternalRowSerializer valueSerializer;

    public FullCacheTableQuery(
            FileStoreTable table, File tempDirectory, int executorId, int numExecutors)
            throws Exception {
        checkArgument(
                numExecutors > 0 && executorId >= 0 && executorId < numExecutors,
                "Invalid query-service executor assignment.");
        checkArgument(
                table.bucketMode() == BucketMode.HASH_FIXED && !table.primaryKeys().isEmpty(),
                "Full cache Query Service requires a fixed-bucket primary-key table.");
        Map<String, String> options = new HashMap<>();
        options.put(LOOKUP_CACHE_MODE.key(), FULL.name());
        // The operator already refreshes independently of the RPC threads. Keep refresh failures
        // synchronous so a partially applied snapshot fails the service instead of serving stale
        // data.
        options.put(LOOKUP_REFRESH_ASYNC.key(), "false");
        options.put(CoreOptions.LOOKUP_CACHE_BLOB_DESCRIPTOR.key(), "false");
        this.table = table.copy(options);
        this.directory = new File(tempDirectory, "query-full-cache-" + UUID.randomUUID());
        this.executorId = executorId;
        this.numExecutors = numExecutors;
        this.valueType = table.rowType();
        this.valueSerializer = InternalSerializers.create(valueType);

        List<String> primaryKeys = table.primaryKeys();
        List<String> partitionKeys = table.partitionKeys();
        List<String> trimmedKeys = table.schema().trimmedPrimaryKeys();
        this.keyGetters = new InternalRow.FieldGetter[primaryKeys.size()];
        this.partitionKey = new boolean[primaryKeys.size()];
        for (int i = 0; i < primaryKeys.size(); i++) {
            String name = primaryKeys.get(i);
            int partitionIndex = partitionKeys.indexOf(name);
            partitionKey[i] = partitionIndex >= 0;
            keyGetters[i] =
                    InternalRow.createFieldGetter(
                            table.rowType().getField(name).type(),
                            partitionKey[i] ? partitionIndex : trimmedKeys.indexOf(name));
        }

        try {
            openCache();
        } catch (Exception e) {
            try {
                close();
            } catch (Exception cleanupFailure) {
                e.addSuppressed(cleanupFailure);
            }
            throw e;
        }
    }

    private void openCache() throws Exception {
        FullCacheLookupTable.Context context =
                new FullCacheLookupTable.Context(
                        table,
                        IntStream.range(0, table.rowType().getFieldCount()).toArray(),
                        null,
                        null,
                        directory,
                        table.primaryKeys(),
                        null);
        cache =
                new PrimaryKeyLookupTable(
                        context,
                        table.coreOptions().toConfiguration().get(CoreOptions.LOOKUP_CACHE_ROWS),
                        table.primaryKeys()) {
                    @Override
                    protected LookupStreamingReader createReader(
                            LookupFileStoreTable readerTable, @Nullable Predicate scanPredicate) {
                        return super.createReader(readerTable, scanPredicate)
                                .withPartitionBucketFilter(FullCacheTableQuery.this::owns);
                    }
                };
        cache.open();
    }

    private boolean owns(BinaryRow partition, int bucket) {
        return ChannelComputer.select(partition, bucket, numExecutors) == executorId;
    }

    /** Catch up this shard without requiring a client lookup to trigger loading. */
    public synchronized void refresh() throws Exception {
        checkAvailable();
        try {
            Long nextSnapshot = cache.nextSnapshotId();
            Long earliestSnapshot = table.snapshotManager().earliestSnapshotId();
            if (nextSnapshot != null
                    && earliestSnapshot != null
                    && nextSnapshot < earliestSnapshot) {
                rebuildCache();
                return;
            }
            try {
                cache.refresh();
            } catch (OutOfRangeException | ReopenException e) {
                // Rebuild from a fresh snapshot after expiration or overwrite. Never reopen a
                // closed cache instance or publish a half-built replacement.
                rebuildCache();
            }
        } catch (Exception e) {
            failure = new IOException("Failed to refresh the query-service full cache.", e);
            throw e;
        }
    }

    private void rebuildCache() throws Exception {
        FullCacheLookupTable previous = cache;
        cache = null;
        previous.close();
        openCache();
    }

    @Nullable
    @Override
    public synchronized InternalRow lookup(BinaryRow partition, int bucket, InternalRow key)
            throws IOException {
        checkAvailable();
        if (!owns(partition, bucket)) {
            return null;
        }
        // The wire key excludes partition columns. Restore primary-key order, including
        // interleaved partition fields, before querying the full-cache state.
        GenericRow fullKey = new GenericRow(keyGetters.length);
        for (int i = 0; i < keyGetters.length; i++) {
            fullKey.setField(i, keyGetters[i].getFieldOrNull(partitionKey[i] ? partition : key));
        }
        List<InternalRow> values = cache.get(fullKey);
        if (values.isEmpty()) {
            return null;
        }
        InternalRow value = values.get(0);
        if (valueProjection != null) {
            value = valueProjection.replaceRow(value);
        }
        // RPC serialization happens after releasing this monitor.
        return valueSerializer.copy(value);
    }

    private void checkAvailable() throws IOException {
        if (closed) {
            throw new IOException("Query-service full cache is closed.");
        }
        if (failure != null) {
            throw failure;
        }
    }

    @Override
    public synchronized FullCacheTableQuery withValueProjection(int[] projection) {
        this.valueType = table.rowType().project(projection);
        this.valueSerializer = InternalSerializers.create(valueType);
        this.valueProjection = ProjectedRow.from(projection.clone());
        return this;
    }

    @Override
    public synchronized InternalRowSerializer createValueSerializer() {
        return InternalSerializers.create(valueType);
    }

    @Override
    public synchronized void close() throws IOException {
        closed = true;
        FullCacheLookupTable previous = cache;
        cache = null;
        try {
            if (previous != null) {
                previous.close();
            }
        } finally {
            FileIOUtils.deleteDirectory(directory);
        }
    }
}
