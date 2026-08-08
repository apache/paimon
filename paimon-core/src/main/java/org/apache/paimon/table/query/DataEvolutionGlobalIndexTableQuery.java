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

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.BinaryRowSerializer;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.data.serializer.InternalSerializers;
import org.apache.paimon.lookup.ValueState;
import org.apache.paimon.lookup.local.LocalKvStateFactory;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.FileIOUtils;
import org.apache.paimon.utils.TypeUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.apache.paimon.CoreOptions.LOOKUP_CACHE_ROWS;
import static org.apache.paimon.table.query.GlobalIndexQueryServiceUtils.normalizeKey;
import static org.apache.paimon.table.query.GlobalIndexQueryServiceUtils.querySpec;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * Shard-local materialized query for an append table whose completeness is gated by a global BTree
 * index.
 *
 * <p>A refresh builds a shadow key-to-single-value state. The previous state remains allocated
 * until the new generation is atomically swapped in, but request admission is fenced as soon as a
 * newer generation is observed. This fail-closed gap prevents a key in an unindexed append tail
 * from being interpreted as a table miss through a cached endpoint. A fresh process has no active
 * generation and also fails closed until bootstrap completes. Null keys are outside the lookup
 * domain. Duplicate non-null keys or a projected value which cannot fit in one protocol response
 * invalidate the whole building generation before it can become ready.
 */
public class DataEvolutionGlobalIndexTableQuery implements TableQuery {

    private static final Logger LOG =
            LoggerFactory.getLogger(DataEvolutionGlobalIndexTableQuery.class);

    private final FileStoreTable table;
    private final GlobalIndexQueryServiceUtils.QuerySpec spec;
    private final File stateRoot;
    private final BinaryRowSerializer keySerializer;
    private final InternalRowSerializer logicalKeySerializer;
    private final InternalRowSerializer valueSerializer;
    private final int[] valueProjection;
    private final ReadWriteLock lock;

    @Nullable private LocalKvStateFactory activeStateFactory;
    @Nullable private ValueState<BinaryRow, BinaryRow> activeState;
    @Nullable private File activeStatePath;
    @Nullable private LocalKvStateFactory buildingStateFactory;
    @Nullable private ValueState<BinaryRow, BinaryRow> buildingState;
    @Nullable private File buildingStatePath;
    private long latestGeneration;
    private long buildingSnapshotId;
    private long servedGeneration;
    private long servedSnapshotId;
    private boolean buildingDuplicateDetected;
    private boolean buildingOversizedValueDetected;
    private String notReadyReason;

    public DataEvolutionGlobalIndexTableQuery(
            FileStoreTable table, String lookupField, List<String> valueFields, File stateRoot) {
        this.table = table;
        this.spec = querySpec(table, lookupField, valueFields);
        this.stateRoot = stateRoot;
        RowType keyType = TypeUtils.project(table.rowType(), new int[] {spec.lookupPosition()});
        RowType valueType = TypeUtils.project(table.rowType(), spec.valuePositions());
        this.keySerializer = new BinaryRowSerializer(keyType.getFieldCount());
        this.logicalKeySerializer = InternalSerializers.create(keyType);
        this.valueSerializer = InternalSerializers.create(valueType);
        this.valueProjection = spec.valuePositions();
        this.lock = new ReentrantReadWriteLock();
        this.latestGeneration = Long.MIN_VALUE;
        this.buildingSnapshotId = GlobalIndexQueryServiceUtils.EMPTY_SNAPSHOT_ID;
        this.servedGeneration = Long.MIN_VALUE;
        this.servedSnapshotId = GlobalIndexQueryServiceUtils.EMPTY_SNAPSHOT_ID;
        this.buildingDuplicateDetected = false;
        this.buildingOversizedValueDetected = false;
        this.notReadyReason = "Query service bootstrap has not started.";
    }

    public GlobalIndexQueryServiceUtils.QuerySpec spec() {
        return spec;
    }

    public void beginRefresh(long generation, long snapshotId) throws IOException {
        lock.writeLock().lock();
        try {
            checkArgument(
                    generation > this.latestGeneration,
                    "Refresh generation %s must be newer than %s.",
                    generation,
                    this.latestGeneration);
            this.latestGeneration = generation;
            this.buildingSnapshotId = snapshotId;
            this.buildingDuplicateDetected = false;
            this.buildingOversizedValueDetected = false;
            this.notReadyReason = "Refreshing snapshot " + snapshotId + '.';
            closeBuildingState();
            Options options = table.coreOptions().toConfiguration();
            this.buildingStatePath = new File(stateRoot, "generation-" + generation);
            FileIOUtils.deleteDirectoryQuietly(buildingStatePath);
            this.buildingStateFactory =
                    new LocalKvStateFactory(
                            buildingStatePath.toString(), options, null, null, false);
            this.buildingState =
                    buildingStateFactory.valueState(
                            "global-index-query",
                            keySerializer,
                            new BinaryRowSerializer(valueSerializer.getArity()),
                            options.get(LOOKUP_CACHE_ROWS));
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void put(long generation, BinaryRow key, BinaryRow value) throws IOException {
        lock.writeLock().lock();
        try {
            checkGeneration(generation);
            long serializedValueBytes = Integer.BYTES + (long) value.getSizeInBytes();
            if (serializedValueBytes > GlobalIndexQueryServiceUtils.MAX_TOTAL_VALUE_BYTES) {
                buildingOversizedValueDetected = true;
                throw new OversizedLookupValueException(
                        String.format(
                                "Projected value for lookup field '%s' is %s serialized bytes; the query protocol limit is %s bytes.",
                                spec.lookupField(),
                                serializedValueBytes,
                                GlobalIndexQueryServiceUtils.MAX_TOTAL_VALUE_BYTES));
            }
            BinaryRow normalizedKey = normalizeKey(key);
            checkArgument(
                    !normalizedKey.anyNull(), "Global-index query lookup key must not be null.");
            if (currentBuildingState().get(normalizedKey) != null) {
                buildingDuplicateDetected = true;
                throw new DuplicateLookupKeyException(
                        String.format(
                                "Lookup field '%s' is configured as unique but a duplicate key was found while bootstrapping snapshot %s.",
                                spec.lookupField(), buildingSnapshotId));
            }
            currentBuildingState().put(normalizedKey, value.copy());
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void finishRefresh(long generation) throws IOException {
        lock.writeLock().lock();
        try {
            checkGeneration(generation);
            checkArgument(buildingState != null, "Query state has not been initialized.");
            checkArgument(
                    !buildingDuplicateDetected,
                    "Duplicate lookup keys prevent generation %s from becoming ready.",
                    generation);
            checkArgument(
                    !buildingOversizedValueDetected,
                    "An oversized projected value prevents generation %s from becoming ready.",
                    generation);
            LocalKvStateFactory oldStateFactory = activeStateFactory;
            File oldStatePath = activeStatePath;
            activeStateFactory = buildingStateFactory;
            activeState = buildingState;
            activeStatePath = buildingStatePath;
            servedGeneration = generation;
            servedSnapshotId = buildingSnapshotId;
            buildingStateFactory = null;
            buildingState = null;
            buildingStatePath = null;
            this.notReadyReason = "";
            closeOldActiveState(oldStateFactory, oldStatePath);
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void markNotReady(long generation, long snapshotId, String reason) throws IOException {
        lock.writeLock().lock();
        try {
            if (generation < this.latestGeneration) {
                return;
            }
            this.latestGeneration = generation;
            this.buildingSnapshotId = snapshotId;
            this.buildingDuplicateDetected = false;
            this.buildingOversizedValueDetected = false;
            this.notReadyReason = reason;
            closeBuildingState();
        } finally {
            lock.writeLock().unlock();
        }
    }

    public boolean ready() {
        lock.readLock().lock();
        try {
            return activeState != null && latestGeneration == servedGeneration;
        } finally {
            lock.readLock().unlock();
        }
    }

    @Nullable
    @Override
    public InternalRow lookup(BinaryRow partition, int bucket, InternalRow key) throws IOException {
        // LocalKv state and its codecs reuse mutable serializers. Serialize lookups on one
        // executor instead of allowing concurrent read-lock holders to corrupt shared buffers.
        lock.writeLock().lock();
        try {
            if (activeState == null || latestGeneration != servedGeneration) {
                throw new QueryServiceNotReadyException(
                        String.format(
                                "Global-index query service is not ready for generation %s and snapshot %s: %s",
                                latestGeneration, buildingSnapshotId, notReadyReason));
            }
            BinaryRow binaryKey = valueToBinaryKey(key);
            checkArgument(
                    !binaryKey.anyNull(), "Global-index query does not support null lookup keys.");
            BinaryRow value = currentActiveState().get(binaryKey);
            return value == null ? null : value.copy();
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public DataEvolutionGlobalIndexTableQuery withValueProjection(int[] projection) {
        checkArgument(
                Arrays.equals(valueProjection, projection),
                "Global-index query value projection is fixed by the service ID.");
        return this;
    }

    @Override
    public InternalRowSerializer createValueSerializer() {
        return valueSerializer;
    }

    public long servedGeneration() {
        lock.readLock().lock();
        try {
            return servedGeneration;
        } finally {
            lock.readLock().unlock();
        }
    }

    /** The newest generation observed by this executor, including one which is not ready yet. */
    public long latestGeneration() {
        lock.readLock().lock();
        try {
            return latestGeneration;
        } finally {
            lock.readLock().unlock();
        }
    }

    public long servedSnapshotId() {
        lock.readLock().lock();
        try {
            return servedSnapshotId;
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void close() throws IOException {
        lock.writeLock().lock();
        try {
            IOException failure = null;
            try {
                closeBuildingState();
            } catch (IOException e) {
                failure = e;
            }
            try {
                closeActiveState();
            } catch (IOException e) {
                if (failure == null) {
                    failure = e;
                } else {
                    failure.addSuppressed(e);
                }
            } finally {
                FileIOUtils.deleteDirectoryQuietly(stateRoot);
            }
            if (failure != null) {
                throw failure;
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    private void checkGeneration(long generation) {
        checkArgument(
                generation == this.latestGeneration,
                "Received generation %s while current generation is %s.",
                generation,
                this.latestGeneration);
    }

    private BinaryRow valueToBinaryKey(InternalRow key) {
        if (key instanceof BinaryRow) {
            // Schemaless protocol deserialization may return a row at a non-zero segment offset.
            // Copy to an offset-zero row before using offset-insensitive BinaryRow methods.
            return normalizeKey((BinaryRow) key);
        }
        return normalizeKey(logicalKeySerializer.toBinaryRow(key));
    }

    private ValueState<BinaryRow, BinaryRow> currentActiveState() {
        if (activeState == null) {
            throw new IllegalStateException("Global-index query state is not initialized.");
        }
        return activeState;
    }

    private ValueState<BinaryRow, BinaryRow> currentBuildingState() {
        if (buildingState == null) {
            throw new IllegalStateException("Global-index query refresh has not started.");
        }
        return buildingState;
    }

    private void closeActiveState() throws IOException {
        activeState = null;
        LocalKvStateFactory stateFactory = activeStateFactory;
        File statePath = activeStatePath;
        activeStateFactory = null;
        activeStatePath = null;
        try {
            if (stateFactory != null) {
                stateFactory.close();
            }
        } finally {
            if (statePath != null) {
                FileIOUtils.deleteDirectoryQuietly(statePath);
            }
        }
    }

    private void closeOldActiveState(
            @Nullable LocalKvStateFactory oldStateFactory, @Nullable File oldStatePath) {
        try {
            if (oldStateFactory != null) {
                oldStateFactory.close();
            }
        } catch (IOException e) {
            // The new generation is already atomically active. Cleanup failure must not roll it
            // back or expose a transient not-ready state.
            LOG.warn("Failed to close previous global-index query generation.", e);
        }
        if (oldStatePath != null) {
            FileIOUtils.deleteDirectoryQuietly(oldStatePath);
        }
    }

    private void closeBuildingState() throws IOException {
        buildingState = null;
        LocalKvStateFactory stateFactory = buildingStateFactory;
        File statePath = buildingStatePath;
        buildingStateFactory = null;
        buildingStatePath = null;
        try {
            if (stateFactory != null) {
                stateFactory.close();
            }
        } finally {
            if (statePath != null) {
                FileIOUtils.deleteDirectoryQuietly(statePath);
            }
        }
    }
}
