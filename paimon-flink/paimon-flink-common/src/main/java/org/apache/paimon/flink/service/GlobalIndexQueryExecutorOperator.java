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

package org.apache.paimon.flink.service;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.flink.utils.RuntimeContextUtils;
import org.apache.paimon.service.network.NetworkUtils;
import org.apache.paimon.service.network.stats.DisabledServiceRequestStats;
import org.apache.paimon.service.server.GlobalIndexQueryServer;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.query.DataEvolutionGlobalIndexTableQuery;
import org.apache.paimon.table.query.DuplicateLookupKeyException;
import org.apache.paimon.table.query.GlobalIndexQueryServiceUtils;
import org.apache.paimon.table.query.GlobalIndexQueryServiceUtils.QuerySpec;
import org.apache.paimon.table.query.OversizedLookupValueException;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.io.File;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static org.apache.paimon.flink.service.GlobalIndexQueryBootstrapOperator.COMPLETE;
import static org.apache.paimon.flink.service.GlobalIndexQueryBootstrapOperator.NOT_READY;
import static org.apache.paimon.flink.service.GlobalIndexQueryBootstrapOperator.PUT;
import static org.apache.paimon.flink.service.GlobalIndexQueryBootstrapOperator.START;
import static org.apache.paimon.utils.SerializationUtils.deserializeBinaryRow;

/** Owns one key-hash state shard and only advertises it after a complete snapshot bootstrap. */
public class GlobalIndexQueryExecutorOperator extends AbstractStreamOperator<InternalRow>
        implements OneInputStreamOperator<InternalRow, InternalRow> {

    private static final long serialVersionUID = 1L;

    private final FileStoreTable table;
    private final String lookupField;
    private final List<String> valueFields;
    private final int numBootstraps;

    private transient QuerySpec spec;
    private transient DataEvolutionGlobalIndexTableQuery query;
    private transient GlobalIndexQueryServer server;
    private transient InetSocketAddress address;
    private transient boolean[] completedBootstraps;
    private transient String serverEpoch;
    private transient String servedSnapshotUuid;

    private long generation = Long.MIN_VALUE;
    private long snapshotId = GlobalIndexQueryServiceUtils.EMPTY_SNAPSHOT_ID;
    private boolean publishedReady;
    private String invalidReason;

    public GlobalIndexQueryExecutorOperator(
            FileStoreTable table, String lookupField, List<String> valueFields, int numBootstraps) {
        this.table = table;
        this.lookupField = lookupField;
        this.valueFields = valueFields;
        this.numBootstraps = numBootstraps;
    }

    public static RowType outputType() {
        return RowType.of(
                DataTypes.BIGINT(),
                DataTypes.BOOLEAN(),
                DataTypes.INT(),
                DataTypes.INT(),
                DataTypes.STRING(),
                DataTypes.INT(),
                DataTypes.STRING(),
                DataTypes.BIGINT(),
                DataTypes.BIGINT(),
                DataTypes.STRING(),
                DataTypes.STRING());
    }

    @Override
    public void initializeState(StateInitializationContext context) throws Exception {
        super.initializeState(context);
        this.spec = GlobalIndexQueryServiceUtils.querySpec(table, lookupField, valueFields);

        int executorId = RuntimeContextUtils.getIndexOfThisSubtask(getRuntimeContext());
        int numExecutors = RuntimeContextUtils.getNumberOfParallelSubtasks(getRuntimeContext());
        String[] tempDirs =
                getContainingTask().getEnvironment().getIOManager().getSpillingDirectoriesPaths();
        File stateRoot =
                new File(
                        tempDirs[Math.floorMod(executorId, tempDirs.length)],
                        "paimon-global-index-query-" + executorId + '-' + UUID.randomUUID());
        this.query =
                new DataEvolutionGlobalIndexTableQuery(table, lookupField, valueFields, stateRoot);

        // Executor state is local and deliberately rebuilt after a restart. Remove a stale
        // endpoint before accepting requests so missing bootstrap state can never become a MISS.
        this.serverEpoch = UUID.randomUUID().toString();
        this.server =
                new GlobalIndexQueryServer(
                        executorId,
                        numExecutors,
                        serverEpoch,
                        NetworkUtils.findHostAddress(),
                        Collections.singletonList(0).iterator(),
                        1,
                        1,
                        query,
                        new DisabledServiceRequestStats());
        try {
            server.start();
        } catch (Throwable t) {
            // Flink does not invoke operator close when initializeState fails. Release both
            // resources here so a bind/start failure cannot leak Netty threads or the local KV
            // directory until the TaskManager process exits.
            try {
                server.shutdown();
            } catch (Throwable cleanupFailure) {
                t.addSuppressed(cleanupFailure);
            }
            try {
                query.close();
            } catch (Throwable cleanupFailure) {
                t.addSuppressed(cleanupFailure);
            }
            throw new RuntimeException("Failed to start global-index query server.", t);
        }
        this.address = server.getServerAddress();
        this.completedBootstraps = new boolean[numBootstraps];
        this.servedSnapshotUuid = null;
    }

    @Override
    public void processElement(StreamRecord<InternalRow> streamRecord) throws Exception {
        InternalRow row = streamRecord.getValue();
        long eventGeneration = row.getLong(0);
        if (eventGeneration < generation) {
            return;
        }

        long eventSnapshotId = row.getLong(1);
        int type = row.getInt(2);
        if (eventGeneration > generation) {
            generation = eventGeneration;
            snapshotId = eventSnapshotId;
            Arrays.fill(completedBootstraps, false);
            publishedReady = false;
            invalidReason = null;
            if (type == NOT_READY) {
                query.markNotReady(generation, snapshotId, row.getString(7).toString());
            } else {
                query.beginRefresh(generation, snapshotId);
            }
            // A generation is published only after every executor has swapped. Withdrawing the
            // descriptor here prevents a client from discovering a partially swapped generation.
            // Clients which cached the previous descriptor are fenced by servedGeneration.
            emitAddress(false, type == NOT_READY ? row.getString(7).toString() : "Refreshing");
        }

        if (type == START || type == NOT_READY) {
            return;
        }
        if (type == PUT) {
            if (invalidReason != null) {
                return;
            }
            // Schemaless deserialization keeps an arity prefix before the row. Normalize the
            // offset because BinaryRow#anyNull currently reads null bits from segment offset 0.
            BinaryRow key = deserializeBinaryRow(row.getBinary(5)).copy();
            int executorId = RuntimeContextUtils.getIndexOfThisSubtask(getRuntimeContext());
            int numExecutors = RuntimeContextUtils.getNumberOfParallelSubtasks(getRuntimeContext());
            if (GlobalIndexQueryServiceUtils.route(key, numExecutors) != executorId) {
                throw new IllegalStateException(
                        "Global-index bootstrap row was sent to the wrong executor shard.");
            }
            try {
                query.put(generation, key, deserializeBinaryRow(row.getBinary(6)).copy());
            } catch (DuplicateLookupKeyException | OversizedLookupValueException e) {
                invalidReason = e.getMessage();
                query.markNotReady(generation, snapshotId, invalidReason);
                // Keep discovery explicitly NOT_READY and expose the validation reason. The old
                // shadow state stays allocated for atomic cleanup, but its accepted generation was
                // already fenced when START arrived.
                emitAddress(false, invalidReason);
            }
            return;
        }
        if (type == COMPLETE) {
            if (invalidReason != null) {
                return;
            }
            int bootstrapId = row.getInt(4);
            if (bootstrapId < 0 || bootstrapId >= completedBootstraps.length) {
                throw new IllegalArgumentException("Invalid bootstrap subtask " + bootstrapId);
            }
            completedBootstraps[bootstrapId] = true;
            if (!publishedReady && allBootstrapsComplete()) {
                query.finishRefresh(generation);
                servedSnapshotUuid =
                        query.servedSnapshotId() == GlobalIndexQueryServiceUtils.EMPTY_SNAPSHOT_ID
                                ? null
                                : table.snapshotManager().snapshot(query.servedSnapshotId()).uuid();
                publishedReady = true;
                emitAddress(true, "");
            }
            return;
        }
        throw new IllegalArgumentException("Unknown global-index bootstrap event type " + type);
    }

    private boolean allBootstrapsComplete() {
        for (boolean complete : completedBootstraps) {
            if (!complete) {
                return false;
            }
        }
        return true;
    }

    private void emitAddress(boolean ready, String reason) {
        long descriptorGeneration = ready ? query.servedGeneration() : generation;
        long descriptorSnapshotId = ready ? query.servedSnapshotId() : snapshotId;
        output.collect(
                new StreamRecord<>(
                        GenericRow.of(
                                generation,
                                ready,
                                RuntimeContextUtils.getNumberOfParallelSubtasks(
                                        getRuntimeContext()),
                                RuntimeContextUtils.getIndexOfThisSubtask(getRuntimeContext()),
                                BinaryString.fromString(address.getHostName()),
                                address.getPort(),
                                BinaryString.fromString(reason),
                                descriptorGeneration,
                                descriptorSnapshotId,
                                !ready || servedSnapshotUuid == null
                                        ? null
                                        : BinaryString.fromString(servedSnapshotUuid),
                                BinaryString.fromString(serverEpoch))));
    }

    @Override
    public void close() throws Exception {
        try {
            if (server != null) {
                server.shutdown();
            }
            if (query != null) {
                query.close();
            }
        } finally {
            super.close();
        }
    }
}
