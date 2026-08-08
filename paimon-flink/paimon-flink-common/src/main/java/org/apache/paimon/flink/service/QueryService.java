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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.flink.utils.InternalTypeInfo;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.query.GlobalIndexQueryServiceUtils;
import org.apache.paimon.table.query.GlobalIndexQuerySnapshotLease;
import org.apache.paimon.table.sink.ChannelComputer;
import org.apache.paimon.types.BlobType;
import org.apache.paimon.utils.Preconditions;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;
import java.util.Collections;
import java.util.List;

import static org.apache.paimon.flink.sink.FlinkStreamPartitioner.partition;
import static org.apache.paimon.table.query.GlobalIndexQueryServiceUtils.persistedCoreOptions;

/** A class to build Query Service topology. */
public class QueryService {

    public static final Duration DEFAULT_LEASE_GRACE_PERIOD = Duration.ofMinutes(10);
    public static final Duration MIN_FAILOVER_RECOVERY_MARGIN = Duration.ofMinutes(1);

    public static void build(StreamExecutionEnvironment env, Table table, int parallelism) {
        ReadableConfig conf = env.getConfiguration();
        Preconditions.checkArgument(
                conf.get(ExecutionOptions.RUNTIME_MODE) == RuntimeExecutionMode.STREAMING,
                "Query Service only supports streaming mode.");

        FileStoreTable storeTable = (FileStoreTable) table;
        if (storeTable.bucketMode() != BucketMode.HASH_FIXED
                || storeTable.schema().primaryKeys().isEmpty()) {
            throw new UnsupportedOperationException(
                    "The bucket mode of "
                            + table.name()
                            + " is not fixed or the table has no primary key.");
        }

        DataStream<InternalRow> stream = QueryFileMonitor.build(env, table);
        stream = partition(stream, QueryFileMonitor.createChannelComputer(), parallelism);

        QueryExecutorOperator executorOperator = new QueryExecutorOperator(table);
        DataStreamSink<?> sink =
                stream.transform(
                                "Executor",
                                InternalTypeInfo.fromRowType(QueryExecutorOperator.outputType()),
                                executorOperator)
                        .setParallelism(parallelism)
                        .sinkTo(new QueryAddressRegister(table))
                        .setParallelism(1);

        sink.getTransformation().setMaxParallelism(1);
    }

    /**
     * Build a snapshot-scoped query service for a data-evolution append table global index.
     *
     * <p>The BTree index is an exact coverage gate; requests read key-hash-sharded materialized
     * state, not BTree files. The monitor plans each pinned snapshot once, and bootstrap readers
     * consume every planned split exactly once before shuffling projected key/value rows. A new
     * fully indexed snapshot triggers a full rebuild. Discovery is NOT_READY during that rebuild
     * and is published atomically only after all executors report the same served snapshot. This v1
     * behavior deliberately favors fail-closed correctness over refresh availability and does not
     * expose the old descriptor through discovery during refresh. A per-attempt consumer lease pins
     * the minimum active/building snapshot, inherits the previous attempt's live pin after
     * failover, and advances only after discovery acknowledges the handover plus a configurable
     * grace period.
     *
     * <p>Lookup is exact and does not normalize application identifiers such as URLs. Callers may
     * normalize keys before invoking the client. Null table keys are skipped, null request keys are
     * rejected, and any duplicate non-null key prevents the generation from becoming ready.
     * Selected BLOB value fields are read in descriptor mode and cached as serialized {@code
     * BlobDescriptor} bytes; the service never materializes their payload into query state.
     */
    public static void build(
            StreamExecutionEnvironment env,
            Table table,
            int parallelism,
            String lookupField,
            List<String> valueFields) {
        FileStoreTable storeTable = (FileStoreTable) table;
        GlobalIndexQueryServiceUtils.QuerySpec spec =
                GlobalIndexQueryServiceUtils.querySpec(storeTable, lookupField, valueFields);
        build(
                env,
                table,
                parallelism,
                lookupField,
                valueFields,
                "global-index-query-" + spec.serviceId(),
                DEFAULT_LEASE_GRACE_PERIOD);
    }

    public static void build(
            StreamExecutionEnvironment env,
            Table table,
            int parallelism,
            String lookupField,
            List<String> valueFields,
            String consumerIdPrefix,
            Duration leaseGracePeriod) {
        ReadableConfig conf = env.getConfiguration();
        Preconditions.checkArgument(
                conf.get(ExecutionOptions.RUNTIME_MODE) == RuntimeExecutionMode.STREAMING,
                "Query Service only supports streaming mode.");
        Preconditions.checkArgument(parallelism > 0, "Parallelism must be positive.");

        FileStoreTable storeTable = (FileStoreTable) table;
        GlobalIndexQueryServiceUtils.QuerySpec spec =
                GlobalIndexQueryServiceUtils.querySpec(storeTable, lookupField, valueFields);
        if (consumerIdPrefix == null) {
            consumerIdPrefix = "global-index-query-" + spec.serviceId();
        }
        GlobalIndexQuerySnapshotLease.validateConsumerIdPrefix(consumerIdPrefix);
        Preconditions.checkArgument(
                leaseGracePeriod != null && !leaseGracePeriod.isNegative(),
                "Lease grace period must not be negative.");

        Duration persistedExpiration = persistedCoreOptions(storeTable).consumerExpireTime();
        validateLeaseTiming(persistedExpiration, leaseGracePeriod);
        FileStoreTable queryTable = descriptorReadTable(storeTable, spec);

        DataStream<InternalRow> snapshots =
                GlobalIndexQuerySnapshotMonitor.build(
                        env,
                        queryTable,
                        lookupField,
                        valueFields,
                        parallelism,
                        consumerIdPrefix,
                        leaseGracePeriod);
        snapshots = partition(snapshots, new GlobalIndexEventChannelComputer(), parallelism);
        DataStream<InternalRow> cacheEvents =
                snapshots
                        .transform(
                                "GlobalIndexBootstrap",
                                InternalTypeInfo.fromRowType(
                                        GlobalIndexQueryBootstrapOperator.outputType()),
                                new GlobalIndexQueryBootstrapOperator(
                                        queryTable, lookupField, valueFields, parallelism))
                        .setParallelism(parallelism);
        cacheEvents = partition(cacheEvents, new GlobalIndexEventChannelComputer(), parallelism);

        DataStreamSink<?> sink =
                cacheEvents
                        .transform(
                                "GlobalIndexExecutor",
                                InternalTypeInfo.fromRowType(
                                        GlobalIndexQueryExecutorOperator.outputType()),
                                new GlobalIndexQueryExecutorOperator(
                                        queryTable, lookupField, valueFields, parallelism))
                        .setParallelism(parallelism)
                        .sinkTo(new GlobalIndexQueryAddressRegister(queryTable, spec))
                        .setParallelism(1);
        sink.getTransformation().setMaxParallelism(1);
    }

    static void validateLeaseTiming(Duration persistedExpiration, Duration leaseGracePeriod) {
        Preconditions.checkArgument(
                persistedExpiration != null
                        && persistedExpiration.compareTo(leaseGracePeriod) >= 0
                        && persistedExpiration
                                        .minus(leaseGracePeriod)
                                        .compareTo(MIN_FAILOVER_RECOVERY_MARGIN)
                                >= 0,
                "Persisted consumer.expiration-time must be at least lease-grace-period + %s so a stopped attempt protects its served snapshot through failover.",
                MIN_FAILOVER_RECOVERY_MARGIN);
    }

    private static FileStoreTable descriptorReadTable(
            FileStoreTable table, GlobalIndexQueryServiceUtils.QuerySpec spec) {
        for (int position : spec.valuePositions()) {
            if (BlobType.isBlobFileField(table.rowType().getTypeAt(position))) {
                return (FileStoreTable)
                        table.copy(
                                Collections.singletonMap(
                                        CoreOptions.BLOB_AS_DESCRIPTOR.key(), "true"));
            }
        }
        return table;
    }

    private static class GlobalIndexEventChannelComputer implements ChannelComputer<InternalRow> {

        private int numChannels;

        @Override
        public void setup(int numChannels) {
            this.numChannels = numChannels;
        }

        @Override
        public int channel(InternalRow row) {
            int target = row.getInt(GlobalIndexQueryBootstrapOperator.TARGET);
            Preconditions.checkArgument(
                    target >= 0 && target < numChannels,
                    "Invalid global-index query target %s for %s channels.",
                    target,
                    numChannels);
            return target;
        }
    }
}
