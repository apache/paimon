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
import org.apache.paimon.Snapshot;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.flink.source.AbstractNonCoordinatedSource;
import org.apache.paimon.flink.source.AbstractNonCoordinatedSourceReader;
import org.apache.paimon.flink.source.SimpleSourceSplit;
import org.apache.paimon.flink.utils.InternalTypeInfo;
import org.apache.paimon.options.Options;
import org.apache.paimon.service.GlobalIndexQueryServiceDescriptor;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.query.GlobalIndexQueryServiceUtils;
import org.apache.paimon.table.query.GlobalIndexQueryServiceUtils.QuerySpec;
import org.apache.paimon.table.query.GlobalIndexQueryServiceUtils.SnapshotReadiness;
import org.apache.paimon.table.query.GlobalIndexQuerySnapshotLease;
import org.apache.paimon.table.source.ScanMode;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.SplitSerializer;
import org.apache.paimon.table.source.snapshot.SnapshotReader;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Preconditions;
import org.apache.paimon.utils.SnapshotManager;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Deque;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;

import static org.apache.paimon.table.query.GlobalIndexQueryServiceUtils.persistedCoreOptions;

/** Monitors exact snapshots and emits a fail-closed global-index coverage decision once. */
public class GlobalIndexQuerySnapshotMonitor extends AbstractNonCoordinatedSource<InternalRow> {

    private static final long serialVersionUID = 1L;

    public static final int START = 0;
    public static final int SPLIT = 1;
    public static final int COMPLETE = 2;
    public static final int NOT_READY = 3;
    public static final int TARGET = 3;

    private final FileStoreTable table;
    private final String lookupField;
    private final List<String> valueFields;
    private final int numBootstraps;
    private final String leaseIdPrefix;
    private final Duration leaseGracePeriod;
    private final long monitorInterval;
    private final String expectedTableUuid;
    private final String expectedBranch;

    public GlobalIndexQuerySnapshotMonitor(
            FileStoreTable table,
            String lookupField,
            List<String> valueFields,
            int numBootstraps,
            String leaseIdPrefix,
            Duration leaseGracePeriod) {
        this.table = table;
        this.lookupField = lookupField;
        this.valueFields = valueFields;
        this.numBootstraps = numBootstraps;
        this.leaseIdPrefix = leaseIdPrefix;
        this.leaseGracePeriod = leaseGracePeriod;
        this.expectedTableUuid = table.uuid();
        this.expectedBranch = table.coreOptions().branch();
        this.monitorInterval =
                Options.fromMap(table.options())
                        .get(CoreOptions.CONTINUOUS_DISCOVERY_INTERVAL)
                        .toMillis();
    }

    public static RowType outputType() {
        return RowType.of(
                DataTypes.BIGINT(),
                DataTypes.BIGINT(),
                DataTypes.INT(),
                DataTypes.INT(),
                DataTypes.BYTES(),
                DataTypes.STRING());
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.CONTINUOUS_UNBOUNDED;
    }

    @Override
    public SourceReader<InternalRow, SimpleSourceSplit> createReader(
            SourceReaderContext sourceReaderContext) {
        return new Reader();
    }

    private class Reader extends AbstractNonCoordinatedSourceReader<InternalRow> {

        private transient SnapshotManager snapshotManager;
        private transient QuerySpec spec;
        private transient GlobalIndexQuerySnapshotLease snapshotLease;
        @Nullable private Long lastSnapshotId;
        @Nullable private String lastSnapshotUuid;
        @Nullable private String lastTableUuid;
        @Nullable private String lastBranch;
        @Nullable private Long lastGeneration;
        private transient LeaseHandoverTracker leaseHandovers;
        private boolean leaseInitialized;

        @Override
        public void start() {
            this.snapshotManager = table.store().snapshotManager();
            this.spec = GlobalIndexQueryServiceUtils.querySpec(table, lookupField, valueFields);
            this.snapshotLease =
                    new GlobalIndexQuerySnapshotLease(
                            table.consumerManager(),
                            leaseIdPrefix,
                            persistedCoreOptions(table).consumerExpireTime());
            this.leaseHandovers = new LeaseHandoverTracker(leaseGracePeriod);
        }

        @Override
        public InputStatus pollNext(ReaderOutput<InternalRow> output) throws Exception {
            snapshotLease.checkHealthy();
            Long latestSnapshotId = snapshotManager.latestSnapshotIdFromFileSystem();
            long snapshotId =
                    latestSnapshotId == null
                            ? GlobalIndexQueryServiceUtils.EMPTY_SNAPSHOT_ID
                            : latestSnapshotId;
            if (latestSnapshotId != null) {
                // Acquire the consumer lease before opening or planning the exact snapshot. The
                // lease stays at min(active, building) until global publication plus grace. On a
                // replacement attempt, first inherit the oldest existing consumer pin so the
                // previous attempt's served Blob descriptors remain protected during handover.
                long snapshotToPin = latestSnapshotId;
                if (!leaseInitialized) {
                    OptionalLong inherited = table.consumerManager().minNextSnapshot();
                    if (inherited.isPresent()) {
                        snapshotToPin = Math.min(snapshotToPin, inherited.getAsLong());
                    }
                }
                snapshotLease.pinBuilding(snapshotToPin);
            }
            leaseInitialized = true;
            Snapshot snapshot =
                    latestSnapshotId == null ? null : snapshotManager.snapshot(latestSnapshotId);
            String snapshotUuid = snapshot == null ? null : snapshot.uuid();
            String currentTableUuid = table.uuid();
            String currentBranch = table.coreOptions().branch();
            if (lastSnapshotId != null
                    && lastSnapshotId == snapshotId
                    && Objects.equals(lastSnapshotUuid, snapshotUuid)
                    && Objects.equals(lastTableUuid, currentTableUuid)
                    && Objects.equals(lastBranch, currentBranch)) {
                maybeAdvanceLease();
                Thread.sleep(monitorInterval);
                return InputStatus.MORE_AVAILABLE;
            }

            SnapshotReadiness readiness;
            if (!expectedTableUuid.equals(currentTableUuid)
                    || !expectedBranch.equals(currentBranch)) {
                readiness =
                        SnapshotReadiness.notReady(
                                snapshotId,
                                String.format(
                                        "Table identity changed from UUID %s branch %s to UUID %s branch %s; restart the query service.",
                                        expectedTableUuid,
                                        expectedBranch,
                                        currentTableUuid,
                                        currentBranch));
            } else {
                readiness = GlobalIndexQueryServiceUtils.snapshotReadiness(table, spec, snapshot);
            }
            // Snapshot IDs normally provide the generation. If a table path is recreated or a
            // snapshot ID is reused with another UUID, keep the request fence strictly increasing.
            long generation =
                    lastGeneration == null
                            ? readiness.snapshotId()
                            : Math.max(readiness.snapshotId(), lastGeneration + 1L);
            if (!readiness.ready()) {
                // Start the grace clock only after discovery acknowledges this exact unavailable
                // generation. Starting it here would let downstream backpressure consume the grace
                // period before executors have fenced old requests and the register has withdrawn
                // the ready descriptor.
                for (int target = 0; target < numBootstraps; target++) {
                    emit(
                            output,
                            generation,
                            readiness.snapshotId(),
                            NOT_READY,
                            target,
                            null,
                            readiness.reason());
                }
            } else {
                List<Split> splits = planSnapshot(snapshot);
                List<List<Split>> assignments = assignSplits(splits, numBootstraps);
                for (int target = 0; target < numBootstraps; target++) {
                    emit(output, generation, readiness.snapshotId(), START, target, null, "");
                }
                for (int target = 0; target < numBootstraps; target++) {
                    for (Split split : assignments.get(target)) {
                        emit(
                                output,
                                generation,
                                readiness.snapshotId(),
                                SPLIT,
                                target,
                                SplitSerializer.serialize(split),
                                "");
                    }
                }
                for (int target = 0; target < numBootstraps; target++) {
                    emit(output, generation, readiness.snapshotId(), COMPLETE, target, null, "");
                }
            }
            lastSnapshotId = snapshotId;
            lastSnapshotUuid = snapshotUuid;
            lastTableUuid = currentTableUuid;
            lastBranch = currentBranch;
            lastGeneration = generation;
            maybeAdvanceLease();
            return InputStatus.MORE_AVAILABLE;
        }

        private void maybeAdvanceLease() {
            long now = System.nanoTime();
            Optional<GlobalIndexQueryServiceDescriptor> descriptor =
                    table.store().newServiceManager().globalIndexService(spec.serviceId());
            if (lastSnapshotId != null
                    && lastSnapshotId != GlobalIndexQueryServiceUtils.EMPTY_SNAPSHOT_ID
                    && lastGeneration != null
                    && acknowledgesTargetDescriptor(
                            descriptor,
                            table.schema().id(),
                            expectedTableUuid,
                            expectedBranch,
                            spec.schemaFingerprint(),
                            spec.lookupFieldId(),
                            spec.valueFieldIds(),
                            lastGeneration,
                            lastSnapshotId,
                            lastSnapshotUuid,
                            numBootstraps)) {
                leaseHandovers.acknowledge(lastGeneration, lastSnapshotId, now);
            }
            leaseHandovers.promotableSnapshot(now).ifPresent(snapshotLease::promote);
        }

        @Override
        public void close() {
            if (snapshotLease != null) {
                snapshotLease.close();
            }
        }

        private List<Split> planSnapshot(@Nullable Snapshot snapshot) {
            return GlobalIndexQuerySnapshotMonitor.planSnapshot(table, spec, snapshot);
        }
    }

    static List<Split> planSnapshot(
            FileStoreTable table, QuerySpec spec, @Nullable Snapshot snapshot) {
        if (snapshot == null) {
            return new ArrayList<>();
        }
        RowType readType = table.rowType().project(spec.bootstrapProjection());
        // Do not use FileStoreTable.newScan here. Its StartingScanner is configured from dynamic
        // scan options and can replace the explicitly requested snapshot with the latest snapshot,
        // while scan.bucket can silently prune a bucket-unaware table to zero splits. This reader
        // is the source of truth for the generation fence and must plan exactly the leased target.
        SnapshotReader.Plan plan =
                table.newSnapshotReader()
                        .withMode(ScanMode.ALL)
                        .withSnapshot(snapshot)
                        .withReadType(readType)
                        .read();
        Preconditions.checkState(
                Objects.equals(plan.snapshotId(), snapshot.id()),
                "Expected bootstrap snapshot %s (%s) but planned snapshot %s.",
                snapshot.id(),
                snapshot.uuid(),
                plan.snapshotId());
        return plan.splits();
    }

    static boolean acknowledgesTargetDescriptor(
            Optional<GlobalIndexQueryServiceDescriptor> descriptor,
            long schemaId,
            String tableUuid,
            String branch,
            String schemaFingerprint,
            int lookupFieldId,
            int[] valueFieldIds,
            long generation,
            long snapshotId,
            @Nullable String snapshotUuid,
            int numExecutors) {
        // Coverage is only a pre-bootstrap gate. The bootstrap may still invalidate the target
        // because it discovers a duplicate key or an individually oversized value. Either a READY
        // publication or an all-executor exact NOT_READY publication proves that every executor's
        // accepted-generation fence has advanced, so both safely start the handover grace.
        return acknowledgesDescriptor(
                        descriptor,
                        true,
                        schemaId,
                        tableUuid,
                        branch,
                        schemaFingerprint,
                        lookupFieldId,
                        valueFieldIds,
                        generation,
                        snapshotId,
                        snapshotUuid,
                        numExecutors)
                || acknowledgesDescriptor(
                        descriptor,
                        false,
                        schemaId,
                        tableUuid,
                        branch,
                        schemaFingerprint,
                        lookupFieldId,
                        valueFieldIds,
                        generation,
                        snapshotId,
                        null,
                        numExecutors);
    }

    static boolean acknowledgesDescriptor(
            Optional<GlobalIndexQueryServiceDescriptor> optionalDescriptor,
            boolean ready,
            long schemaId,
            String tableUuid,
            String branch,
            String schemaFingerprint,
            int lookupFieldId,
            int[] valueFieldIds,
            long generation,
            long snapshotId,
            @Nullable String snapshotUuid,
            int numExecutors) {
        if (!optionalDescriptor.isPresent()) {
            return false;
        }
        GlobalIndexQueryServiceDescriptor descriptor = optionalDescriptor.get();
        return descriptor.protocolVersion() == GlobalIndexQueryServiceDescriptor.PROTOCOL_VERSION
                && descriptor.hashVersion() == GlobalIndexQueryServiceDescriptor.KEY_HASH_VERSION
                && GlobalIndexQueryServiceDescriptor.LAYOUT.equals(descriptor.layout())
                && descriptor.ready() == ready
                && descriptor.schemaId() == schemaId
                && tableUuid.equals(descriptor.tableUuid())
                && branch.equals(descriptor.branch())
                && schemaFingerprint.equals(descriptor.schemaFingerprint())
                && descriptor.lookupFieldId() == lookupFieldId
                && Arrays.equals(descriptor.valueFieldIds(), valueFieldIds)
                && descriptor.servedGeneration() == generation
                && descriptor.servedSnapshotId() == snapshotId
                && (ready
                        ? Objects.equals(descriptor.snapshotUuid(), snapshotUuid)
                                && descriptor.endpoints().length == numExecutors
                        : descriptor.snapshotUuid() == null && descriptor.endpoints().length == 0);
    }

    /**
     * Tracks each descriptor handover independently so a newer snapshot cannot restart an older
     * snapshot's grace period. Entries are bounded by the snapshots acknowledged during one grace
     * window and are removed in acknowledgement order.
     */
    static class LeaseHandoverTracker {

        private final long graceNanos;
        private final Deque<LeaseHandover> handovers = new ArrayDeque<>();
        private long lastAcknowledgedGeneration = Long.MIN_VALUE;

        LeaseHandoverTracker(Duration gracePeriod) {
            this.graceNanos = gracePeriod.toNanos();
        }

        void acknowledge(long generation, long snapshotId, long acknowledgedNanos) {
            if (generation <= lastAcknowledgedGeneration) {
                return;
            }
            handovers.addLast(new LeaseHandover(snapshotId, acknowledgedNanos));
            lastAcknowledgedGeneration = generation;
        }

        OptionalLong promotableSnapshot(long nowNanos) {
            OptionalLong result = OptionalLong.empty();
            while (!handovers.isEmpty()
                    && nowNanos - handovers.peekFirst().acknowledgedNanos >= graceNanos) {
                result = OptionalLong.of(handovers.removeFirst().snapshotId);
            }
            return result;
        }
    }

    private static class LeaseHandover {

        private final long snapshotId;
        private final long acknowledgedNanos;

        private LeaseHandover(long snapshotId, long acknowledgedNanos) {
            this.snapshotId = snapshotId;
            this.acknowledgedNanos = acknowledgedNanos;
        }
    }

    static List<List<Split>> assignSplits(List<Split> splits, int parallelism) {
        List<List<Split>> assignments = new ArrayList<>(parallelism);
        long[] loads = new long[parallelism];
        for (int i = 0; i < parallelism; i++) {
            assignments.add(new ArrayList<>());
        }
        List<Split> orderedSplits = new ArrayList<>(splits);
        orderedSplits.sort(
                Comparator.comparingLong(GlobalIndexQuerySnapshotMonitor::weight).reversed());
        for (Split split : orderedSplits) {
            int target = 0;
            for (int i = 1; i < loads.length; i++) {
                if (loads[i] < loads[target]) {
                    target = i;
                }
            }
            assignments.get(target).add(split);
            loads[target] += weight(split);
        }
        return assignments;
    }

    private static long weight(Split split) {
        return Math.max(1L, split.mergedRowCount().orElse(split.rowCount()));
    }

    private static void emit(
            ReaderOutput<InternalRow> output,
            long generation,
            long snapshotId,
            int type,
            int target,
            @Nullable byte[] split,
            String reason) {
        output.collect(
                GenericRow.of(
                        generation,
                        snapshotId,
                        type,
                        target,
                        split,
                        BinaryString.fromString(reason)));
    }

    public static DataStream<InternalRow> build(
            StreamExecutionEnvironment env,
            FileStoreTable table,
            String lookupField,
            List<String> valueFields,
            int numBootstraps,
            String leaseIdPrefix,
            Duration leaseGracePeriod) {
        return env.fromSource(
                        new GlobalIndexQuerySnapshotMonitor(
                                table,
                                lookupField,
                                valueFields,
                                numBootstraps,
                                leaseIdPrefix,
                                leaseGracePeriod),
                        WatermarkStrategy.noWatermarks(),
                        "GlobalIndexSnapshotMonitor-" + table.name(),
                        InternalTypeInfo.fromRowType(outputType()))
                .setParallelism(1);
    }
}
