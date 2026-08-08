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

import org.apache.paimon.Snapshot;
import org.apache.paimon.service.GlobalIndexQueryServiceDescriptor;
import org.apache.paimon.service.GlobalIndexQueryServiceDescriptor.Endpoint;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.query.GlobalIndexQueryServiceUtils.QuerySpec;
import org.apache.paimon.table.source.ScanMode;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.snapshot.SnapshotReader;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Arrays;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Tests coordinator-side, data-evolution-safe split assignment. */
class GlobalIndexQuerySnapshotMonitorTest {

    @Test
    void testLeaseExpirationCoversGraceAndFailoverMargin() {
        assertThatCode(
                        () ->
                                QueryService.validateLeaseTiming(
                                        Duration.ofMinutes(11), Duration.ofMinutes(10)))
                .doesNotThrowAnyException();
        assertThatThrownBy(
                        () ->
                                QueryService.validateLeaseTiming(
                                        Duration.ofMinutes(10), Duration.ofMinutes(10)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("lease-grace-period + PT1M");
    }

    @Test
    void testEverySnapshotSplitIsAssignedExactlyOnce() {
        List<Split> splits = Arrays.asList(split(7L), split(10L), split(8L), split(9L));

        List<List<Split>> assignments = GlobalIndexQuerySnapshotMonitor.assignSplits(splits, 2);

        assertThat(assignments).hasSize(2);
        Map<Split, Integer> occurrences = new IdentityHashMap<>();
        long[] loads = new long[2];
        for (int target = 0; target < assignments.size(); target++) {
            for (Split split : assignments.get(target)) {
                occurrences.merge(split, 1, Integer::sum);
                loads[target] += split.mergedRowCount().getAsLong();
            }
        }
        assertThat(occurrences).hasSize(splits.size());
        assertThat(occurrences.values()).containsOnly(1);
        assertThat(loads).containsExactly(17L, 17L);
        assertThat(splits)
                .extracting(split -> split.mergedRowCount().getAsLong())
                .containsExactly(7L, 10L, 8L, 9L);
    }

    @Test
    void testPlanUsesExactSnapshotReaderAndRejectsMismatchedPlan() {
        FileStoreTable table = mock(FileStoreTable.class);
        QuerySpec spec = mock(QuerySpec.class);
        SnapshotReader reader = mock(SnapshotReader.class);
        SnapshotReader.Plan plan = mock(SnapshotReader.Plan.class);
        Snapshot snapshot = mock(Snapshot.class);
        List<Split> splits = Arrays.asList(split(3L), split(5L));
        RowType rowType = RowType.of(DataTypes.INT(), DataTypes.STRING());
        RowType projectedType = rowType.project(new int[] {1});

        when(table.rowType()).thenReturn(rowType);
        when(spec.bootstrapProjection()).thenReturn(new int[] {1});
        when(table.newSnapshotReader()).thenReturn(reader);
        when(reader.withMode(ScanMode.ALL)).thenReturn(reader);
        when(snapshot.id()).thenReturn(7L);
        when(snapshot.uuid()).thenReturn("snapshot-7");
        when(reader.withSnapshot(snapshot)).thenReturn(reader);
        when(reader.withReadType(projectedType)).thenReturn(reader);
        when(reader.read()).thenReturn(plan);
        when(plan.snapshotId()).thenReturn(7L);
        when(plan.splits()).thenReturn(splits);

        assertThat(GlobalIndexQuerySnapshotMonitor.planSnapshot(table, spec, snapshot))
                .containsExactlyElementsOf(splits);
        verify(reader).withMode(ScanMode.ALL);
        verify(reader).withSnapshot(snapshot);
        verify(reader).withReadType(projectedType);

        when(plan.snapshotId()).thenReturn(8L);
        assertThatThrownBy(
                        () -> GlobalIndexQuerySnapshotMonitor.planSnapshot(table, spec, snapshot))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Expected bootstrap snapshot 7")
                .hasMessageContaining("planned snapshot 8");
    }

    @Test
    void testConsecutiveReadyHandoversKeepIndependentGraceDeadlines() {
        long minute = Duration.ofMinutes(1).toNanos();
        GlobalIndexQuerySnapshotMonitor.LeaseHandoverTracker tracker =
                new GlobalIndexQuerySnapshotMonitor.LeaseHandoverTracker(Duration.ofMinutes(10));

        assertAcknowledged(
                descriptor(true, 10L, 10L, "snapshot-10"), true, 10L, 10L, "snapshot-10");
        tracker.acknowledge(10L, 10L, 0L);
        assertAcknowledged(
                descriptor(true, 11L, 11L, "snapshot-11"), true, 11L, 11L, "snapshot-11");
        tracker.acknowledge(11L, 11L, 5L * minute);
        assertThat(tracker.promotableSnapshot(9L * minute).isPresent()).isFalse();
        assertThat(tracker.promotableSnapshot(10L * minute).getAsLong()).isEqualTo(10L);

        tracker.acknowledge(12L, 12L, 12L * minute);
        assertThat(tracker.promotableSnapshot(14L * minute).isPresent()).isFalse();
        assertThat(tracker.promotableSnapshot(15L * minute).getAsLong()).isEqualTo(11L);
        assertThat(tracker.promotableSnapshot(22L * minute).getAsLong()).isEqualTo(12L);
    }

    @Test
    void testConsecutiveUnavailableHandoversAndSkippedGeneration() {
        long minute = Duration.ofMinutes(1).toNanos();
        GlobalIndexQuerySnapshotMonitor.LeaseHandoverTracker tracker =
                new GlobalIndexQuerySnapshotMonitor.LeaseHandoverTracker(Duration.ofMinutes(10));

        assertAcknowledged(descriptor(false, 20L, 20L, null), false, 20L, 20L, null);
        tracker.acknowledge(20L, 20L, 0L);
        // Generation 21 was never observed by the monitor. A later exact acknowledgement is a
        // conservative handover point for all skipped generations.
        assertAcknowledged(descriptor(false, 22L, 22L, null), false, 22L, 22L, null);
        tracker.acknowledge(22L, 22L, 5L * minute);

        assertThat(tracker.promotableSnapshot(10L * minute).getAsLong()).isEqualTo(20L);
        assertThat(tracker.promotableSnapshot(14L * minute).isPresent()).isFalse();
        assertThat(tracker.promotableSnapshot(15L * minute).getAsLong()).isEqualTo(22L);
    }

    @Test
    void testDescriptorAcknowledgementRequiresExactIdentityStateAndFence() {
        GlobalIndexQueryServiceDescriptor ready = descriptor(true, 30L, 30L, "snapshot-30");

        assertAcknowledged(ready, true, 30L, 30L, "snapshot-30");
        assertThat(
                        acknowledges(
                                ready,
                                false,
                                "table-uuid",
                                "main",
                                7L,
                                "fingerprint",
                                3,
                                new int[] {4, 5},
                                30L,
                                30L,
                                null))
                .isFalse();
        assertThat(
                        acknowledges(
                                ready,
                                true,
                                "other-table",
                                "main",
                                7L,
                                "fingerprint",
                                3,
                                new int[] {4, 5},
                                30L,
                                30L,
                                "snapshot-30"))
                .isFalse();
        assertThat(
                        acknowledges(
                                ready,
                                true,
                                "table-uuid",
                                "main",
                                7L,
                                "fingerprint",
                                3,
                                new int[] {4, 6},
                                30L,
                                30L,
                                "snapshot-30"))
                .isFalse();
        assertThat(
                        acknowledges(
                                ready,
                                true,
                                "table-uuid",
                                "main",
                                7L,
                                "fingerprint",
                                3,
                                new int[] {4, 5},
                                31L,
                                30L,
                                "snapshot-30"))
                .isFalse();
        assertThat(
                        acknowledges(
                                ready,
                                true,
                                "table-uuid",
                                "main",
                                7L,
                                "fingerprint",
                                3,
                                new int[] {4, 5},
                                30L,
                                30L,
                                "other-snapshot"))
                .isFalse();

        GlobalIndexQueryServiceDescriptor wrongProtocol =
                descriptor(
                        GlobalIndexQueryServiceDescriptor.PROTOCOL_VERSION + 1,
                        true,
                        30L,
                        30L,
                        "snapshot-30");
        assertThat(
                        acknowledges(
                                wrongProtocol,
                                true,
                                "table-uuid",
                                "main",
                                7L,
                                "fingerprint",
                                3,
                                new int[] {4, 5},
                                30L,
                                30L,
                                "snapshot-30"))
                .isFalse();
    }

    @Test
    void testBootstrapInvalidTargetStillStartsLeaseGrace() {
        GlobalIndexQueryServiceDescriptor bootstrapInvalid = descriptor(false, 40L, 40L, null);
        // The coverage gate considered snapshot 40 ready and therefore retained its UUID, but the
        // all-executor bootstrap result rejected it (for example, duplicate or oversized value).
        assertThat(acknowledgesTarget(bootstrapInvalid, 40L, 40L, "snapshot-40")).isTrue();

        GlobalIndexQuerySnapshotMonitor.LeaseHandoverTracker tracker =
                new GlobalIndexQuerySnapshotMonitor.LeaseHandoverTracker(Duration.ofMinutes(10));
        tracker.acknowledge(40L, 40L, 0L);
        assertThat(tracker.promotableSnapshot(Duration.ofMinutes(9).toNanos()).isPresent())
                .isFalse();
        assertThat(tracker.promotableSnapshot(Duration.ofMinutes(10).toNanos()).getAsLong())
                .isEqualTo(40L);
    }

    private void assertAcknowledged(
            GlobalIndexQueryServiceDescriptor descriptor,
            boolean ready,
            long generation,
            long snapshotId,
            String snapshotUuid) {
        assertThat(
                        acknowledges(
                                descriptor,
                                ready,
                                "table-uuid",
                                "main",
                                7L,
                                "fingerprint",
                                3,
                                new int[] {4, 5},
                                generation,
                                snapshotId,
                                snapshotUuid))
                .isTrue();
    }

    private boolean acknowledges(
            GlobalIndexQueryServiceDescriptor descriptor,
            boolean ready,
            String tableUuid,
            String branch,
            long schemaId,
            String schemaFingerprint,
            int lookupFieldId,
            int[] valueFieldIds,
            long generation,
            long snapshotId,
            String snapshotUuid) {
        return GlobalIndexQuerySnapshotMonitor.acknowledgesDescriptor(
                Optional.of(descriptor),
                ready,
                schemaId,
                tableUuid,
                branch,
                schemaFingerprint,
                lookupFieldId,
                valueFieldIds,
                generation,
                snapshotId,
                snapshotUuid,
                2);
    }

    private boolean acknowledgesTarget(
            GlobalIndexQueryServiceDescriptor descriptor,
            long generation,
            long snapshotId,
            String snapshotUuid) {
        return GlobalIndexQuerySnapshotMonitor.acknowledgesTargetDescriptor(
                Optional.of(descriptor),
                7L,
                "table-uuid",
                "main",
                "fingerprint",
                3,
                new int[] {4, 5},
                generation,
                snapshotId,
                snapshotUuid,
                2);
    }

    private GlobalIndexQueryServiceDescriptor descriptor(
            boolean ready, long generation, long snapshotId, String snapshotUuid) {
        return descriptor(
                GlobalIndexQueryServiceDescriptor.PROTOCOL_VERSION,
                ready,
                generation,
                snapshotId,
                snapshotUuid);
    }

    private GlobalIndexQueryServiceDescriptor descriptor(
            int protocolVersion,
            boolean ready,
            long generation,
            long snapshotId,
            String snapshotUuid) {
        Endpoint[] endpoints =
                ready
                        ? new Endpoint[] {
                            new Endpoint(0, new InetSocketAddress("127.0.0.1", 10000), "epoch-0"),
                            new Endpoint(1, new InetSocketAddress("127.0.0.1", 10001), "epoch-1")
                        }
                        : new Endpoint[0];
        return new GlobalIndexQueryServiceDescriptor(
                protocolVersion,
                "table-uuid",
                "main",
                7L,
                "fingerprint",
                3,
                new int[] {4, 5},
                generation,
                snapshotId,
                snapshotUuid,
                GlobalIndexQueryServiceDescriptor.KEY_HASH_VERSION,
                GlobalIndexQueryServiceDescriptor.LAYOUT,
                "owner",
                ready,
                ready ? "" : "not ready",
                endpoints);
    }

    private Split split(long rows) {
        Split split = mock(Split.class);
        when(split.rowCount()).thenReturn(rows);
        when(split.mergedRowCount()).thenReturn(OptionalLong.of(rows));
        return split;
    }
}
