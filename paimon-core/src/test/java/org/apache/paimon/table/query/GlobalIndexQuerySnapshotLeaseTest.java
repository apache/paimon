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
import org.apache.paimon.consumer.Consumer;
import org.apache.paimon.consumer.ConsumerManager;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.options.ExpireConfig;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.TableTestBase;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.utils.SnapshotManager;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.LocalDateTime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/** Integration tests for query-service consumer leases and snapshot expiration. */
public class GlobalIndexQuerySnapshotLeaseTest extends TableTestBase {

    @Test
    public void testCloseRefreshesLeaseForCompleteFailoverExpirationWindow() {
        ConsumerManager consumerManager = mock(ConsumerManager.class);
        GlobalIndexQuerySnapshotLease lease =
                new GlobalIndexQuerySnapshotLease(
                        consumerManager, "global-index-query-test", Duration.ofMinutes(2));
        lease.pinBuilding(7L);
        clearInvocations(consumerManager);

        lease.close();

        verify(consumerManager).resetConsumer(eq(lease.consumerId()), any(Consumer.class));
    }

    @Test
    public void testConsumerIdPrefixRejectsPathTraversal() throws Exception {
        FileStoreTable table = createAppendTable();
        assertThatThrownBy(
                        () ->
                                new GlobalIndexQuerySnapshotLease(
                                        table.consumerManager(),
                                        "x/../../service/service-owner-poison",
                                        table.coreOptions().consumerExpireTime()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("single alphanumeric path segment");
    }

    @Test
    public void testBuildingAndActiveSnapshotsAreProtectedUntilRelease() throws Exception {
        FileStoreTable table = createAppendTable();
        for (int i = 1; i <= 4; i++) {
            write(
                    table,
                    GenericRow.of(
                            BinaryString.fromString("key-" + i),
                            BinaryString.fromString("value-" + i)));
        }

        SnapshotManager snapshots = table.snapshotManager();
        ExpireConfig expireAllButLatest =
                ExpireConfig.builder()
                        .snapshotRetainMin(1)
                        .snapshotRetainMax(1)
                        .snapshotMaxDeletes(Integer.MAX_VALUE)
                        .snapshotTimeRetain(Duration.ZERO)
                        .build();
        GlobalIndexQuerySnapshotLease lease =
                new GlobalIndexQuerySnapshotLease(
                        table.consumerManager(),
                        "global-index-query-test",
                        table.coreOptions().consumerExpireTime());
        try {
            lease.pinBuilding(1L);
            table.newExpireSnapshots().config(expireAllButLatest).expire();
            assertThat(snapshots.earliestSnapshotId()).isEqualTo(1L);
            assertThat(table.fileIO().exists(snapshots.snapshotPath(1L))).isTrue();

            // Simulate the globally published generation and completed handover grace period.
            lease.promote(4L);
            table.newExpireSnapshots().config(expireAllButLatest).expire();
            assertThat(snapshots.earliestSnapshotId()).isEqualTo(4L);
            assertThat(table.fileIO().exists(snapshots.snapshotPath(1L))).isFalse();

            write(
                    table,
                    GenericRow.of(
                            BinaryString.fromString("key-5"), BinaryString.fromString("value-5")));
            table.newExpireSnapshots().config(expireAllButLatest).expire();
            assertThat(snapshots.earliestSnapshotId()).isEqualTo(4L);
        } finally {
            lease.close();
        }

        // Closing may be part of Flink failover. The stopped attempt keeps its last pin until
        // consumer expiration, closing the gap before a replacement attempt creates a new lease.
        table.newExpireSnapshots().config(expireAllButLatest).expire();
        assertThat(snapshots.earliestSnapshotId()).isEqualTo(4L);
        assertThat(table.consumerManager().consumer(lease.consumerId())).isPresent();

        table.consumerManager().expire(LocalDateTime.now().plusDays(2));
        table.newExpireSnapshots().config(expireAllButLatest).expire();
        assertThat(snapshots.earliestSnapshotId()).isEqualTo(5L);
        assertThat(table.consumerManager().consumer(lease.consumerId())).isEmpty();
    }

    private FileStoreTable createAppendTable() throws Exception {
        Schema schema =
                Schema.newBuilder()
                        .column("url", DataTypes.STRING())
                        .column("descriptor", DataTypes.STRING())
                        .option(CoreOptions.BUCKET.key(), "-1")
                        .option(CoreOptions.ROW_TRACKING_ENABLED.key(), "true")
                        .option(CoreOptions.DATA_EVOLUTION_ENABLED.key(), "true")
                        .option(CoreOptions.CONSUMER_EXPIRATION_TIME.key(), "1 d")
                        .build();
        catalog.createTable(identifier(), schema, false);
        return (FileStoreTable) catalog.getTable(identifier());
    }
}
