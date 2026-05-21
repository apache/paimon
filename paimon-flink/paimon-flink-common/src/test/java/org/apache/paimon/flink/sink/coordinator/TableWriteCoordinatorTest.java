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

package org.apache.paimon.flink.sink.coordinator;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryRowWriter;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.TableTestBase;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.table.sink.StreamTableWrite;
import org.apache.paimon.table.sink.StreamWriteBuilder;
import org.apache.paimon.table.sink.TableCommitImpl;
import org.apache.paimon.types.DataTypes;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static org.apache.paimon.data.BinaryRow.EMPTY_ROW;
import static org.apache.paimon.utils.SerializationUtils.serializeBinaryRow;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TableWriteCoordinatorTest extends TableTestBase {

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testLatestIdentifierAndScan(boolean initSnapshot) throws Exception {
        Identifier identifier = new Identifier("db", "table");
        Schema schema = Schema.newBuilder().column("f0", DataTypes.INT()).build();
        catalog.createDatabase("db", false);
        catalog.createTable(identifier, schema, false);
        FileStoreTable table = getTable(identifier);

        // initial with snapshot 1
        if (initSnapshot) {
            write(table, GenericRow.of(1));
        }
        TableWriteCoordinator coordinator = new TableWriteCoordinator(table);

        // latest snapshot get snapshot 2
        write(table, GenericRow.of(1));
        Snapshot latest = table.latestSnapshot().get();
        String commitUser = latest.commitUser();
        coordinator.latestCommittedIdentifier(commitUser);

        // scan should scan snapshot 2
        ScanCoordinationRequest request =
                new ScanCoordinationRequest(serializeBinaryRow(EMPTY_ROW), 0, false, false);
        ScanCoordinationResponse scan = coordinator.scan(request);
        assertThat(scan.snapshot().id()).isEqualTo(latest.id());
        assertThat(scan.extractDataFiles().size()).isEqualTo(initSnapshot ? 2 : 1);
    }

    @Test
    public void testNoManifestCache() throws Exception {
        Identifier identifier = new Identifier("db", "table");
        catalog.createDatabase("db", false);
        createTable(identifier);
        FileStoreTable table = getTable(identifier);
        table.setManifestCache(null);
        assertThatThrownBy(() -> new TableWriteCoordinator(table))
                .isInstanceOf(NullPointerException.class);
    }

    /**
     * Tests that when a partition has been rescaled (different bucket count than the table
     * default), the coordinator returns the correct per-partition bucket count even for buckets
     * with no data files.
     *
     * <p>Scenario:
     *
     * <ul>
     *   <li>Table bucket (default): 32
     *   <li>Partition A bucket: 2 (rescaled)
     *   <li>Partition A bucket=0: has data files, totalBuckets=2
     *   <li>Partition A bucket=1: no data files
     * </ul>
     *
     * <p>When scanning bucket=1 of partition A (empty bucket), the response {@code totalBuckets}
     * must be 2 (from the per-partition mapping), not 32 (the table default).
     */
    @Test
    public void testEmptyBucketUsesPartitionBucketCount() throws Exception {
        // Table default: 32 buckets; partition A was rescaled to 2
        int tableBuckets = 32;
        int partitionBuckets = 2;

        Identifier identifier = new Identifier("db2", "table");
        catalog.createDatabase("db2", false);
        Schema schema =
                Schema.newBuilder()
                        .column("pt", DataTypes.INT())
                        .column("k", DataTypes.INT())
                        .column("v", DataTypes.INT())
                        .primaryKey("pt", "k")
                        .partitionKeys("pt")
                        .option(CoreOptions.BUCKET.key(), String.valueOf(tableBuckets))
                        .build();
        catalog.createTable(identifier, schema, false);
        FileStoreTable table = getTable(identifier);

        // Write to partition A, bucket=0 with totalBuckets=2 (rescaled partition)
        String commitUser = UUID.randomUUID().toString();
        StreamWriteBuilder writeBuilder = table.newStreamWriteBuilder().withCommitUser(commitUser);
        List<CommitMessage> messages;
        try (StreamTableWrite write = writeBuilder.newWrite()) {
            write.write(GenericRow.of(1, 1, 100), 0);
            messages = write.prepareCommit(false, 0);
        }

        // Override totalBuckets to simulate partition rescale to 2
        CommitMessageImpl original = (CommitMessageImpl) messages.get(0);
        CommitMessageImpl rescaled =
                new CommitMessageImpl(
                        original.partition(),
                        original.bucket(),
                        partitionBuckets,
                        original.newFilesIncrement(),
                        original.compactIncrement());
        try (TableCommitImpl commit = table.newCommit(commitUser)) {
            commit.commit(0, Collections.<CommitMessage>singletonList(rescaled));
        }

        // Reload the table and create coordinator
        FileStoreTable freshTable = getTable(identifier);
        TableWriteCoordinator coordinator = new TableWriteCoordinator(freshTable);

        BinaryRow partitionA = partitionRow(1);

        // Scan bucket=0 (has files): totalBuckets should be 2
        ScanCoordinationRequest requestBucket0 =
                new ScanCoordinationRequest(serializeBinaryRow(partitionA), 0, false, false);
        ScanCoordinationResponse responseBucket0 = coordinator.scan(requestBucket0);
        assertThat(responseBucket0.totalBuckets())
                .as("bucket=0 (has files) should use per-partition bucket count 2")
                .isEqualTo(partitionBuckets);
        assertThat(responseBucket0.extractDataFiles()).isNotEmpty();

        // Scan bucket=1 (empty): totalBuckets must be 2, not the table default 32
        ScanCoordinationRequest requestBucket1 =
                new ScanCoordinationRequest(serializeBinaryRow(partitionA), 1, false, false);
        ScanCoordinationResponse responseBucket1 = coordinator.scan(requestBucket1);
        assertThat(responseBucket1.totalBuckets())
                .as(
                        "bucket=1 (empty bucket) must use per-partition bucket count 2, not table default 32")
                .isEqualTo(partitionBuckets);
        assertThat(responseBucket1.extractDataFiles()).isEmpty();
    }

    private BinaryRow partitionRow(int partitionValue) {
        BinaryRow row = new BinaryRow(1);
        BinaryRowWriter writer = new BinaryRowWriter(row);
        writer.writeInt(0, partitionValue);
        writer.complete();
        return row;
    }
}
