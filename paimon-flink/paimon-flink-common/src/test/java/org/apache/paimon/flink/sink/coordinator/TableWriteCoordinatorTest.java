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
import org.apache.paimon.flink.FlinkConnectorOptions;
import org.apache.paimon.fs.Path;
import org.apache.paimon.index.GlobalIndexMeta;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.index.pk.PrimaryKeyIndexSourceFile;
import org.apache.paimon.index.pk.PrimaryKeyIndexSourceMeta;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.TableTestBase;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.table.sink.StreamTableWrite;
import org.apache.paimon.table.sink.StreamWriteBuilder;
import org.apache.paimon.table.sink.TableCommitImpl;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.utils.SegmentsCache;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.lang.reflect.Field;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.apache.paimon.data.BinaryRow.EMPTY_ROW;
import static org.apache.paimon.flink.FlinkConnectorOptions.SINK_WRITER_COORDINATOR_CACHE_EXPIRE_AFTER_ACCESS;
import static org.apache.paimon.flink.FlinkConnectorOptions.SINK_WRITER_COORDINATOR_CACHE_MEMORY;
import static org.apache.paimon.flink.FlinkConnectorOptions.SINK_WRITER_COORDINATOR_CACHE_SOFT_VALUES;
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
                new ScanCoordinationRequest(serializeBinaryRow(EMPTY_ROW), 0, false, false, false);
        ScanCoordinationResponse scan = coordinator.scan(request);
        assertThat(scan.snapshot().id()).isEqualTo(latest.id());
        assertThat(scan.extractDataFiles().size()).isEqualTo(initSnapshot ? 2 : 1);
    }

    @Test
    public void testScanVectorIndexPayloads() throws Exception {
        Identifier identifier = new Identifier("db", "table");
        Schema schema = Schema.newBuilder().column("f0", DataTypes.INT()).build();
        catalog.createDatabase("db", false);
        catalog.createTable(identifier, schema, false);
        FileStoreTable table = getTable(identifier);
        write(table, GenericRow.of(1));

        PrimaryKeyIndexSourceMeta sourceMeta =
                new PrimaryKeyIndexSourceMeta(
                        Collections.singletonList(new PrimaryKeyIndexSourceFile("data", 1)));
        IndexFileMeta ann =
                new IndexFileMeta(
                        "test-vector-ann",
                        "ann",
                        1,
                        1,
                        new GlobalIndexMeta(0, 0, 0, null, new byte[0], sourceMeta.serialize()),
                        null);
        try (TableCommitImpl commit = table.newCommit("vector-index")) {
            commit.commit(
                    100,
                    Collections.singletonList(
                            new CommitMessageImpl(
                                    EMPTY_ROW,
                                    0,
                                    1,
                                    DataIncrement.indexIncrement(Collections.singletonList(ann)),
                                    CompactIncrement.emptyIncrement())));
        }

        TableWriteCoordinator coordinator = new TableWriteCoordinator(table);
        ScanCoordinationRequest request =
                new ScanCoordinationRequest(serializeBinaryRow(EMPTY_ROW), 0, false, false, true);
        ScanCoordinationResponse scan = coordinator.scan(request);

        assertThat(scan.extractVectorIndexPayloads()).containsExactly(ann);
    }

    @Test
    public void testPrefetchManifestsWarmsCache() throws Exception {
        Identifier identifier = new Identifier("db", "table");
        Schema schema =
                Schema.newBuilder()
                        .column("f0", DataTypes.INT())
                        .option(
                                FlinkConnectorOptions.SINK_WRITER_COORDINATOR_PREFETCH_MANIFESTS
                                        .key(),
                                "true")
                        .build();
        catalog.createDatabase("db", false);
        catalog.createTable(identifier, schema, false);
        FileStoreTable table = getTable(identifier);

        write(table, GenericRow.of(1));
        write(table, GenericRow.of(2));

        // reset the manifest cache to a fresh, cold instance (the writes above may have populated
        // it) so we can assert that constructing the coordinator is what warms it
        // the existing cache on the table comes from CachingCatalog, which is distinct from
        // TableWriteCoordinator
        SegmentsCache<Path> cache = table.getManifestCache();
        table.setManifestCache(
                SegmentsCache.create(
                        cache.pageSize(),
                        cache.maxMemorySize(),
                        cache.maxElementSize(),
                        cache.ttl(),
                        cache.softValues()));
        assertThat(table.getManifestCache().totalCacheBytes()).isZero();

        // constructing the coordinator runs refresh() which warms the manifest cache when the
        // prefetch option is enabled
        TableWriteCoordinator coordinator = new TableWriteCoordinator(table);
        assertThat(table.getManifestCache().totalCacheBytes()).isGreaterThan(0);

        // scan results remain correct after warming
        ScanCoordinationRequest request =
                new ScanCoordinationRequest(serializeBinaryRow(EMPTY_ROW), 0, false, false, false);
        ScanCoordinationResponse scan = coordinator.scan(request);
        assertThat(scan.snapshot().id()).isEqualTo(table.latestSnapshot().get().id());
        assertThat(scan.extractDataFiles().size()).isEqualTo(2);
    }

    @Test
    public void testPrefetchWarmsAllManifestsAfterScan() throws Exception {
        Identifier identifier = new Identifier("db", "table");
        // a partitioned, fixed-bucket table so the data spans multiple partitions (and multiple
        // buckets within each partition)
        Schema schema =
                Schema.newBuilder()
                        .column("pt", DataTypes.INT())
                        .column("f0", DataTypes.INT())
                        .partitionKeys("pt")
                        .option(CoreOptions.BUCKET.key(), "2")
                        .option(CoreOptions.BUCKET_KEY.key(), "f0")
                        .build();
        catalog.createDatabase("db", false);
        catalog.createTable(identifier, schema, false);
        FileStoreTable table = getTable(identifier);

        // write each partition in its own commit so manifests are confined to a single partition: a
        // scan for one partition must skip the other partition's manifest at the manifest-file
        // level. Within each commit both buckets are written, so scanning a partition has to load
        // all of that partition's buckets.
        writeWithBucketAssigner(
                table, row -> row.getInt(1) % 2, GenericRow.of(0, 1), GenericRow.of(0, 2));
        writeWithBucketAssigner(
                table, row -> row.getInt(1) % 2, GenericRow.of(1, 1), GenericRow.of(1, 2));

        // the scan returns the entries of both partitions, each spanning both buckets, confirming
        // the table spans more than one partition and that every partition spans more than one
        // bucket
        Snapshot latest = table.latestSnapshot().get();
        List<ManifestEntry> entries = table.store().newScan().withSnapshot(latest).plan().files();
        assertThat(entries.stream().map(e -> e.partition().getInt(0)).collect(Collectors.toSet()))
                .containsExactlyInAnyOrder(0, 1);
        assertThat(entries.stream().map(ManifestEntry::bucket).collect(Collectors.toSet()))
                .containsExactlyInAnyOrder(0, 1);

        // construct the coordinator with prefetch disabled (the default) on a cold cache, so the
        // cache its shared scan is bound to stays cold until the scan request runs
        SegmentsCache<Path> cache = table.getManifestCache();
        table.setManifestCache(
                SegmentsCache.create(
                        cache.pageSize(),
                        cache.maxMemorySize(),
                        cache.maxElementSize(),
                        cache.ttl(),
                        cache.softValues()));
        TableWriteCoordinator coordinator = new TableWriteCoordinator(table);
        assertThat(table.getManifestCache().totalCacheBytes()).isZero();

        // a scan request for partition 0 reads only the partition-0 manifest, skipping the
        // partition-1 manifest at the manifest-file level: the cache therefore holds only a single
        // partition's manifest, proving the partition filter is active (and leaving the stale
        // partition state on the shared scan)
        ScanCoordinationRequest request =
                new ScanCoordinationRequest(
                        serializeBinaryRow(partitionRow(0)), 0, false, false, false);
        coordinator.scan(request);
        long filteredCacheBytes = table.getManifestCache().totalCacheBytes();
        assertThat(filteredCacheBytes).isGreaterThan(0);

        // enable prefetch via reflection (to avoid widening the coordinator's interface) and run a
        // checkpoint refresh; the prefetch must warm the full set of manifests rather than
        // inheriting the stale partition state, so the cache grows beyond the single-partition
        // subset
        Field prefetchManifests = TableWriteCoordinator.class.getDeclaredField("prefetchManifests");
        prefetchManifests.setAccessible(true);
        prefetchManifests.setBoolean(coordinator, true);
        coordinator.checkpoint();
        assertThat(table.getManifestCache().totalCacheBytes()).isGreaterThan(filteredCacheBytes);
    }

    @Test
    public void testBuildManifestCacheOptions() {
        // by default soft values are on and there is no idle TTL; the cache is bounded by memory
        Options defaults = new Options();
        SegmentsCache<Path> cache = WriteOperatorCoordinator.buildManifestCache(defaults);
        assertThat(cache.softValues()).isTrue();
        assertThat(cache.ttl()).isNull();
        assertThat(cache.maxMemorySize())
                .isEqualTo(SINK_WRITER_COORDINATOR_CACHE_MEMORY.defaultValue());

        // an explicit expire-after-access TTL is honored
        Options withTtl = new Options();
        withTtl.set(SINK_WRITER_COORDINATOR_CACHE_EXPIRE_AFTER_ACCESS, Duration.ofMinutes(5));
        cache = WriteOperatorCoordinator.buildManifestCache(withTtl);
        assertThat(cache.ttl()).isEqualTo(Duration.ofMinutes(5));
        assertThat(cache.softValues()).isTrue();

        // disabling soft values switches to strong references; still no TTL by default
        Options strongRefs = new Options();
        strongRefs.set(SINK_WRITER_COORDINATOR_CACHE_SOFT_VALUES, false);
        cache = WriteOperatorCoordinator.buildManifestCache(strongRefs);
        assertThat(cache.softValues()).isFalse();
        assertThat(cache.ttl()).isNull();

        // a zero cache memory disables the cache entirely
        Options noCache = new Options();
        noCache.set(SINK_WRITER_COORDINATOR_CACHE_MEMORY, MemorySize.ofBytes(0));
        assertThat(WriteOperatorCoordinator.buildManifestCache(noCache)).isNull();
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
                        .option(
                                CoreOptions.BUCKET_PER_PARTITION_COUNT_ENABLED.key(),
                                Boolean.TRUE.toString())
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
                new ScanCoordinationRequest(serializeBinaryRow(partitionA), 0, false, false, false);
        ScanCoordinationResponse responseBucket0 = coordinator.scan(requestBucket0);
        assertThat(responseBucket0.totalBuckets())
                .as("bucket=0 (has files) should use per-partition bucket count 2")
                .isEqualTo(partitionBuckets);
        assertThat(responseBucket0.extractDataFiles()).isNotEmpty();

        // Scan bucket=1 (empty): totalBuckets must be 2, not the table default 32
        ScanCoordinationRequest requestBucket1 =
                new ScanCoordinationRequest(serializeBinaryRow(partitionA), 1, false, false, false);
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
