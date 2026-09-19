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

package org.apache.paimon.operation.metrics;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.metrics.Gauge;
import org.apache.paimon.metrics.MetricGroup;
import org.apache.paimon.metrics.MetricGroupImpl;
import org.apache.paimon.metrics.MetricRegistry;
import org.apache.paimon.operation.FileStoreScan;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.TableTestBase;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.source.ScanMode;
import org.apache.paimon.table.source.StreamTableScan;
import org.apache.paimon.table.source.snapshot.SnapshotReader;
import org.apache.paimon.types.DataTypes;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests that {@link ScanMetrics#LAST_SCAN_RESULTED_TABLE_FILES_SIZE} and {@link
 * ScanMetrics#LAST_SCAN_RESULTED_RECORD_COUNT} describe the files the consumer of a plan actually
 * reads, across every read path of {@link SnapshotReader}.
 *
 * <p>A DELTA plan carries both ADD and DELETE entries. A normal read takes only the ADD entries; a
 * change read also reads the DELETE entries as its before files. The metrics must follow that
 * choice: neither count the DELETE entries for an ordinary read, nor drop them for a change read.
 */
public class ScanResultedFilesMetricsTest extends TableTestBase {

    // ------------------------------------------------------------------------------------------
    // SnapshotReader paths
    // ------------------------------------------------------------------------------------------

    @Test
    public void testBatchReadReportsAddEntries() throws Exception {
        FileStoreTable table = createAppendTable("batch_read");
        write(table, row(1, "a"), row(2, "b"));
        write(table, row(3, "c"));

        CapturingMetricRegistry registry = new CapturingMetricRegistry();
        table.newSnapshotReader().withMetricRegistry(registry).read();

        Expected expected = expectedOf(table, ScanMode.ALL, latestId(table), FileKind.ADD);
        assertThat(expected.files).isGreaterThan(0);
        assertResultedFiles(registry, expected.size, expected.records);
    }

    @Test
    public void testNormalReadOfDeltaPlanExcludesDeleteEntries() throws Exception {
        FileStoreTable table = createAppendTable("delta_normal_read");
        write(table, row(1, "a"), row(2, "b"));
        overwrite(table, row(9, "z"));
        long overwriteId = latestId(table);

        // premise: the overwrite's DELTA plan really holds entries of both kinds
        Expected deleted = expectedOf(table, ScanMode.DELTA, overwriteId, FileKind.DELETE);
        Expected added = expectedOf(table, ScanMode.DELTA, overwriteId, FileKind.ADD);
        assertThat(deleted.files).isGreaterThan(0);
        assertThat(added.files).isGreaterThan(0);

        CapturingMetricRegistry registry = new CapturingMetricRegistry();
        table.newSnapshotReader()
                .withMetricRegistry(registry)
                .withMode(ScanMode.DELTA)
                .withSnapshot(overwriteId)
                .read();

        // only the ADD entries are read, so the DELETE side must not be counted
        assertResultedFiles(registry, added.size, added.records);
        // the existing file count metric keeps counting every entry of the plan; the asymmetry
        // with size and records is deliberate, not a regression
        assertThat(gauge(registry, ScanMetrics.LAST_SCAN_RESULTED_TABLE_FILES))
                .isEqualTo(added.files + deleted.files);
    }

    @Test
    public void testChangeReadOfOverwriteIncludesBeforeFiles() throws Exception {
        FileStoreTable table = createAppendTable("delta_change_read");
        write(table, row(1, "a"), row(2, "b"));
        overwrite(table, row(9, "z"));
        long overwriteId = latestId(table);

        Expected deleted = expectedOf(table, ScanMode.DELTA, overwriteId, FileKind.DELETE);
        Expected added = expectedOf(table, ScanMode.DELTA, overwriteId, FileKind.ADD);
        assertThat(deleted.files).isGreaterThan(0);

        CapturingMetricRegistry registry = new CapturingMetricRegistry();
        table.newSnapshotReader()
                .withMetricRegistry(registry)
                .withSnapshot(overwriteId)
                .readChanges();

        // the DELETE entries are the before files of the change and are read as well
        assertResultedFiles(registry, added.size + deleted.size, added.records + deleted.records);
        // and the change read reports strictly more than the normal read of the same plan
        assertThat(gauge(registry, ScanMetrics.LAST_SCAN_RESULTED_TABLE_FILES_SIZE))
                .isGreaterThan(added.size);
    }

    @Test
    public void testChangeReadOfDeletionOnlyChange() throws Exception {
        FileStoreTable table = createAppendTable("deletion_only");
        write(table, row(1, "a"), row(2, "b"));
        // an overwrite that writes nothing drops every existing file and adds none
        overwrite(table);
        long deletionId = latestId(table);

        Expected deleted = expectedOf(table, ScanMode.DELTA, deletionId, FileKind.DELETE);
        Expected added = expectedOf(table, ScanMode.DELTA, deletionId, FileKind.ADD);
        assertThat(deleted.files).isGreaterThan(0);
        assertThat(added.files).isEqualTo(0);

        // a normal read of this plan reads nothing
        CapturingMetricRegistry normal = new CapturingMetricRegistry();
        table.newSnapshotReader()
                .withMetricRegistry(normal)
                .withMode(ScanMode.DELTA)
                .withSnapshot(deletionId)
                .read();
        assertResultedFiles(normal, 0, 0);

        // a change read of the same plan scans the old files to produce the deletes, and must not
        // report zero while doing so
        CapturingMetricRegistry change = new CapturingMetricRegistry();
        table.newSnapshotReader().withMetricRegistry(change).withSnapshot(deletionId).readChanges();
        assertResultedFiles(change, deleted.size, deleted.records);
        assertThat(deleted.size).isGreaterThan(0);
    }

    @Test
    public void testIncrementalDiffReportsBothSnapshots() throws Exception {
        FileStoreTable table = createAppendTable("incremental_diff");
        write(table, row(1, "a"));
        long beforeId = latestId(table);
        write(table, row(2, "b"), row(3, "c"));
        long afterId = latestId(table);

        Expected before = expectedOf(table, ScanMode.ALL, beforeId, FileKind.ADD);
        Expected after = expectedOf(table, ScanMode.ALL, afterId, FileKind.ADD);

        Snapshot beforeSnapshot = table.snapshotManager().snapshot(beforeId);
        CapturingMetricRegistry registry = new CapturingMetricRegistry();
        table.newSnapshotReader()
                .withMetricRegistry(registry)
                .withSnapshot(afterId)
                .readIncrementalDiff(beforeSnapshot);

        // the diff is computed from two independent scans, and the ADD entries of both are read
        assertResultedFiles(registry, before.size + after.size, before.records + after.records);
    }

    @Test
    public void testNoMetricRegistryDoesNotFail() throws Exception {
        FileStoreTable table = createAppendTable("no_registry");
        write(table, row(1, "a"));
        overwrite(table, row(2, "b"));
        long overwriteId = latestId(table);

        // every path must keep working when no registry was attached
        table.newSnapshotReader().read();
        table.newSnapshotReader().withMode(ScanMode.DELTA).withSnapshot(overwriteId).read();
        table.newSnapshotReader().withSnapshot(overwriteId).readChanges();
        table.newSnapshotReader()
                .withSnapshot(overwriteId)
                .readIncrementalDiff(table.snapshotManager().snapshot(overwriteId - 1));
    }

    // ------------------------------------------------------------------------------------------
    // streaming overwrite through FollowUpScanner
    // ------------------------------------------------------------------------------------------

    @Test
    public void testStreamingOverwriteOnAppendTableReadsAddEntriesOnly() throws Exception {
        FileStoreTable table =
                createAppendTable(
                        "stream_append_overwrite",
                        CoreOptions.STREAMING_READ_APPEND_OVERWRITE.key(),
                        "true");
        write(table, row(1, "a"), row(2, "b"));

        CapturingMetricRegistry registry = new CapturingMetricRegistry();
        StreamTableScan scan = table.newStreamScan();
        scan.withMetricRegistry(registry);
        scan.plan();

        overwrite(table, row(9, "z"));
        long overwriteId = latestId(table);
        scan.plan();

        // FollowUpScanner.getOverwriteChangesPlan takes the DELTA read() path for append tables,
        // so only the ADD entries are read
        Expected added = expectedOf(table, ScanMode.DELTA, overwriteId, FileKind.ADD);
        Expected deleted = expectedOf(table, ScanMode.DELTA, overwriteId, FileKind.DELETE);
        assertThat(deleted.files).isGreaterThan(0);
        assertResultedFiles(registry, added.size, added.records);
    }

    @Test
    public void testStreamingOverwriteOnPrimaryKeyTableReadsBeforeFiles() throws Exception {
        FileStoreTable table =
                createPrimaryKeyTable(
                        "stream_pk_overwrite", CoreOptions.STREAMING_READ_OVERWRITE.key(), "true");
        write(table, row(1, "a"), row(2, "b"));

        CapturingMetricRegistry registry = new CapturingMetricRegistry();
        StreamTableScan scan = table.newStreamScan();
        scan.withMetricRegistry(registry);
        scan.plan();

        overwrite(table, row(1, "z"));
        long overwriteId = latestId(table);
        scan.plan();

        // FollowUpScanner.getOverwriteChangesPlan takes readChanges() for primary key tables, so
        // the DELETE entries are read as before files and must be counted
        Expected added = expectedOf(table, ScanMode.DELTA, overwriteId, FileKind.ADD);
        Expected deleted = expectedOf(table, ScanMode.DELTA, overwriteId, FileKind.DELETE);
        assertThat(deleted.files).isGreaterThan(0);
        assertResultedFiles(registry, added.size + deleted.size, added.records + deleted.records);
    }

    // ------------------------------------------------------------------------------------------
    // helpers
    // ------------------------------------------------------------------------------------------

    private FileStoreTable createAppendTable(String name, String... options) throws Exception {
        return createTable(name, false, options);
    }

    private FileStoreTable createPrimaryKeyTable(String name, String... options) throws Exception {
        return createTable(name, true, options);
    }

    private FileStoreTable createTable(String name, boolean primaryKey, String... options)
            throws Exception {
        Schema.Builder builder =
                Schema.newBuilder()
                        .column("k", DataTypes.INT())
                        .column("v", DataTypes.STRING())
                        .option(CoreOptions.BUCKET.key(), "1");
        if (primaryKey) {
            builder.primaryKey("k");
        } else {
            builder.option(CoreOptions.BUCKET_KEY.key(), "k");
        }
        for (int i = 0; i < options.length; i += 2) {
            builder.option(options[i], options[i + 1]);
        }
        Identifier identifier = identifier(name);
        catalog.createTable(identifier, builder.build(), false);
        return (FileStoreTable) catalog.getTable(identifier);
    }

    private static GenericRow row(int k, String v) {
        return GenericRow.of(k, BinaryString.fromString(v));
    }

    private void overwrite(FileStoreTable table, GenericRow... rows) throws Exception {
        BatchWriteBuilder writeBuilder = table.newBatchWriteBuilder().withOverwrite();
        try (BatchTableWrite write = writeBuilder.newWrite();
                BatchTableCommit commit = writeBuilder.newCommit()) {
            for (GenericRow r : rows) {
                write.write(r);
            }
            commit.commit(write.prepareCommit());
        }
    }

    private static long latestId(FileStoreTable table) {
        return table.snapshotManager().latestSnapshotId();
    }

    /**
     * Independent oracle: folds the entries of the given kind straight out of the raw store scan,
     * without going through {@link SnapshotReader}.
     */
    private static Expected expectedOf(
            FileStoreTable table, ScanMode mode, long snapshotId, FileKind kind) {
        FileStoreScan.Plan plan =
                table.store().newScan().withKind(mode).withSnapshot(snapshotId).plan();
        List<ManifestEntry> entries = plan.files(kind);
        long size = 0L;
        long records = 0L;
        for (ManifestEntry entry : entries) {
            size += entry.file().fileSize();
            records += entry.file().rowCount();
        }
        return new Expected(entries.size(), size, records);
    }

    private static void assertResultedFiles(
            CapturingMetricRegistry registry, long expectedSize, long expectedRecords) {
        assertThat(gauge(registry, ScanMetrics.LAST_SCAN_RESULTED_TABLE_FILES_SIZE))
                .as("resulted table files size")
                .isEqualTo(expectedSize);
        assertThat(gauge(registry, ScanMetrics.LAST_SCAN_RESULTED_RECORD_COUNT))
                .as("resulted record count")
                .isEqualTo(expectedRecords);
    }

    @SuppressWarnings("unchecked")
    private static long gauge(CapturingMetricRegistry registry, String name) {
        MetricGroup group = registry.scanGroup;
        assertThat(group).as("scan metric group was created").isNotNull();
        Gauge<Long> gauge = (Gauge<Long>) group.getMetrics().get(name);
        assertThat(gauge).as("gauge %s", name).isNotNull();
        return gauge.getValue();
    }

    private static final class Expected {
        private final long files;
        private final long size;
        private final long records;

        private Expected(long files, long size, long records) {
            this.files = files;
            this.size = size;
            this.records = records;
        }
    }

    /** Keeps the scan metric group so the gauges can be read back. */
    private static final class CapturingMetricRegistry implements MetricRegistry {

        private MetricGroup scanGroup;

        @Override
        public MetricGroup createMetricGroup(String groupName, Map<String, String> variables) {
            MetricGroup group = new MetricGroupImpl(groupName, variables);
            if (ScanMetrics.GROUP_NAME.equals(groupName)) {
                scanGroup = group;
            }
            return group;
        }
    }
}
