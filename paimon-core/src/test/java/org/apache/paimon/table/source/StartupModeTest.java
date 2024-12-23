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

package org.apache.paimon.table.source;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.fs.FileIOFinder;
import org.apache.paimon.fs.Path;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.StreamTableCommit;
import org.apache.paimon.table.sink.StreamTableWrite;
import org.apache.paimon.table.source.snapshot.ScannerTestBase;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.utils.IOUtils;
import org.apache.paimon.utils.TraceableFileIO;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.apache.paimon.CoreOptions.StartupMode;
import static org.apache.paimon.CoreOptions.TABLE_SCHEMA_PATH;
import static org.apache.paimon.testutils.assertj.PaimonAssertions.anyCauseMatches;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link StartupMode}. */
public class StartupModeTest extends ScannerTestBase {

    StreamTableWrite write;
    StreamTableCommit commit;

    @BeforeEach
    @Override
    public void before() throws Exception {
        tablePath = new Path(TraceableFileIO.SCHEME + "://" + tempDir.toString());
        fileIO = FileIOFinder.find(tablePath);
        commitUser = UUID.randomUUID().toString();
    }

    @Test
    public void testStartFromLatest() throws Exception {
        initializeTable(StartupMode.LATEST);
        initializeTestData(); // initialize 3 commits

        // streaming Mode
        StreamTableScan dataTableScan = table.newStreamScan();
        TableScan.Plan firstPlan = dataTableScan.plan();
        TableScan.Plan secondPlan = dataTableScan.plan();

        assertThat(firstPlan.splits()).isEmpty();
        assertThat(secondPlan.splits()).isEmpty();

        // write next data
        writeAndCommit(4, rowData(1, 10, 103L));
        TableScan.Plan thirdPlan = dataTableScan.plan();
        assertThat(thirdPlan.splits())
                .isEqualTo(snapshotReader.withSnapshot(4).withMode(ScanMode.DELTA).read().splits());

        // batch mode
        TableScan batchScan = table.newScan();
        TableScan.Plan plan = batchScan.plan();
        assertThat(plan.splits())
                .isEqualTo(snapshotReader.withSnapshot(4).withMode(ScanMode.ALL).read().splits());
    }

    @Test
    public void testStartFromLatestFull() throws Exception {
        initializeTable(StartupMode.LATEST_FULL);
        initializeTestData(); // initialize 3 commits

        // streaming Mode
        StreamTableScan dataTableScan = table.newStreamScan();
        TableScan.Plan firstPlan = dataTableScan.plan();
        TableScan.Plan secondPlan = dataTableScan.plan();

        assertThat(firstPlan.splits())
                .isEqualTo(snapshotReader.withSnapshot(3).withMode(ScanMode.ALL).read().splits());
        assertThat(secondPlan.splits()).isEmpty();

        // write next data
        writeAndCommit(4, rowData(1, 10, 103L));
        TableScan.Plan thirdPlan = dataTableScan.plan();
        assertThat(thirdPlan.splits())
                .isEqualTo(snapshotReader.withSnapshot(4).withMode(ScanMode.DELTA).read().splits());

        // batch mode
        TableScan batchScan = table.newScan();
        TableScan.Plan plan = batchScan.plan();
        assertThat(plan.splits())
                .isEqualTo(snapshotReader.withSnapshot(4).withMode(ScanMode.ALL).read().splits());
    }

    @Test
    public void testStartFromTimestamp() throws Exception {
        initializeTable(StartupMode.LATEST);
        initializeTestData(); // initialize 3 commits

        long timestamp = System.currentTimeMillis();
        Thread.sleep(10L);

        // write next data
        writeAndCommit(4, rowData(1, 10, 103L));

        Map<String, String> properties = new HashMap<>();
        properties.put(CoreOptions.SCAN_MODE.key(), StartupMode.FROM_TIMESTAMP.toString());
        properties.put(CoreOptions.SCAN_TIMESTAMP_MILLIS.key(), String.valueOf(timestamp));
        FileStoreTable readTable = table.copy(properties);

        // streaming Mode
        StreamTableScan dataTableScan = readTable.newStreamScan();
        TableScan.Plan firstPlan = dataTableScan.plan();
        TableScan.Plan secondPlan = dataTableScan.plan();

        assertThat(firstPlan.splits()).isEmpty();
        assertThat(secondPlan.splits())
                .isEqualTo(snapshotReader.withSnapshot(4).withMode(ScanMode.DELTA).read().splits());

        // batch mode
        TableScan batchScan = readTable.newScan();
        TableScan.Plan plan = batchScan.plan();
        assertThat(plan.splits())
                .isEqualTo(snapshotReader.withSnapshot(3).withMode(ScanMode.ALL).read().splits());
    }

    @Test
    public void testStartFromTimestampString() throws Exception {
        initializeTable(StartupMode.LATEST);
        initializeTestData(); // initialize 3 commits
        String timestampString =
                LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS"));
        Thread.sleep(10L);

        // write next data
        writeAndCommit(4, rowData(1, 10, 103L));

        Map<String, String> properties = new HashMap<>();
        properties.put(CoreOptions.SCAN_MODE.key(), StartupMode.FROM_TIMESTAMP.toString());
        properties.put(CoreOptions.SCAN_TIMESTAMP.key(), timestampString);
        FileStoreTable readTable = table.copy(properties);

        // streaming Mode
        StreamTableScan dataTableScan = readTable.newStreamScan();
        TableScan.Plan firstPlan = dataTableScan.plan();
        TableScan.Plan secondPlan = dataTableScan.plan();

        assertThat(firstPlan.splits()).isEmpty();
        assertThat(secondPlan.splits())
                .isEqualTo(snapshotReader.withSnapshot(4).withMode(ScanMode.DELTA).read().splits());

        // batch mode
        TableScan batchScan = readTable.newScan();
        TableScan.Plan plan = batchScan.plan();
        assertThat(plan.splits())
                .isEqualTo(snapshotReader.withSnapshot(3).withMode(ScanMode.ALL).read().splits());
    }

    @Test
    public void testStartFromCompactedFull() throws Exception {
        initializeTable(StartupMode.COMPACTED_FULL);
        initializeTestData(); // initialize 3 commits

        write.compact(binaryRow(1), 0, true);
        commit.commit(4, write.prepareCommit(true, 4));
        writeAndCommit(5, rowData(1, 10, 103L));

        // streaming Mode
        StreamTableScan dataTableScan = table.newStreamScan();
        TableScan.Plan firstPlan = dataTableScan.plan();
        TableScan.Plan secondPlan = dataTableScan.plan();

        assertThat(firstPlan.splits())
                .isEqualTo(snapshotReader.withSnapshot(4).withMode(ScanMode.ALL).read().splits());
        assertThat(secondPlan.splits())
                .isEqualTo(snapshotReader.withSnapshot(5).withMode(ScanMode.DELTA).read().splits());

        // batch mode
        TableScan batchScan = table.newScan();
        TableScan.Plan plan = batchScan.plan();
        assertThat(plan.splits())
                .isEqualTo(snapshotReader.withSnapshot(4).withMode(ScanMode.ALL).read().splits());
    }

    @Test
    public void testStartFromSnapshot() throws Exception {
        Map<String, String> properties = new HashMap<>();
        properties.put(CoreOptions.SCAN_SNAPSHOT_ID.key(), "2");
        initializeTable(StartupMode.FROM_SNAPSHOT, properties);
        initializeTestData(); // initialize 3 commits

        // streaming Mode
        StreamTableScan dataTableScan = table.newStreamScan();
        TableScan.Plan firstPlan = dataTableScan.plan();
        TableScan.Plan secondPlan = dataTableScan.plan();

        assertThat(firstPlan.splits()).isEmpty();
        assertThat(secondPlan.splits())
                .isEqualTo(snapshotReader.withSnapshot(2).withMode(ScanMode.DELTA).read().splits());

        // batch mode
        TableScan batchScan = table.newScan();
        TableScan.Plan plan = batchScan.plan();
        assertThat(plan.splits())
                .isEqualTo(snapshotReader.withSnapshot(2).withMode(ScanMode.ALL).read().splits());
    }

    @Test
    public void testStartFromSnapshotWithDelayDuration() throws Exception {
        Map<String, String> properties = new HashMap<>();
        properties.put(CoreOptions.SCAN_SNAPSHOT_ID.key(), "2");
        properties.put(CoreOptions.STREAMING_READ_SNAPSHOT_DELAY.key(), "5 s");
        initializeTable(StartupMode.FROM_SNAPSHOT, properties);
        initializeTestData(); // initialize 3 commits

        // streaming Mode
        StreamTableScan dataTableScan = table.newStreamScan();
        TableScan.Plan firstPlan = dataTableScan.plan();
        assertThat(firstPlan.splits()).isEmpty();

        Thread.sleep(3000);
        TableScan.Plan secondPlan = dataTableScan.plan();
        assertThat(secondPlan.splits()).isEmpty();

        Thread.sleep(5000);
        TableScan.Plan thirdPlan = dataTableScan.plan();

        assertThat(thirdPlan.splits())
                .isEqualTo(snapshotReader.withSnapshot(2).withMode(ScanMode.DELTA).read().splits());
    }

    @Test
    public void testStartFromSnapshotWithoutDelayDuration() throws Exception {
        Map<String, String> properties = new HashMap<>();
        properties.put(CoreOptions.SCAN_SNAPSHOT_ID.key(), "2");
        initializeTable(StartupMode.FROM_SNAPSHOT, properties);
        initializeTestData(); // initialize 3 commits

        // streaming Mode
        StreamTableScan dataTableScan = table.newStreamScan();
        TableScan.Plan firstPlan = dataTableScan.plan();

        long startTime = System.currentTimeMillis();
        TableScan.Plan secondPlan = dataTableScan.plan();
        long endTime = System.currentTimeMillis();
        long duration = endTime - startTime;

        // without delay read test
        assertThat(duration).isLessThan(100);

        assertThat(firstPlan.splits()).isEmpty();
        assertThat(secondPlan.splits())
                .isEqualTo(snapshotReader.withSnapshot(2).withMode(ScanMode.DELTA).read().splits());
    }

    @Test
    public void testTimeTravelFromExpiredSnapshot() throws Exception {
        Map<String, String> properties = new HashMap<>();
        // retain 2 snapshots
        properties.put(CoreOptions.SNAPSHOT_NUM_RETAINED_MAX.key(), "2");
        properties.put(CoreOptions.SNAPSHOT_NUM_RETAINED_MIN.key(), "2");
        // specify consume from an expired snapshot
        properties.put(CoreOptions.SCAN_SNAPSHOT_ID.key(), "1");
        initializeTable(StartupMode.FROM_SNAPSHOT, properties);
        initializeTestData(); // initialize 3 commits, expired snapshot 1

        // streaming Mode
        StreamTableScan dataTableScan = table.newStreamScan();
        TableScan.Plan firstPlan = dataTableScan.plan();
        TableScan.Plan secondPlan = dataTableScan.plan();

        assertThat(firstPlan.splits()).isEmpty();
        // ceiled up to the earliest snapshot id = 2
        assertThat(secondPlan.splits())
                .isEqualTo(snapshotReader.withSnapshot(2).withMode(ScanMode.DELTA).read().splits());

        // batch mode
        TableScan batchScan = table.newScan();
        assertThatThrownBy(() -> batchScan.plan())
                .satisfies(
                        anyCauseMatches(
                                IllegalArgumentException.class,
                                "The specified scan snapshotId 1 is out of available snapshotId range [2, 3]."));
    }

    @Test
    public void testStartFromSnapshotFull() throws Exception {
        Map<String, String> properties = new HashMap<>();
        properties.put(CoreOptions.SCAN_SNAPSHOT_ID.key(), "2");
        initializeTable(StartupMode.FROM_SNAPSHOT_FULL, properties);
        initializeTestData(); // initialize 3 commits

        StreamTableScan dataTableScan = table.newStreamScan();
        TableScan.Plan firstPlan = dataTableScan.plan();
        TableScan.Plan secondPlan = dataTableScan.plan();

        assertThat(firstPlan.splits())
                .isEqualTo(snapshotReader.withSnapshot(2).withMode(ScanMode.ALL).read().splits());
        assertThat(secondPlan.splits())
                .isEqualTo(snapshotReader.withSnapshot(3).withMode(ScanMode.DELTA).read().splits());

        // batch mode
        TableScan batchScan = table.newScan();
        TableScan.Plan plan = batchScan.plan();
        assertThat(plan.splits())
                .isEqualTo(snapshotReader.withSnapshot(2).withMode(ScanMode.ALL).read().splits());
    }

    @Test
    public void testTimeTravelFromExpiredSnapshotFull() throws Exception {
        Map<String, String> properties = new HashMap<>();
        // retaine 2 snapshots
        properties.put(CoreOptions.SNAPSHOT_NUM_RETAINED_MAX.key(), "2");
        properties.put(CoreOptions.SNAPSHOT_NUM_RETAINED_MIN.key(), "2");
        // specify consume from a expired snapshot
        properties.put(CoreOptions.SCAN_SNAPSHOT_ID.key(), "1");
        initializeTable(StartupMode.FROM_SNAPSHOT_FULL, properties);
        initializeTestData(); // initialize 3 commits, expired snapshot 1

        StreamTableScan dataTableScan = table.newStreamScan();
        TableScan.Plan firstPlan = dataTableScan.plan();
        TableScan.Plan secondPlan = dataTableScan.plan();

        // ceiled up to the earliest snapshot id = 2
        assertThat(firstPlan.splits())
                .isEqualTo(snapshotReader.withSnapshot(2).withMode(ScanMode.ALL).read().splits());
        assertThat(secondPlan.splits())
                .isEqualTo(snapshotReader.withSnapshot(3).withMode(ScanMode.DELTA).read().splits());

        // batch mode
        TableScan batchScan = table.newScan();
        assertThatThrownBy(() -> batchScan.plan())
                .satisfies(
                        anyCauseMatches(
                                IllegalArgumentException.class,
                                "The specified scan snapshotId 1 is out of available snapshotId range [2, 3]."));
    }

    private void initializeTable(CoreOptions.StartupMode startupMode) throws Exception {
        initializeTable(startupMode, Collections.emptyMap());
    }

    private void initializeTable(
            CoreOptions.StartupMode startupMode, Map<String, String> properties) throws Exception {
        Options options = new Options();
        options.set(TABLE_SCHEMA_PATH, tablePath.toString());
        options.set(CoreOptions.SCAN_MODE, startupMode);
        for (Map.Entry<String, String> property : properties.entrySet()) {
            options.set(property.getKey(), property.getValue());
        }
        table = createFileStoreTable(options);
        snapshotReader = table.newSnapshotReader();
        write = table.newWrite(commitUser);
        commit = table.newCommit(commitUser);
    }

    private void initializeTestData() throws Exception {
        write.write(rowData(1, 10, 100L));
        write.write(rowData(1, 20, 200L));
        write.write(rowData(1, 40, 400L));
        commit.commit(1, write.prepareCommit(true, 1));

        write.write(rowData(1, 10, 101L));
        write.write(rowData(1, 30, 300L));
        write.write(rowDataWithKind(RowKind.DELETE, 1, 40, 400L));
        commit.commit(2, write.prepareCommit(true, 2));

        write.write(rowData(1, 10, 102L));
        write.write(rowData(1, 30, 400L));
        commit.commit(3, write.prepareCommit(true, 3));
    }

    private void writeAndCommit(long commitIdentifier, GenericRow... rows) throws Exception {
        for (GenericRow row : rows) {
            write.write(row);
        }
        commit.commit(commitIdentifier, write.prepareCommit(true, commitIdentifier));
    }

    @AfterEach
    public void afterEach() throws Exception {
        IOUtils.closeAll(write, commit);
    }
}
