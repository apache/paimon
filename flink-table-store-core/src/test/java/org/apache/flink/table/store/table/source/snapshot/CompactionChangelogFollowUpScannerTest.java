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

package org.apache.flink.table.store.table.source.snapshot;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.store.CoreOptions;
import org.apache.flink.table.store.file.Snapshot;
import org.apache.flink.table.store.file.utils.SnapshotManager;
import org.apache.flink.table.store.table.FileStoreTable;
import org.apache.flink.table.store.table.sink.TableCommit;
import org.apache.flink.table.store.table.sink.TableWrite;
import org.apache.flink.table.store.table.source.DataTableScan;
import org.apache.flink.table.store.table.source.TableRead;
import org.apache.flink.table.store.types.RowKind;

import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link CompactionChangelogFollowUpScanner}. */
public class CompactionChangelogFollowUpScannerTest extends SnapshotEnumeratorTestBase {

    @Test
    public void testGetPlan() throws Exception {
        FileStoreTable table = createFileStoreTable();
        SnapshotManager snapshotManager = table.snapshotManager();
        TableWrite write = table.newWrite(commitUser);
        TableCommit commit = table.newCommit(commitUser);

        write.write(rowData(1, 10, 100L));
        write.write(rowData(1, 20, 200L));
        write.write(rowData(1, 40, 400L));
        commit.commit(0, write.prepareCommit(true, 0));

        write.write(rowData(1, 10, 101L));
        write.write(rowData(1, 30, 300L));
        write.write(rowData(1, 10, 102L));
        write.write(rowDataWithKind(RowKind.DELETE, 1, 40, 400L));
        write.compact(binaryRow(1), 0, true);
        commit.commit(1, write.prepareCommit(true, 1));

        write.write(rowData(1, 10, 103L));
        write.write(rowDataWithKind(RowKind.DELETE, 1, 30, 300L));
        write.write(rowData(1, 40, 401L));
        write.compact(binaryRow(1), 0, true);
        commit.commit(1, write.prepareCommit(true, 1));

        assertThat(snapshotManager.latestSnapshotId()).isEqualTo(5);

        DataTableScan scan = table.newScan().withLevel(table.options().numLevels() - 1);
        TableRead read = table.newRead();
        CompactionChangelogFollowUpScanner scanner = new CompactionChangelogFollowUpScanner();

        Snapshot snapshot = snapshotManager.snapshot(1);
        assertThat(snapshot.commitKind()).isEqualTo(Snapshot.CommitKind.APPEND);
        assertThat(scanner.shouldScanSnapshot(snapshot)).isFalse();

        snapshot = snapshotManager.snapshot(2);
        assertThat(snapshot.commitKind()).isEqualTo(Snapshot.CommitKind.APPEND);
        assertThat(scanner.shouldScanSnapshot(snapshot)).isFalse();

        snapshot = snapshotManager.snapshot(3);
        assertThat(snapshot.commitKind()).isEqualTo(Snapshot.CommitKind.COMPACT);
        assertThat(scanner.shouldScanSnapshot(snapshot)).isTrue();
        DataTableScan.DataFilePlan plan = scanner.getPlan(3, scan);
        assertThat(plan.snapshotId).isEqualTo(3);
        assertThat(getResult(read, plan.splits()))
                .hasSameElementsAs(Arrays.asList("+I 1|10|102", "+I 1|20|200", "+I 1|30|300"));

        snapshot = snapshotManager.snapshot(4);
        assertThat(snapshot.commitKind()).isEqualTo(Snapshot.CommitKind.APPEND);
        assertThat(scanner.shouldScanSnapshot(snapshot)).isFalse();

        snapshot = snapshotManager.snapshot(5);
        assertThat(snapshot.commitKind()).isEqualTo(Snapshot.CommitKind.COMPACT);
        assertThat(scanner.shouldScanSnapshot(snapshot)).isTrue();
        plan = scanner.getPlan(5, scan);
        assertThat(plan.snapshotId).isEqualTo(5);
        assertThat(getResult(read, plan.splits()))
                .hasSameElementsAs(
                        Arrays.asList("-U 1|10|102", "+U 1|10|103", "-D 1|30|300", "+I 1|40|401"));

        write.close();
        commit.close();
    }

    @Override
    protected FileStoreTable createFileStoreTable() throws Exception {
        Configuration conf = new Configuration();
        conf.set(CoreOptions.CHANGELOG_PRODUCER, CoreOptions.ChangelogProducer.FULL_COMPACTION);
        return createFileStoreTable(conf);
    }
}
