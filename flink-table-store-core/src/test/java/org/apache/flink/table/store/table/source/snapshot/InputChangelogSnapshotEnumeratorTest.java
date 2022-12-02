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
import org.apache.flink.table.store.table.FileStoreTable;
import org.apache.flink.table.store.table.sink.TableCommit;
import org.apache.flink.table.store.table.sink.TableWrite;
import org.apache.flink.table.store.table.source.DataTableScan;
import org.apache.flink.table.store.table.source.TableRead;
import org.apache.flink.types.RowKind;

import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link InputChangelogSnapshotEnumerator}. */
public class InputChangelogSnapshotEnumeratorTest extends DataFileSnapshotEnumeratorTestBase {

    @Test
    public void testFullStartupMode() throws Exception {
        FileStoreTable table = createFileStoreTable();
        TableRead read = table.newRead();
        TableWrite write = table.newWrite(commitUser);
        TableCommit commit = table.newCommit(commitUser);
        SnapshotEnumerator enumerator =
                new InputChangelogSnapshotEnumerator(
                        tablePath, table.newScan(), CoreOptions.LogStartupMode.FULL, null, null);

        // first call without any snapshot, should return null
        assertThat(enumerator.enumerate()).isNull();

        // write base data
        write.write(rowData(1, 10, 100L));
        write.write(rowData(1, 20, 200L));
        write.write(rowData(1, 40, 400L));
        commit.commit(0, write.prepareCommit(true, 0));

        write.write(rowData(1, 10, 101L));
        write.write(rowData(1, 30, 300L));
        write.write(rowDataWithKind(RowKind.DELETE, 1, 40, 400L));
        commit.commit(1, write.prepareCommit(true, 1));

        // first call with snapshot, should return complete records from 2nd commit
        DataTableScan.DataFilePlan plan = enumerator.enumerate();
        assertThat(plan.snapshotId).isEqualTo(2);
        assertThat(getResult(read, plan.splits()))
                .hasSameElementsAs(Arrays.asList("+I 1|10|101", "+I 1|20|200", "+I 1|30|300"));

        // incremental call without new snapshots, should return null
        assertThat(enumerator.enumerate()).isNull();

        // write incremental data
        write.write(rowDataWithKind(RowKind.DELETE, 1, 10, 101L));
        write.write(rowData(1, 20, 201L));
        write.write(rowData(1, 10, 102L));
        write.write(rowData(1, 40, 400L));
        commit.commit(2, write.prepareCommit(true, 2));

        write.write(rowData(1, 10, 103L));
        write.write(rowDataWithKind(RowKind.DELETE, 1, 40, 400L));
        write.write(rowData(1, 50, 500L));
        commit.commit(3, write.prepareCommit(true, 3));

        // first incremental call, should return incremental records from 3rd commit
        plan = enumerator.enumerate();
        assertThat(plan.snapshotId).isEqualTo(3);
        assertThat(getResult(read, plan.splits()))
                .hasSameElementsAs(
                        Arrays.asList("-D 1|10|101", "+I 1|10|102", "+I 1|20|201", "+I 1|40|400"));

        // second incremental call, should return incremental records from 4th commit
        plan = enumerator.enumerate();
        assertThat(plan.snapshotId).isEqualTo(4);
        assertThat(getResult(read, plan.splits()))
                .hasSameElementsAs(Arrays.asList("+I 1|10|103", "-D 1|40|400", "+I 1|50|500"));

        // no more new snapshots, should return null
        assertThat(enumerator.enumerate()).isNull();

        write.close();
        commit.close();
    }

    @Test
    public void testFromTimestampStartupMode() throws Exception {
        FileStoreTable table = createFileStoreTable();
        TableRead read = table.newRead();
        TableWrite write = table.newWrite(commitUser);
        TableCommit commit = table.newCommit(commitUser);

        write.write(rowData(1, 10, 100L));
        write.write(rowData(1, 20, 200L));
        write.write(rowData(1, 40, 400L));
        commit.commit(0, write.prepareCommit(true, 0));

        // log current time millis, we'll start from here
        Thread.sleep(50);
        long startMillis = System.currentTimeMillis();
        Thread.sleep(50);

        write.write(rowData(1, 10, 101L));
        write.write(rowData(1, 30, 300L));
        write.write(rowDataWithKind(RowKind.DELETE, 1, 40, 400L));
        commit.commit(1, write.prepareCommit(true, 1));

        write.write(rowDataWithKind(RowKind.DELETE, 1, 10, 101L));
        write.write(rowData(1, 20, 201L));
        write.write(rowData(1, 10, 102L));
        write.write(rowData(1, 40, 400L));
        commit.commit(2, write.prepareCommit(true, 2));

        // first call with snapshot, should return empty plan
        SnapshotEnumerator enumerator =
                new InputChangelogSnapshotEnumerator(
                        tablePath,
                        table.newScan(),
                        CoreOptions.LogStartupMode.FROM_TIMESTAMP,
                        startMillis,
                        null);

        DataTableScan.DataFilePlan plan = enumerator.enumerate();
        assertThat(plan.snapshotId).isEqualTo(1);
        assertThat(plan.splits()).isEmpty();

        // first incremental call, should return incremental records from 2nd commit
        plan = enumerator.enumerate();
        assertThat(plan.snapshotId).isEqualTo(2);
        assertThat(getResult(read, plan.splits()))
                .hasSameElementsAs(Arrays.asList("+I 1|10|101", "+I 1|30|300", "-D 1|40|400"));

        // second incremental call, should return incremental records from 3rd commit
        plan = enumerator.enumerate();
        assertThat(plan.snapshotId).isEqualTo(3);
        assertThat(getResult(read, plan.splits()))
                .hasSameElementsAs(
                        Arrays.asList("-D 1|10|101", "+I 1|10|102", "+I 1|20|201", "+I 1|40|400"));

        // no new snapshots
        assertThat(enumerator.enumerate()).isNull();

        // more incremental records
        write.write(rowData(1, 10, 103L));
        write.write(rowDataWithKind(RowKind.DELETE, 1, 40, 400L));
        write.write(rowData(1, 50, 500L));
        commit.commit(3, write.prepareCommit(true, 3));

        plan = enumerator.enumerate();
        assertThat(plan.snapshotId).isEqualTo(4);
        assertThat(getResult(read, plan.splits()))
                .hasSameElementsAs(Arrays.asList("+I 1|10|103", "-D 1|40|400", "+I 1|50|500"));

        assertThat(enumerator.enumerate()).isNull();

        write.close();
        commit.close();
    }

    @Test
    public void testLatestStartupMode() throws Exception {
        FileStoreTable table = createFileStoreTable();
        TableRead read = table.newRead();
        TableWrite write = table.newWrite(commitUser);
        TableCommit commit = table.newCommit(commitUser);

        write.write(rowData(1, 10, 100L));
        write.write(rowData(1, 20, 200L));
        write.write(rowData(1, 40, 400L));
        commit.commit(0, write.prepareCommit(true, 0));

        write.write(rowData(1, 10, 101L));
        write.write(rowData(1, 30, 300L));
        write.write(rowDataWithKind(RowKind.DELETE, 1, 40, 400L));
        commit.commit(1, write.prepareCommit(true, 1));

        SnapshotEnumerator enumerator =
                new InputChangelogSnapshotEnumerator(
                        tablePath, table.newScan(), CoreOptions.LogStartupMode.LATEST, null, null);

        DataTableScan.DataFilePlan plan = enumerator.enumerate();
        assertThat(plan.snapshotId).isEqualTo(2);
        assertThat(plan.splits()).isEmpty();

        assertThat(enumerator.enumerate()).isNull();

        write.write(rowDataWithKind(RowKind.DELETE, 1, 10, 101L));
        write.write(rowData(1, 20, 201L));
        write.write(rowData(1, 10, 102L));
        write.write(rowData(1, 40, 400L));
        commit.commit(2, write.prepareCommit(true, 2));

        plan = enumerator.enumerate();
        assertThat(plan.snapshotId).isEqualTo(3);
        assertThat(getResult(read, plan.splits()))
                .hasSameElementsAs(
                        Arrays.asList("-D 1|10|101", "+I 1|10|102", "+I 1|20|201", "+I 1|40|400"));
        assertThat(enumerator.enumerate()).isNull();

        write.write(rowData(1, 10, 103L));
        write.write(rowDataWithKind(RowKind.DELETE, 1, 40, 400L));
        write.write(rowData(1, 50, 500L));
        commit.commit(3, write.prepareCommit(true, 3));

        plan = enumerator.enumerate();
        assertThat(plan.snapshotId).isEqualTo(4);
        assertThat(getResult(read, plan.splits()))
                .hasSameElementsAs(Arrays.asList("+I 1|10|103", "-D 1|40|400", "+I 1|50|500"));
        assertThat(enumerator.enumerate()).isNull();

        write.close();
        commit.close();
    }

    @Override
    protected Configuration getConf() {
        Configuration conf = new Configuration();
        conf.set(CoreOptions.CHANGELOG_PRODUCER, CoreOptions.ChangelogProducer.INPUT);
        return conf;
    }
}
