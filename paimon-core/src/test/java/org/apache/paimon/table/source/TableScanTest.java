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

import org.apache.paimon.table.sink.StreamTableCommit;
import org.apache.paimon.table.sink.StreamTableWrite;
import org.apache.paimon.table.source.snapshot.ScannerTestBase;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.utils.SnapshotManager;

import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link TableScan}. */
public class TableScanTest extends ScannerTestBase {

    @Test
    public void testPlan() throws Exception {
        SnapshotManager snapshotManager = table.snapshotManager();
        StreamTableWrite write = table.newWrite(commitUser);
        StreamTableCommit commit = table.newCommit(commitUser);
        TableScan scan = table.newScan();

        write.write(rowData(1, 10, 100L));
        write.write(rowData(1, 20, 200L));
        write.write(rowData(1, 40, 400L));
        commit.commit(0, write.prepareCommit(true, 0));

        write.write(rowData(1, 10, 101L));
        write.write(rowData(1, 30, 300L));
        write.write(rowDataWithKind(RowKind.DELETE, 1, 40, 400L));
        commit.commit(1, write.prepareCommit(true, 1));

        assertThat(snapshotManager.latestSnapshotId()).isEqualTo(2);

        TableScan.Plan plan = scan.plan();
        assertThat(getResult(table.newRead(), plan.splits()))
                .hasSameElementsAs(Arrays.asList("+I 1|10|101", "+I 1|20|200", "+I 1|30|300"));

        write.write(rowData(1, 10, 102L));
        write.write(rowData(1, 30, 301L));
        commit.commit(2, write.prepareCommit(true, 2));

        assertThatThrownBy(scan::plan).isInstanceOf(EndOfScanException.class);

        write.close();
        commit.close();
    }

    @Test
    public void testPushDownLimit() throws Exception {
        createAppendOnlyTable();

        StreamTableWrite write = table.newWrite(commitUser);
        StreamTableCommit commit = table.newCommit(commitUser);

        write.write(rowData(1, 10, 100L));
        write.write(rowData(2, 20, 200L));
        write.write(rowData(3, 30, 300L));
        commit.commit(0, write.prepareCommit(true, 0));

        write.write(rowData(4, 40, 400L));
        write.write(rowData(5, 50, 500L));
        commit.commit(1, write.prepareCommit(true, 1));

        // no limit pushed down
        TableScan.Plan plan1 = table.newScan().plan();
        assertThat(plan1.splits().size()).isEqualTo(5);

        // with limit 1
        TableScan.Plan plan2 = table.newScan().withLimit(1).plan();
        assertThat(plan2.splits().size()).isEqualTo(1);

        // with limit4
        TableScan.Plan plan3 = table.newScan().withLimit(4).plan();
        assertThat(plan3.splits().size()).isEqualTo(4);

        write.close();
        commit.close();
    }
}
