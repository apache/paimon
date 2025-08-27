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

import org.apache.paimon.predicate.FieldRef;
import org.apache.paimon.predicate.TopN;
import org.apache.paimon.stats.SimpleStatsEvolutions;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.StreamTableCommit;
import org.apache.paimon.table.sink.StreamTableWrite;
import org.apache.paimon.table.source.snapshot.ScannerTestBase;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.utils.SnapshotManager;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.paimon.predicate.SortValue.NullOrdering.NULLS_FIRST;
import static org.apache.paimon.predicate.SortValue.NullOrdering.NULLS_LAST;
import static org.apache.paimon.predicate.SortValue.SortDirection.ASCENDING;
import static org.apache.paimon.predicate.SortValue.SortDirection.DESCENDING;
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

    @Test
    public void testPushDownTopN() throws Exception {
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

        write.write(rowData(6, null, 600L));
        write.write(rowData(6, null, 600L));
        write.write(rowData(6, 60, 600L));
        write.write(rowData(7, null, 700L));
        write.write(rowData(7, null, 700L));
        write.write(rowData(7, 70, 700L));
        commit.commit(2, write.prepareCommit(true, 2));
        write.close();
        commit.close();

        // disable stats
        FileStoreTable disableStatsTable =
                table.copy(Collections.singletonMap("metadata.stats-mode", "none"));
        write = disableStatsTable.newWrite(commitUser);
        commit = disableStatsTable.newCommit(commitUser);

        write.write(rowData(8, 80, 800L));
        write.write(rowData(9, 90, 900L));
        commit.commit(3, write.prepareCommit(true, 3));

        // no TopN pushed down
        TableScan.Plan plan1 = table.newScan().plan();
        assertThat(plan1.splits().size()).isEqualTo(9);

        DataField field = table.schema().fields().get(1);
        FieldRef ref = new FieldRef(field.id(), field.name(), field.type());
        SimpleStatsEvolutions evolutions =
                new SimpleStatsEvolutions(
                        (id) -> table.schemaManager().schema(id).fields(), table.schema().id());

        // with bottom1 null first
        TableScan.Plan plan2 =
                table.newScan().withTopN(new TopN(ref, ASCENDING, NULLS_FIRST, 1)).plan();
        List<Split> splits2 = plan2.splits();
        assertThat(splits2.size()).isEqualTo(3);
        assertThat(((DataSplit) splits2.get(0)).minValue(field.id(), field, evolutions))
                .isEqualTo(null);
        assertThat(((DataSplit) splits2.get(1)).minValue(field.id(), field, evolutions))
                .isEqualTo(null);
        assertThat(((DataSplit) splits2.get(2)).minValue(field.id(), field, evolutions))
                .isEqualTo(60);
        assertThat(((DataSplit) splits2.get(2)).nullCount(field.id(), evolutions)).isEqualTo(2);

        // with bottom1 null last
        TableScan.Plan plan3 =
                table.newScan().withTopN(new TopN(ref, ASCENDING, NULLS_LAST, 1)).plan();
        List<Split> splits3 = plan3.splits();
        assertThat(splits3.size()).isEqualTo(3);
        assertThat(((DataSplit) splits3.get(0)).minValue(field.id(), field, evolutions))
                .isEqualTo(null);
        assertThat(((DataSplit) splits3.get(1)).minValue(field.id(), field, evolutions))
                .isEqualTo(null);
        assertThat(((DataSplit) splits3.get(2)).minValue(field.id(), field, evolutions))
                .isEqualTo(10);

        // with bottom5 null first
        TableScan.Plan plan4 =
                table.newScan().withTopN(new TopN(ref, ASCENDING, NULLS_FIRST, 5)).plan();
        List<Split> splits4 = plan4.splits();
        assertThat(splits4.size()).isEqualTo(7);
        assertThat(((DataSplit) splits4.get(0)).minValue(field.id(), field, evolutions))
                .isEqualTo(null);
        assertThat(((DataSplit) splits4.get(1)).minValue(field.id(), field, evolutions))
                .isEqualTo(null);
        assertThat(((DataSplit) splits4.get(2)).minValue(field.id(), field, evolutions))
                .isEqualTo(60);
        assertThat(((DataSplit) splits4.get(2)).nullCount(field.id(), evolutions)).isEqualTo(2);
        assertThat(((DataSplit) splits4.get(3)).minValue(field.id(), field, evolutions))
                .isEqualTo(70);
        assertThat(((DataSplit) splits4.get(3)).nullCount(field.id(), evolutions)).isEqualTo(2);
        assertThat(((DataSplit) splits4.get(4)).minValue(field.id(), field, evolutions))
                .isEqualTo(10);

        // with bottom5 null last
        TableScan.Plan plan5 =
                table.newScan().withTopN(new TopN(ref, ASCENDING, NULLS_LAST, 5)).plan();
        List<Split> splits5 = plan5.splits();
        assertThat(splits5.size()).isEqualTo(7);
        assertThat(((DataSplit) splits5.get(0)).minValue(field.id(), field, evolutions))
                .isEqualTo(null);
        assertThat(((DataSplit) splits5.get(1)).minValue(field.id(), field, evolutions))
                .isEqualTo(null);
        assertThat(((DataSplit) splits5.get(2)).minValue(field.id(), field, evolutions))
                .isEqualTo(10);
        assertThat(((DataSplit) splits5.get(3)).minValue(field.id(), field, evolutions))
                .isEqualTo(20);
        assertThat(((DataSplit) splits5.get(4)).minValue(field.id(), field, evolutions))
                .isEqualTo(30);
        assertThat(((DataSplit) splits5.get(5)).minValue(field.id(), field, evolutions))
                .isEqualTo(40);
        assertThat(((DataSplit) splits5.get(6)).minValue(field.id(), field, evolutions))
                .isEqualTo(50);

        // with top1 null first
        TableScan.Plan plan6 =
                table.newScan().withTopN(new TopN(ref, DESCENDING, NULLS_FIRST, 1)).plan();
        List<Split> splits6 = plan6.splits();
        assertThat(splits6.size()).isEqualTo(3);
        assertThat(((DataSplit) splits6.get(0)).maxValue(field.id(), field, evolutions))
                .isEqualTo(null);
        assertThat(((DataSplit) splits6.get(1)).maxValue(field.id(), field, evolutions))
                .isEqualTo(null);
        assertThat(((DataSplit) splits6.get(2)).maxValue(field.id(), field, evolutions))
                .isEqualTo(70);
        assertThat(((DataSplit) splits6.get(2)).nullCount(field.id(), evolutions)).isEqualTo(2);

        // with top1 null last
        TableScan.Plan plan7 =
                table.newScan().withTopN(new TopN(ref, DESCENDING, NULLS_LAST, 1)).plan();
        List<Split> splits7 = plan7.splits();
        assertThat(splits7.size()).isEqualTo(3);
        assertThat(((DataSplit) splits7.get(0)).maxValue(field.id(), field, evolutions))
                .isEqualTo(null);
        assertThat(((DataSplit) splits7.get(1)).maxValue(field.id(), field, evolutions))
                .isEqualTo(null);
        assertThat(((DataSplit) splits7.get(2)).maxValue(field.id(), field, evolutions))
                .isEqualTo(70);

        // with top5 null first
        TableScan.Plan plan8 =
                table.newScan().withTopN(new TopN(ref, DESCENDING, NULLS_FIRST, 5)).plan();
        List<Split> splits8 = plan8.splits();
        assertThat(splits8.size()).isEqualTo(7);
        assertThat(((DataSplit) splits8.get(0)).maxValue(field.id(), field, evolutions))
                .isEqualTo(null);
        assertThat(((DataSplit) splits8.get(1)).maxValue(field.id(), field, evolutions))
                .isEqualTo(null);
        assertThat(((DataSplit) splits8.get(2)).maxValue(field.id(), field, evolutions))
                .isEqualTo(70);
        assertThat(((DataSplit) splits8.get(2)).nullCount(field.id(), evolutions)).isEqualTo(2);
        assertThat(((DataSplit) splits8.get(3)).maxValue(field.id(), field, evolutions))
                .isEqualTo(60);
        assertThat(((DataSplit) splits8.get(3)).nullCount(field.id(), evolutions)).isEqualTo(2);
        assertThat(((DataSplit) splits8.get(4)).maxValue(field.id(), field, evolutions))
                .isEqualTo(50);

        // with top5 null last
        TableScan.Plan plan9 =
                table.newScan().withTopN(new TopN(ref, DESCENDING, NULLS_LAST, 5)).plan();
        List<Split> splits9 = plan9.splits();
        assertThat(splits9.size()).isEqualTo(7);
        assertThat(((DataSplit) splits9.get(0)).maxValue(field.id(), field, evolutions))
                .isEqualTo(null);
        assertThat(((DataSplit) splits9.get(1)).maxValue(field.id(), field, evolutions))
                .isEqualTo(null);
        assertThat(((DataSplit) splits9.get(2)).maxValue(field.id(), field, evolutions))
                .isEqualTo(70);
        assertThat(((DataSplit) splits9.get(3)).maxValue(field.id(), field, evolutions))
                .isEqualTo(60);
        assertThat(((DataSplit) splits9.get(4)).maxValue(field.id(), field, evolutions))
                .isEqualTo(50);
        assertThat(((DataSplit) splits9.get(5)).maxValue(field.id(), field, evolutions))
                .isEqualTo(40);
        assertThat(((DataSplit) splits9.get(6)).maxValue(field.id(), field, evolutions))
                .isEqualTo(30);

        write.close();
        commit.close();
    }

    @Test
    public void testPushDownTopNSchemaEvolution() throws Exception {
        createAppendOnlyTable();

        StreamTableWrite write = table.newWrite(commitUser);
        StreamTableCommit commit = table.newCommit(commitUser);

        write.write(rowData(1, 10, 100L));
        write.write(rowData(2, 20, 200L));
        write.write(rowData(3, 30, 300L));
        commit.commit(0, write.prepareCommit(true, 0));
        write.close();
        commit.close();

        // schema evolution
        updateColumn("a", DataTypes.BIGINT());
        write = table.newWrite(commitUser);
        commit = table.newCommit(commitUser);
        write.write(rowData(4, 40L, 400L));
        write.write(rowData(5, 50L, 500L));
        write.write(rowData(6, 60L, 600L));
        commit.commit(1, write.prepareCommit(true, 1));
        write.close();
        commit.close();

        DataField field = table.schema().fields().get(1);
        FieldRef ref = new FieldRef(field.id(), field.name(), field.type());
        SimpleStatsEvolutions evolutions =
                new SimpleStatsEvolutions(
                        (id) -> table.schemaManager().schema(id).fields(), table.schema().id());

        TableScan.Plan plan1 =
                table.newScan().withTopN(new TopN(ref, DESCENDING, NULLS_LAST, 1)).plan();
        assertThat(plan1.splits().size()).isEqualTo(1);
        assertThat(((DataSplit) plan1.splits().get(0)).maxValue(field.id(), field, evolutions))
                .isEqualTo(60L);
        assertThat(((DataSplit) plan1.splits().get(0)).minValue(field.id(), field, evolutions))
                .isEqualTo(60L);

        TableScan.Plan plan2 =
                table.newScan().withTopN(new TopN(ref, ASCENDING, NULLS_FIRST, 1)).plan();
        assertThat(plan2.splits().size()).isEqualTo(1);
        assertThat(((DataSplit) plan2.splits().get(0)).maxValue(field.id(), field, evolutions))
                .isEqualTo(10L);
        assertThat(((DataSplit) plan2.splits().get(0)).minValue(field.id(), field, evolutions))
                .isEqualTo(10L);
    }

    @Test
    public void testPushDownTopNOnlyNull() throws Exception {
        createAppendOnlyTable();

        StreamTableWrite write = table.newWrite(commitUser);
        StreamTableCommit commit = table.newCommit(commitUser);

        write.write(rowData(1, null, 100L));
        write.write(rowData(2, null, 200L));
        write.write(rowData(3, null, 300L));
        commit.commit(0, write.prepareCommit(true, 0));
        write.close();
        commit.close();

        DataField field = table.schema().fields().get(1);
        FieldRef ref = new FieldRef(field.id(), field.name(), field.type());
        SimpleStatsEvolutions evolutions =
                new SimpleStatsEvolutions(
                        (id) -> table.schemaManager().schema(id).fields(), table.schema().id());

        // the min/max will be null, and the null count is not null.
        TableScan.Plan plan1 =
                table.newScan().withTopN(new TopN(ref, DESCENDING, NULLS_LAST, 1)).plan();
        assertThat(plan1.splits().size()).isEqualTo(1);
        assertThat(((DataSplit) plan1.splits().get(0)).nullCount(field.id(), evolutions))
                .isEqualTo(1);
        assertThat(((DataSplit) plan1.splits().get(0)).minValue(field.id(), field, evolutions))
                .isNull();
        assertThat(((DataSplit) plan1.splits().get(0)).maxValue(field.id(), field, evolutions))
                .isNull();

        TableScan.Plan plan2 =
                table.newScan().withTopN(new TopN(ref, ASCENDING, NULLS_FIRST, 1)).plan();
        assertThat(plan2.splits().size()).isEqualTo(1);
        assertThat(((DataSplit) plan2.splits().get(0)).nullCount(field.id(), evolutions))
                .isEqualTo(1);
        assertThat(((DataSplit) plan2.splits().get(0)).minValue(field.id(), field, evolutions))
                .isNull();
        assertThat(((DataSplit) plan2.splits().get(0)).maxValue(field.id(), field, evolutions))
                .isNull();
    }
}
