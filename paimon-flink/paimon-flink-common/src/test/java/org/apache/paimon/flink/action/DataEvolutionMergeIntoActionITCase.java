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

package org.apache.paimon.flink.action;

import org.apache.paimon.utils.BlockingIterator;

import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.apache.paimon.CoreOptions.DATA_EVOLUTION_ENABLED;
import static org.apache.paimon.CoreOptions.ROW_TRACKING_ENABLED;
import static org.apache.paimon.flink.util.ReadWriteTableTestUtil.bEnv;
import static org.apache.paimon.flink.util.ReadWriteTableTestUtil.buildDdl;
import static org.apache.paimon.flink.util.ReadWriteTableTestUtil.init;
import static org.apache.paimon.flink.util.ReadWriteTableTestUtil.insertInto;
import static org.apache.paimon.flink.util.ReadWriteTableTestUtil.sEnv;
import static org.junit.jupiter.api.Assertions.assertEquals;

/** ITCase for {@link DataEvolutionMergeIntoAction}. */
public class DataEvolutionMergeIntoActionITCase extends ActionITCaseBase {

    @BeforeEach
    public void setup() throws Exception {
        init(warehouse);

        prepareTargetTable();

        prepareSourceTable();
    }

    @Test
    public void test() throws Exception {
        Map<String, String> config = new HashMap<>();
        config.put("warehouse", warehouse);
        DataEvolutionMergeIntoAction action =
                new DataEvolutionMergeIntoAction(database, "T", config);
        action.withMergeCondition("T.id=S.id")
                .withMatchedUpdateSet("T.name=S.name")
                .withSourceTable("S")
                .withSinkParallelism(2);

        //        Tuple2<DataStream<RowData>, RowType> source = action.buildSource();
        //        DataStream<Tuple2<Long, RowData>> assigned =
        //                action.shuffleByFirstRowId(source.f0, source.f1);
        //        DataStream<Committable> written = action.writePartialColumns(assigned, source.f1,
        // 1);
        //        DataStream<Committable> committed = action.commit(written);
        //        try (CloseableIterator<Committable> result = committed.executeAndCollect()) {
        //            while (result.hasNext()) {
        //                Committable committable = result.next();
        //                CommitMessageImpl commitMessage = (CommitMessageImpl)
        // committable.commitMessage();
        //                System.out.println(commitMessage);
        //            }
        //        }

        action.run();

        CloseableIterator<Row> finalData = bEnv.executeSql("SELECT * FROM T").collect();
        try (BlockingIterator<Row, Row> result = BlockingIterator.of(finalData)) {
            for (Row row : result.collect(20)) {
                System.out.println(row);
            }
        }
    }

    @Test
    public void testRewriteMergeCondition() throws Exception {
        Map<String, String> config = new HashMap<>();
        config.put("warehouse", warehouse);
        DataEvolutionMergeIntoAction action =
                new DataEvolutionMergeIntoAction(database, "T", config);

        String mergeCondition = "T.id=S.id";
        assertEquals("`RT`.id=S.id", action.rewriteMergeCondition(mergeCondition));

        mergeCondition = "`T`.id=S.id";
        assertEquals("`RT`.id=S.id", action.rewriteMergeCondition(mergeCondition));

        mergeCondition = "t.id = s.id AND T.pt = s.pt";
        assertEquals(
                "`RT`.id = s.id AND `RT`.pt = s.pt", action.rewriteMergeCondition(mergeCondition));

        mergeCondition = "TT.id = 1 AND T.id = 2";
        assertEquals("TT.id = 1 AND `RT`.id = 2", action.rewriteMergeCondition(mergeCondition));

        mergeCondition = "TT.id = 'T.id' AND T.id = \"T.id\"";
        assertEquals(
                "TT.id = 'T.id' AND `RT`.id = \"T.id\"",
                action.rewriteMergeCondition(mergeCondition));
    }

    private void prepareTargetTable() throws Exception {
        sEnv.executeSql(
                buildDdl(
                        "T",
                        Arrays.asList("id INT", "name STRING", "`value` DOUBLE", "dt STRING"),
                        Collections.emptyList(),
                        Collections.singletonList("dt"),
                        new HashMap<String, String>() {
                            {
                                put(ROW_TRACKING_ENABLED.key(), "true");
                                put(DATA_EVOLUTION_ENABLED.key(), "true");
                            }
                        }));
        insertInto(
                "T",
                "(1, 'name1', 0.1, '01-22')",
                "(2, 'name2', 0.2, '01-22')",
                "(3, 'name3', 0.3, '01-22')",
                "(4, 'name4', 0.4, '01-22')",
                "(5, 'name5', 0.5, '01-22')",
                "(6, 'name6', 0.6, '01-22')",
                "(7, 'name7', 0.7, '01-22')",
                "(8, 'name8', 0.8, '01-22')",
                "(9, 'name9', 0.9, '01-22')",
                "(10, 'name10', 1.0, '01-22')");

        insertInto(
                "T",
                "(11, 'name11', 1.1, '01-22')",
                "(12, 'name12', 1.2, '01-22')",
                "(13, 'name13', 1.3, '01-22')",
                "(14, 'name14', 1.4, '01-22')",
                "(15, 'name15', 1.5, '01-22')",
                "(16, 'name16', 1.6, '01-22')",
                "(17, 'name17', 1.7, '01-22')",
                "(18, 'name18', 1.8, '01-22')",
                "(19, 'name19', 1.9, '01-22')",
                "(20, 'name20', 2.0, '01-22')");
    }

    private void prepareSourceTable() throws Exception {
        sEnv.executeSql(
                buildDdl(
                        "S",
                        Arrays.asList("id INT", "name STRING", "`value` DOUBLE"),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        new HashMap<String, String>() {
                            {
                                put(ROW_TRACKING_ENABLED.key(), "true");
                                put(DATA_EVOLUTION_ENABLED.key(), "true");
                            }
                        }));
        insertInto(
                "S",
                "(1, 'new_name1', 100.1)",
                "(7, 'new_name7', CAST(NULL AS DOUBLE))",
                "(11, 'new_name11', 101.1)",
                "(15, CAST(NULL AS STRING), 101.1)",
                "(18, 'new_name18', 101.8)",
                "(21, 'new_name21', 102.1)");
    }
}
