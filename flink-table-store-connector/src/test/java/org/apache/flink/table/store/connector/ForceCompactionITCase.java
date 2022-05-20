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

package org.apache.flink.table.store.connector;

import org.junit.Test;

import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** ITCase for enabling commit.force-compact. */
public class ForceCompactionITCase extends FileStoreTableITCase {

    @Override
    protected List<String> ddl() {
        return Collections.singletonList(
                "CREATE TABLE IF NOT EXISTS T (\n"
                        + "  f0 INT\n, "
                        + "  f1 STRING\n, "
                        + "  f2 STRING\n"
                        + ") PARTITIONED BY (f1)");
    }

    @Test
    public void testDynamicPartition() {
        batchSql("ALTER TABLE T SET ('num-levels' = '3')");
        batchSql("ALTER TABLE T SET ('commit.force-compact' = 'true')");
        batchSql(
                "INSERT INTO T VALUES(1, 'Winter', 'Winter is Coming'),"
                        + "(2, 'Winter', 'The First Snowflake'), "
                        + "(2, 'Spring', 'The First Rose in Spring'), "
                        + "(7, 'Summer', 'Summertime Sadness')");
        batchSql("INSERT INTO T VALUES(12, 'Winter', 'Last Christmas')");
        batchSql("INSERT INTO T VALUES(11, 'Winter', 'Winter is Coming')");
        batchSql("INSERT INTO T VALUES(10, 'Autumn', 'Refrain')");
        batchSql(
                "INSERT INTO T VALUES(6, 'Summer', 'Watermelon Sugar'), "
                        + "(4, 'Spring', 'Spring Water')");
        batchSql(
                "INSERT INTO T VALUES(66, 'Summer', 'Summer Vibe'),"
                        + " (9, 'Autumn', 'Wake Me Up When September Ends')");
        batchSql(
                "INSERT INTO T VALUES(666, 'Summer', 'Summer Vibe'),"
                        + " (9, 'Autumn', 'Wake Me Up When September Ends')");
        batchSql(
                "INSERT INTO T VALUES(6666, 'Summer', 'Summer Vibe'),"
                        + " (9, 'Autumn', 'Wake Me Up When September Ends')");
        batchSql(
                "INSERT INTO T VALUES(66666, 'Summer', 'Summer Vibe'),"
                        + " (9, 'Autumn', 'Wake Me Up When September Ends')");
        batchSql(
                "INSERT INTO T VALUES(666666, 'Summer', 'Summer Vibe'),"
                        + " (9, 'Autumn', 'Wake Me Up When September Ends')");
        batchSql(
                "INSERT INTO T VALUES(6666666, 'Summer', 'Summer Vibe'),"
                        + " (9, 'Autumn', 'Wake Me Up When September Ends')");

        assertThat(batchSql("SELECT * FROM T")).hasSize(21);
    }
}
