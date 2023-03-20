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

package org.apache.paimon.flink;

import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Large data ITCase. */
public class LargeDataITCase extends CatalogITCaseBase {

    @Test
    public void testSpillableWriteBuffer() throws Exception {
        // set Parallelism to 1, or multiple source tasks resulting in uncertain order.
        setParallelism(1);
        sql(
                "CREATE TABLE T1 (a INT PRIMARY KEY NOT ENFORCED, b INT) WITH ("
                        + "'write-buffer-size'='256 kb', 'write-buffer-spillable'='true')");
        sql("CREATE TABLE T2 (a INT PRIMARY KEY NOT ENFORCED, b INT)");
        sql(
                "CREATE TEMPORARY TABLE datagen (a INT, b INT) WITH ("
                        + "'connector'='datagen', "
                        + "'fields.a.min'='0', "
                        + "'fields.a.max'='50000', "
                        + "'fields.b.min'='0', "
                        + "'fields.b.max'='10', "
                        + "'number-of-rows'='20000')");

        tEnv.createStatementSet()
                .addInsertSql("INSERT INTO T1 SELECT * FROM datagen")
                .addInsertSql("INSERT INTO T2 SELECT * FROM datagen")
                .execute()
                .await();

        List<Row> result1 = sql("SELECT * FROM T1");
        List<Row> result2 = sql("SELECT * FROM T2");
        assertThat(result1).containsExactlyElementsOf(result2);
    }

    @Test
    public void testPartitionedSpillableWriteBuffer() throws Exception {
        sql(
                "CREATE TABLE T1 (a INT, b INT, c INT, PRIMARY KEY (a, b) NOT ENFORCED) PARTITIONED BY (b) WITH ("
                        + "'write-buffer-size'='1 mb', 'write-buffer-spillable'='true')");
        sql(
                "CREATE TABLE T2 (a INT, b INT, c INT, PRIMARY KEY (a, b) NOT ENFORCED) PARTITIONED BY (b)");
        sql(
                "CREATE TEMPORARY TABLE datagen (a INT, b INT, c INT) WITH ("
                        + "'connector'='datagen', 'number-of-rows'='10000', 'fields.b.min'='1', 'fields.b.max'='10')");

        tEnv.createStatementSet()
                .addInsertSql("INSERT INTO T1 SELECT * FROM datagen")
                .addInsertSql("INSERT INTO T2 SELECT * FROM datagen")
                .execute()
                .await();

        List<Row> result1 = sql("SELECT * FROM T1");
        List<Row> result2 = sql("SELECT * FROM T2");
        assertThat(result1).containsExactlyInAnyOrderElementsOf(result2);
    }
}
