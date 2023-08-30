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

import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** ITCase for batch file store. */
public class GlobalDynamicBucketTableITCase extends CatalogITCaseBase {

    @Override
    protected List<String> ddl() {
        return Collections.singletonList(
                "CREATE TABLE IF NOT EXISTS T ("
                        + "pt INT, "
                        + "pk INT, "
                        + "v INT, "
                        + "PRIMARY KEY (pk) NOT ENFORCED"
                        + ") PARTITIONED BY (pt) WITH ("
                        + " 'bucket'='-1', "
                        + " 'dynamic-bucket.target-row-num'='3' "
                        + ")");
    }

    @Test
    public void testWriteRead() {
        sql("INSERT INTO T VALUES (1, 1, 1), (1, 2, 2), (1, 3, 3), (1, 4, 4), (1, 5, 5)");
        assertThat(sql("SELECT * FROM T"))
                .containsExactlyInAnyOrder(
                        Row.of(1, 1, 1),
                        Row.of(1, 2, 2),
                        Row.of(1, 3, 3),
                        Row.of(1, 4, 4),
                        Row.of(1, 5, 5));
        sql("INSERT INTO T VALUES (1, 3, 33), (1, 1, 11)");
        assertThat(sql("SELECT * FROM T"))
                .containsExactlyInAnyOrder(
                        Row.of(1, 1, 11),
                        Row.of(1, 2, 2),
                        Row.of(1, 3, 33),
                        Row.of(1, 4, 4),
                        Row.of(1, 5, 5));

        assertThat(sql("SELECT DISTINCT bucket FROM T$files"))
                .containsExactlyInAnyOrder(Row.of(0), Row.of(1));

        // change partition
        sql("INSERT INTO T VALUES (2, 1, 2), (2, 2, 3)");
        assertThat(sql("SELECT * FROM T"))
                .containsExactlyInAnyOrder(
                        Row.of(2, 1, 2),
                        Row.of(2, 2, 3),
                        Row.of(1, 3, 33),
                        Row.of(1, 4, 4),
                        Row.of(1, 5, 5));
    }

    @Test
    public void testWriteWithAssignerParallelism() {
        sql(
                "INSERT INTO T /*+ OPTIONS('dynamic-bucket.assigner-parallelism'='3') */ "
                        + "VALUES (1, 1, 1), (1, 2, 2), (1, 3, 3), (1, 4, 4), (1, 5, 5)");
        assertThat(sql("SELECT DISTINCT bucket FROM T$files"))
                .containsExactlyInAnyOrder(Row.of(0), Row.of(1), Row.of(2));
    }

    @Test
    public void testLargeRecords() {
        sql(
                "create table large_t (pt int, k int, v int, primary key (k) not enforced) partitioned by (pt) with ("
                        + "'bucket'='-1', "
                        + "'dynamic-bucket.target-row-num'='10000')");
        sql(
                "create temporary table src (pt int, k int, v int) with ("
                        + "'connector'='datagen', "
                        + "'number-of-rows'='100000', "
                        + "'fields.k.min'='0', "
                        + "'fields.k.max'='100000', "
                        + "'fields.pt.min'='0', "
                        + "'fields.pt.max'='1')");
        sql("insert into large_t select * from src");
        sql("insert into large_t select * from src");
        assertThat(sql("select k, count(*) from large_t group by k having count(*) > 1")).isEmpty();
    }
}
