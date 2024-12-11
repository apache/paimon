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

import org.apache.paimon.utils.BlockingIterator;

import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** ITCase for system table. */
public class SystemTableITCase extends CatalogTableITCase {

    @Test
    public void testBinlogTableStreamRead() throws Exception {
        sql(
                "CREATE TABLE T (a INT, b INT, primary key (a) NOT ENFORCED) with ('changelog-producer' = 'lookup', "
                        + "'bucket' = '2')");
        BlockingIterator<Row, Row> iterator =
                streamSqlBlockIter("SELECT * FROM T$binlog /*+ OPTIONS('scan.mode' = 'latest') */");
        sql("INSERT INTO T VALUES (1, 2)");
        sql("INSERT INTO T VALUES (1, 3)");
        sql("INSERT INTO T VALUES (2, 2)");
        List<Row> rows = iterator.collect(3);
        assertThat(rows)
                .containsExactly(
                        Row.of("+I", new Integer[] {1}, new Integer[] {2}),
                        Row.of("+U", new Integer[] {1, 1}, new Integer[] {2, 3}),
                        Row.of("+I", new Integer[] {2}, new Integer[] {2}));
        iterator.close();
    }

    @Test
    public void testBinlogTableBatchRead() {
        sql(
                "CREATE TABLE T (a INT, b INT, primary key (a) NOT ENFORCED) with ('changelog-producer' = 'lookup', "
                        + "'bucket' = '2')");
        sql("INSERT INTO T VALUES (1, 2)");
        sql("INSERT INTO T VALUES (1, 3)");
        sql("INSERT INTO T VALUES (2, 2)");
        List<Row> rows = sql("SELECT * FROM T$binlog /*+ OPTIONS('scan.mode' = 'latest') */");
        assertThat(rows)
                .containsExactly(
                        Row.of("+I", new Integer[] {1}, new Integer[] {3}),
                        Row.of("+I", new Integer[] {2}, new Integer[] {2}));
    }

    @Test
    public void testSummaryTableAppendOnlyTable() {
        sql(
                "CREATE TABLE T ("
                        + "a INT,"
                        + " b INT"
                        + ") comment 'this is comment'"
                        + " with ("
                        + "'bucket' = '2',"
                        + "'bucket-key' = 'a')");
        // no data, no partition.
        tEnv.executeSql("SELECT * FROM T$summary").print();

        // 1 file, 4 record.
        sql("INSERT INTO T VALUES (1, 2), (1, 2), (1, 2), (1, 2)");
        tEnv.executeSql("SELECT * FROM T$summary").print();

        // append table with unaware_bucket.
        sql(
                "CREATE TABLE T_unaware_bucket ("
                        + "a INT,"
                        + " b INT"
                        + ") with ("
                        + "'bucket' = '-1'"
                        + ")");
        sql("INSERT INTO T_unaware_bucket VALUES (1, 2)");
        tEnv.executeSql("SELECT * FROM T_unaware_bucket$summary").print();


    }

    @Test
    public void testSummaryTableAppendOnlyTableWithPartition() {
        // append table with partitioned.
        sql(
                "CREATE TABLE T_with_partition ("
                        + "a INT,"
                        + " b INT,"
                        + " dt string,"
                        + " hm string"
                        + ") PARTITIONED BY (dt, hm) with ("
                        + "'bucket' = '2',"
                        + " 'bucket-key' = 'a')");
        sql(
                "INSERT INTO T_with_partition VALUES (1, 2, '20240101', '11'),(1, 2, '20240101', '11')");
        tEnv.executeSql("SELECT * FROM T_with_partition$summary").print();

        // append table with partitioned and unaware bucket.
        sql(
                "CREATE TABLE T_with_partition_unaware_bucket ("
                        + "a INT,"
                        + " b INT,"
                        + " dt string,"
                        + " hm string"
                        + ") PARTITIONED BY (dt, hm) with ("
                        + "'bucket' = '-1')");
        sql(
                "INSERT INTO T_with_partition_unaware_bucket VALUES (1, 2, '20240101', '11'),(1, 2, '20240101', '11')");
        tEnv.executeSql("SELECT * FROM T_with_partition_unaware_bucket$summary").print();
    }

    @Test
    public void testSummaryTablePrimaryKeyTable() {
        sql(
                "CREATE TABLE T (a INT,"
                        + " b INT,"
                        + " primary key (a) NOT ENFORCED"
                        + ") with ("
                        + "'bucket' = '2')");
        //        sql("INSERT INTO T VALUES (1, 2)");
        tEnv.executeSql("SELECT * FROM T$summary").print();
        tEnv.executeSql("desc T$summary").print();

        sql(
                "CREATE TABLE T_unaware_bucket (a INT,"
                        + " b INT,"
                        + " primary key (a) NOT ENFORCED"
                        + ") with ("
                        + "'bucket' = '-1')");
        sql("INSERT INTO T_unaware_bucket VALUES (1, 2)");
        tEnv.executeSql("SELECT * FROM T_unaware_bucket$summary").print();

        sql(
                "CREATE TABLE T_with_partition ("
                        + "a INT,"
                        + " b INT,"
                        + " dt string,"
                        + " hm string,"
                        + " primary key (a, dt, hm) NOT ENFORCED"
                        + ") PARTITIONED BY (dt, hm) with ( "
                        + "'bucket' = '2')");

        sql(
                "INSERT INTO T_with_partition VALUES"
                        + " (1, 2, '20240101', '11')"
                        + ",(1, 2, '20240101', '12')");
        sql("INSERT INTO T_with_partition VALUES (1, 2, '20240101', '13')");
        sql("INSERT INTO T_with_partition VALUES (1, 2, '20240101', '13')");
        tEnv.executeSql("SELECT * FROM T_with_partition$summary").print();
    }

    @Test
    public void testSummaryTableWithCompact() {
        sql(
                "CREATE TABLE T ("
                        + "a INT,"
                        + " b INT,"
                        + " dt string,"
                        + " hm string,"
                        + " primary key (a, dt, hm) NOT ENFORCED"
                        + ") PARTITIONED BY (dt, hm) with ('changelog-producer' = 'lookup', "
                        + "'bucket' = '2')");
        sql("INSERT INTO T VALUES (1, 2, '20240101', '11')" + ",(1, 2, '20240101', '11')");
        sql(
                "INSERT INTO T /*+ OPTIONS("
                        + "'full-compaction.delta-commits' = '1'"
                        + ") */"
                        + " VALUES (1, 2, '20240101', '11')");
        tEnv.executeSql("SELECT * FROM T$summary").print();
    }
}
