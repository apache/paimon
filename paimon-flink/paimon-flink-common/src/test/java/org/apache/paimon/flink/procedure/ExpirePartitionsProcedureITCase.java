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

package org.apache.paimon.flink.procedure;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.flink.CatalogITCaseBase;
import org.apache.paimon.table.FileStoreTable;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/** IT Case for {@link ExpirePartitionsProcedure}. */
public class ExpirePartitionsProcedureITCase extends CatalogITCaseBase {

    @Test
    public void testExpirePartitionsProcedure() throws Exception {
        sql(
                "CREATE TABLE T ("
                        + " k STRING,"
                        + " dt STRING,"
                        + " PRIMARY KEY (k, dt) NOT ENFORCED"
                        + ") PARTITIONED BY (dt) WITH ("
                        + " 'bucket' = '1'"
                        + ")");
        FileStoreTable table = paimonTable("T");

        sql("INSERT INTO T VALUES ('1', '2024-06-01')");
        // This partition never expires.
        sql("INSERT INTO T VALUES ('Never-expire', '9999-09-09')");
        Function<InternalRow, String> consumerReadResult =
                (InternalRow row) -> row.getString(0) + ":" + row.getString(1);

        assertThat(read(table, consumerReadResult))
                .containsExactlyInAnyOrder("1:2024-06-01", "Never-expire:9999-09-09");

        assertThat(
                        callExpirePartitions(
                                "CALL sys.expire_partitions("
                                        + "`table` => 'default.T'"
                                        + ", expiration_time => '1 d'"
                                        + ", timestamp_formatter => 'yyyy-MM-dd')"))
                .containsExactlyInAnyOrder("dt=2024-06-01");

        assertThat(read(table, consumerReadResult))
                .containsExactlyInAnyOrder("Never-expire:9999-09-09");
    }

    @Test
    public void testShowExpirePartitionsProcedureResults() throws Exception {
        sql(
                "CREATE TABLE T ("
                        + " k STRING,"
                        + " dt STRING,"
                        + " hm STRING,"
                        + " PRIMARY KEY (k, dt, hm) NOT ENFORCED"
                        + ") PARTITIONED BY (dt, hm) WITH ("
                        + " 'bucket' = '1'"
                        + ")");
        FileStoreTable table = paimonTable("T");
        // Test there are no expired partitions.
        assertThat(
                        callExpirePartitions(
                                "CALL sys.expire_partitions("
                                        + "`table` => 'default.T'"
                                        + ", expiration_time => '1 d'"
                                        + ", timestamp_formatter => 'yyyy-MM-dd')"))
                .containsExactlyInAnyOrder("No expired partitions.");

        sql("INSERT INTO T VALUES ('1', '2024-06-01', '01:00')");
        sql("INSERT INTO T VALUES ('2', '2024-06-02', '02:00')");
        // This partition never expires.
        sql("INSERT INTO T VALUES ('Never-expire', '9999-09-09', '99:99')");

        Function<InternalRow, String> consumerReadResult =
                (InternalRow row) ->
                        row.getString(0) + ":" + row.getString(1) + ":" + row.getString(2);
        assertThat(read(table, consumerReadResult))
                .containsExactlyInAnyOrder(
                        "1:2024-06-01:01:00",
                        "2:2024-06-02:02:00",
                        "Never-expire:9999-09-09:99:99");

        // Show a list of expired partitions.
        assertThat(
                        callExpirePartitions(
                                "CALL sys.expire_partitions("
                                        + "`table` => 'default.T'"
                                        + ", expiration_time => '1 d'"
                                        + ", timestamp_formatter => 'yyyy-MM-dd')"))
                .containsExactlyInAnyOrder("dt=2024-06-01, hm=01:00", "dt=2024-06-02, hm=02:00");

        assertThat(read(table, consumerReadResult))
                .containsExactlyInAnyOrder("Never-expire:9999-09-09:99:99");
    }

    @Test
    public void testPartitionExpireValuesTimeStrategy() throws Exception {
        sql(
                "CREATE TABLE T ("
                        + " k STRING,"
                        + " dt STRING,"
                        + " PRIMARY KEY (k, dt) NOT ENFORCED"
                        + ") PARTITIONED BY (dt) WITH ("
                        + " 'bucket' = '1'"
                        + ")");
        FileStoreTable table = paimonTable("T");
        Function<InternalRow, String> consumerReadResult =
                (InternalRow row) -> row.getString(0) + ":" + row.getString(1);

        sql("INSERT INTO T VALUES ('HXH', '2024-06-01')");
        // This partition never expires.
        sql("INSERT INTO T VALUES ('Never-expire', '9999-09-09')");

        assertThat(read(table, consumerReadResult))
                .containsExactlyInAnyOrder("HXH:2024-06-01", "Never-expire:9999-09-09");

        assertThat(
                        callExpirePartitions(
                                "CALL sys.expire_partitions(`table` => 'default.T',"
                                        + " expiration_time => '1 d',"
                                        + " timestamp_formatter => 'yyyy-MM-dd',"
                                        + " expire_strategy => 'values-time')"))
                .containsExactlyInAnyOrder("dt=2024-06-01");

        assertThat(read(table, consumerReadResult))
                .containsExactlyInAnyOrder("Never-expire:9999-09-09");
    }

    @Test
    public void testPartitionExpireUpdateTimeStrategy() throws Exception {
        sql(
                "CREATE TABLE T ("
                        + " k STRING,"
                        + " dt STRING,"
                        + " hm STRING,"
                        + " PRIMARY KEY (k, dt, hm) NOT ENFORCED"
                        + ") PARTITIONED BY (dt, hm) WITH ("
                        + " 'bucket' = '1'"
                        + ")");
        FileStoreTable table = paimonTable("T");
        Function<InternalRow, String> consumerReadResult =
                (InternalRow row) ->
                        row.getString(0) + ":" + row.getString(1) + ":" + row.getString(2);

        // This partition will expire.
        sql("INSERT INTO T VALUES ('Max-Date', '9999-09-09', '99:99')");
        // Waiting for partition 'pt=9999-09-09, hm=99:99' to expire.
        Thread.sleep(2500);
        sql("INSERT INTO T VALUES ('HXH', '2024-06-01', '01:00')");
        sql("INSERT INTO T VALUES ('HXH', '2024-06-01', '02:00')");

        // Partitions that are updated within 2 second would be retained.
        assertThat(
                        callExpirePartitions(
                                "CALL sys.expire_partitions("
                                        + "`table` => 'default.T'"
                                        + ",expiration_time => '2 s'"
                                        + ",expire_strategy => 'update-time')"))
                .containsExactlyInAnyOrder("dt=9999-09-09, hm=99:99");

        assertThat(read(table, consumerReadResult))
                .containsExactlyInAnyOrder("HXH:2024-06-01:01:00", "HXH:2024-06-01:02:00");

        // Waiting for all partitions to expire.
        Thread.sleep(1500);
        // All partition will expire.
        assertThat(
                        callExpirePartitions(
                                "CALL sys.expire_partitions("
                                        + "`table` => 'default.T',"
                                        + " expiration_time => '1 s',"
                                        + " expire_strategy => 'update-time')"))
                .containsExactlyInAnyOrder("dt=2024-06-01, hm=01:00", "dt=2024-06-01, hm=02:00");

        assertThat(read(table, consumerReadResult)).isEmpty();
    }

    @Test
    public void testPartitionExpireUpdateTimeStrategyInOnePartition() throws Exception {
        sql(
                "CREATE TABLE T ("
                        + " k STRING,"
                        + " dt STRING,"
                        + " hm STRING,"
                        + " PRIMARY KEY (k, dt, hm) NOT ENFORCED"
                        + ") PARTITIONED BY (dt, hm) WITH ("
                        + " 'bucket' = '1'"
                        + ")");
        FileStoreTable table = paimonTable("T");
        Function<InternalRow, String> consumerReadResult =
                (InternalRow row) ->
                        row.getString(0) + ":" + row.getString(1) + ":" + row.getString(2);

        // This partition will not expire.
        sql("INSERT INTO T VALUES ('HXH', '2024-06-01', '01:00')");
        // Waiting for partitions 'pt=2024-06-01, hm=01:00' to expire.
        Thread.sleep(2500);
        // Updating the same partition data will update partition last update time, then this
        // partition will not expire.
        sql("INSERT INTO T VALUES ('HXH', '2024-06-01', '01:00')");

        // Partitions that are updated within 2 second would be retained,in this case no expired
        // partitions.
        assertThat(
                        callExpirePartitions(
                                "CALL sys.expire_partitions("
                                        + "`table` => 'default.T',"
                                        + " expiration_time => '2 s',"
                                        + " expire_strategy => 'update-time')"))
                .containsExactlyInAnyOrder("No expired partitions.");

        assertThat(read(table, consumerReadResult))
                .containsExactlyInAnyOrder("HXH:2024-06-01:01:00");

        // Waiting for all partitions to expire.
        Thread.sleep(1500);

        // The partition 'dt=2024-06-01, hm=01:00' will expire.
        assertThat(
                        callExpirePartitions(
                                "CALL sys.expire_partitions("
                                        + "`table` => 'default.T',"
                                        + " expiration_time => '1 s',"
                                        + " expire_strategy => 'update-time')"))
                .containsExactlyInAnyOrder("dt=2024-06-01, hm=01:00");

        assertThat(read(table, consumerReadResult)).isEmpty();
    }

    @Test
    public void testPartitionExpireWithNonDateFormatPartition() throws Exception {
        sql(
                "CREATE TABLE T ("
                        + " k STRING,"
                        + " dt STRING,"
                        + " hm STRING,"
                        + " PRIMARY KEY (k, dt, hm) NOT ENFORCED"
                        + ") PARTITIONED BY (dt, hm) WITH ("
                        + " 'bucket' = '1'"
                        + ")");
        FileStoreTable table = paimonTable("T");
        Function<InternalRow, String> consumerReadResult =
                (InternalRow row) ->
                        row.getString(0) + ":" + row.getString(1) + ":" + row.getString(2);
        // This partition will expire.
        sql("INSERT INTO T VALUES ('HXH', 'pt-1', 'hm-1')");
        Thread.sleep(2500);
        sql("INSERT INTO T VALUES ('HXH', 'pt-2', 'hm-2')");
        sql("INSERT INTO T VALUES ('HXH', 'pt-3', 'hm-3')");

        // Only update-time strategy support non date format partition to expire.
        assertThat(
                        callExpirePartitions(
                                "CALL sys.expire_partitions("
                                        + "`table` => 'default.T',"
                                        + " expiration_time => '2 s',"
                                        + " expire_strategy => 'update-time')"))
                .containsExactlyInAnyOrder("dt=pt-1, hm=hm-1");

        assertThat(read(table, consumerReadResult))
                .containsExactlyInAnyOrder("HXH:pt-2:hm-2", "HXH:pt-3:hm-3");

        // Waiting for all partitions to expire.
        Thread.sleep(1500);

        assertThat(
                        callExpirePartitions(
                                "CALL sys.expire_partitions("
                                        + "`table` => 'default.T',"
                                        + " expiration_time => '1 s',"
                                        + " expire_strategy => 'update-time')"))
                .containsExactlyInAnyOrder("dt=pt-2, hm=hm-2", "dt=pt-3, hm=hm-3");

        assertThat(read(table, consumerReadResult)).isEmpty();
    }

    /** Return a list of expired partitions. */
    public List<String> callExpirePartitions(String callSql) {
        return sql(callSql).stream()
                .map(row -> row.getField(0).toString())
                .collect(Collectors.toList());
    }

    private List<String> read(
            FileStoreTable table, Function<InternalRow, String> consumerReadResult)
            throws IOException {
        List<String> ret = new ArrayList<>();
        table.newRead()
                .createReader(table.newScan().plan().splits())
                .forEachRemaining(row -> ret.add(consumerReadResult.apply(row)));
        return ret;
    }
}
