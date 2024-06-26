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
        // Never expire.
        sql("INSERT INTO T VALUES ('2', '9024-06-01')");
        Function<InternalRow, String> consumerReadResult =
                (InternalRow row) -> row.getString(0) + ":" + row.getString(1);
        assertThat(read(table, consumerReadResult))
                .containsExactlyInAnyOrder("1:2024-06-01", "2:9024-06-01");
        sql(
                "CALL sys.expire_partitions(`table` => 'default.T', expiration_time => '1 d', timestamp_formatter => 'yyyy-MM-dd')");
        assertThat(read(table, consumerReadResult)).containsExactlyInAnyOrder("2:9024-06-01");
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
        List<String> result =
                sql(
                                "CALL sys.expire_partitions(`table` => 'default.T', expiration_time => '1 d', timestamp_formatter => 'yyyy-MM-dd')")
                        .stream()
                        .map(row -> row.getField(0).toString())
                        .collect(Collectors.toList());
        assertThat(result).containsExactlyInAnyOrder("No expired partitions.");

        sql("INSERT INTO T VALUES ('1', '2024-06-01', '01:00')");
        sql("INSERT INTO T VALUES ('2', '2024-06-02', '02:00')");
        // Never expire.
        sql("INSERT INTO T VALUES ('3', '9024-06-01', '03:00')");

        Function<InternalRow, String> consumerReadResult =
                (InternalRow row) ->
                        row.getString(0) + ":" + row.getString(1) + ":" + row.getString(2);
        assertThat(read(table, consumerReadResult))
                .containsExactlyInAnyOrder(
                        "1:2024-06-01:01:00", "2:2024-06-02:02:00", "3:9024-06-01:03:00");

        result =
                sql(
                                "CALL sys.expire_partitions(`table` => 'default.T', expiration_time => '1 d', timestamp_formatter => 'yyyy-MM-dd')")
                        .stream()
                        .map(row -> row.getField(0).toString())
                        .collect(Collectors.toList());
        // Show a list of expired partitions.
        assertThat(result)
                .containsExactlyInAnyOrder("dt=2024-06-01, hm=01:00", "dt=2024-06-02, hm=02:00");

        assertThat(read(table, consumerReadResult)).containsExactlyInAnyOrder("3:9024-06-01:03:00");
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
