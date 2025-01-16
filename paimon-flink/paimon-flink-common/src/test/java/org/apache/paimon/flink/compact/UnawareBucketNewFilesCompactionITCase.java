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

package org.apache.paimon.flink.compact;

import org.apache.paimon.flink.util.AbstractTestBase;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;

import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.config.TableConfigOptions;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * IT cases for {@link UnawareBucketNewFilesCompactionCoordinatorOperator} and {@link
 * UnawareBucketNewFilesCompactionWorkerOperator}.
 */
public class UnawareBucketNewFilesCompactionITCase extends AbstractTestBase {

    @Test
    public void testCompactNewFiles() throws Exception {
        String warehouse = getTempDirPath();
        TableEnvironment tEnv =
                tableEnvironmentBuilder()
                        .batchMode()
                        .parallelism(2)
                        .setConf(TableConfigOptions.TABLE_DML_SYNC, true)
                        .build();
        tEnv.executeSql(
                "CREATE CATALOG mycat WITH (\n"
                        + "  'type' = 'paimon',\n"
                        + "  'warehouse' = '"
                        + warehouse
                        + "'\n"
                        + ")");
        tEnv.executeSql("USE CATALOG mycat");
        tEnv.executeSql(
                "CREATE TABLE T (\n"
                        + "  pt INT,\n"
                        + "  a INT,\n"
                        + "  b STRING\n"
                        + ") PARTITIONED BY (pt) WITH (\n"
                        + "  'write-only' = 'true',\n"
                        + "  'compaction.min.file-num' = '3',\n"
                        + "  'compaction.max.file-num' = '3',\n"
                        + "  'precommit-compact' = 'true',\n"
                        + "  'sink.parallelism' = '2'\n"
                        + ")");

        List<String> values = new ArrayList<>();
        for (int pt = 0; pt < 2; pt++) {
            for (int a = 0; a < 50; a++) {
                values.add(String.format("(%d, %d, '%d')", pt, a, a * 1000));
            }
        }

        Supplier<Map<String, Integer>> getActual =
                () -> {
                    Map<String, Integer> result = new HashMap<>();
                    try (CloseableIterator<Row> it = tEnv.executeSql("SELECT * FROM T").collect()) {
                        while (it.hasNext()) {
                            Row row = it.next();
                            assertThat(row.getArity()).isEqualTo(3);
                            result.compute(
                                    String.format(
                                            "(%s, %s, '%s')",
                                            row.getField(0), row.getField(1), row.getField(2)),
                                    (k, v) -> v == null ? 1 : v + 1);
                        }
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                    return result;
                };

        LocalFileIO fileIO = LocalFileIO.create();
        for (int r = 1; r <= 3; r++) {
            tEnv.executeSql("INSERT INTO T VALUES " + String.join(", ", values)).await();
            assertThat(fileIO.listStatus(new Path(warehouse, "default.db/T/pt=0/bucket-0")))
                    .hasSize(r);
            assertThat(fileIO.listStatus(new Path(warehouse, "default.db/T/pt=1/bucket-0")))
                    .hasSize(r);
            Map<String, Integer> actual = getActual.get();
            assertThat(actual.keySet()).hasSameElementsAs(values);
            final int e = r;
            assertThat(actual.values()).allMatch(i -> i == e);
        }

        tEnv.executeSql("CALL sys.compact('default.T')").await();
        assertThat(fileIO.listStatus(new Path(warehouse, "default.db/T/pt=0/bucket-0"))).hasSize(4);
        assertThat(fileIO.listStatus(new Path(warehouse, "default.db/T/pt=1/bucket-0"))).hasSize(4);
        Map<String, Integer> actual = getActual.get();
        assertThat(actual.keySet()).hasSameElementsAs(values);
        assertThat(actual.values()).allMatch(i -> i == 3);

        tEnv.executeSql("CALL sys.expire_snapshots(`table` => 'default.T', retain_max => 1)")
                .await();
        assertThat(fileIO.listStatus(new Path(warehouse, "default.db/T/pt=0/bucket-0"))).hasSize(1);
        assertThat(fileIO.listStatus(new Path(warehouse, "default.db/T/pt=1/bucket-0"))).hasSize(1);
        actual = getActual.get();
        assertThat(actual.keySet()).hasSameElementsAs(values);
        assertThat(actual.values()).allMatch(i -> i == 3);
    }
}
