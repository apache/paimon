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

import org.apache.paimon.flink.util.AbstractTestBase;

import org.apache.paimon.shade.guava30.com.google.common.collect.ImmutableList;

import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** ITCase for mapping table api. */
public class MappingTableITCase extends AbstractTestBase {

    private TableEnvironment tEnv;
    private String path;

    @BeforeEach
    public void before() throws IOException {
        tEnv = tableEnvironmentBuilder().batchMode().build();
        path = getTempDirPath();
    }

    @Test
    public void testCreateEmptyMappingTable() {
        tEnv.executeSql(
                String.format(
                        "CREATE TABLE T (i INT, j INT) WITH ("
                                + "'connector'='paimon', 'path'='%s')",
                        path));
        assertThatThrownBy(() -> tEnv.executeSql("INSERT INTO T VALUES (1, 2), (3, 4)").await())
                .isInstanceOf(ValidationException.class)
                .hasRootCauseMessage(
                        "Schema file not found in location %s. Please create table first.", path);
    }

    @Test
    public void testCreateMappingTable() throws ExecutionException, InterruptedException {
        tEnv.executeSql(
                String.format(
                        "CREATE TABLE T (i INT, j INT) WITH ("
                                + "'connector'='paimon', 'path'='%s', 'auto-create'='true')",
                        path));
        tEnv.executeSql("INSERT INTO T VALUES (1, 2), (3, 4)").await();

        tEnv.executeSql("DROP TABLE T");
        tEnv.executeSql(
                String.format(
                        "CREATE TABLE T (i INT, j INT) WITH ("
                                + "'connector'='paimon', 'path'='%s')",
                        path));

        List<Row> result = ImmutableList.copyOf(tEnv.executeSql("SELECT * FROM T").collect());
        assertThat(result).containsExactlyInAnyOrder(Row.of(1, 2), Row.of(3, 4));
    }

    @Test
    public void testCreateTemporaryTableRepeat() throws Exception {
        for (int i = 0; i < 5; i++) {
            tEnv.executeSql(
                    String.format(
                            "CREATE TABLE T (i INT, j INT) WITH ("
                                    + "'connector'='paimon', 'path'='%s', 'auto-create'='true')",
                            path));
            tEnv.executeSql("SELECT * FROM T").collect().close();
            tEnv.executeSql("DROP TABLE T");
        }
    }

    @Test
    public void testCreateTemporaryTableConflict() throws Exception {
        tEnv.executeSql(
                String.format(
                        "CREATE TABLE T (i INT, j INT) WITH ("
                                + "'connector'='paimon', 'path'='%s', 'auto-create'='true')",
                        path));
        tEnv.executeSql("SELECT * FROM T").collect().close();
        tEnv.executeSql("DROP TABLE T");

        tEnv.executeSql(
                String.format(
                        "CREATE TABLE T (i INT, j INT, k INT) WITH ("
                                + "'connector'='paimon', 'path'='%s', 'auto-create'='true')",
                        path));

        assertThatThrownBy(() -> tEnv.executeSql("SELECT * FROM T").collect().close())
                .isInstanceOf(ValidationException.class)
                .hasRootCauseMessage(
                        "Flink schema and store schema are not the same, store schema is ROW<`i` INT, `j` INT>, Flink schema is ROW<`i` INT, `j` INT, `k` INT> NOT NULL");
    }
}
