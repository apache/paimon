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

import org.apache.flink.types.Row;

import org.junit.Test;

import java.util.List;
import java.util.stream.Collectors;

import static org.apache.flink.table.store.file.catalog.Catalog.SYSTEM_TABLE_SPLITTER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** ITCase for catalog tables. */
public class CatalogTableITCase extends CatalogITCaseBase {

    @Test
    public void testNotExistMetadataTable() {
        assertThatThrownBy(() -> sql("SELECT snapshot_id, schema_id, commit_kind FROM T$snapshots"))
                .hasMessageContaining("Object 'T$snapshots' not found");
    }

    @Test
    public void testSnapshotsTable() throws Exception {
        sql("CREATE TABLE T (a INT, b INT)");
        sql("INSERT INTO T VALUES (1, 2)");
        sql("INSERT INTO T VALUES (3, 4)");

        List<Row> result = sql("SELECT snapshot_id, schema_id, commit_kind FROM T$snapshots");
        assertThat(result)
                .containsExactlyInAnyOrder(Row.of(1L, 0L, "APPEND"), Row.of(2L, 0L, "APPEND"));
    }

    @Test
    public void testOptionsTable() throws Exception {
        sql("CREATE TABLE T (a INT, b INT)");
        sql("ALTER TABLE T SET ('snapshot.time-retained' = '5 h')");

        List<Row> result = sql("SELECT * FROM T$options");
        assertThat(result).containsExactly(Row.of("snapshot.time-retained", "5 h"));
    }

    @Test
    public void testCreateMetaTable() {
        assertThatThrownBy(() -> sql("CREATE TABLE T$snapshots (a INT, b INT)"))
                .hasRootCauseMessage(
                        String.format(
                                "Table name[%s] cannot contain '%s' separator",
                                "T$snapshots", SYSTEM_TABLE_SPLITTER));
        assertThatThrownBy(() -> sql("CREATE TABLE T$aa$bb (a INT, b INT)"))
                .hasRootCauseMessage(
                        String.format(
                                "Table name[%s] cannot contain '%s' separator",
                                "T$aa$bb", SYSTEM_TABLE_SPLITTER));
    }

    @Test
    public void testSchemasTable() throws Exception {
        sql(
                "CREATE TABLE T(a INT, b INT, c STRING, PRIMARY KEY (a) NOT ENFORCED) with ('a.aa.aaa'='val1', 'b.bb.bbb'='val2')");
        sql("ALTER TABLE T SET ('snapshot.time-retained' = '5 h')");

        assertThat(sql("SHOW CREATE TABLE T$schemas").toString())
                .isEqualTo(
                        "[+I[CREATE TABLE `TABLE_STORE`.`default`.`T$schemas` (\n"
                                + "  `schema_id` BIGINT NOT NULL,\n"
                                + "  `fields` VARCHAR(2147483647) NOT NULL,\n"
                                + "  `partition_keys` VARCHAR(2147483647) NOT NULL,\n"
                                + "  `primary_keys` VARCHAR(2147483647) NOT NULL,\n"
                                + "  `options` VARCHAR(2147483647) NOT NULL,\n"
                                + "  `comment` VARCHAR(2147483647)\n"
                                + ") ]]");

        List<Row> result = sql("SELECT * FROM T$schemas order by schema_id");

        assertThat(result.toString())
                .isEqualTo(
                        "[+I[0, [{\"id\":0,\"name\":\"a\",\"type\":\"INT NOT NULL\"},"
                                + "{\"id\":1,\"name\":\"b\",\"type\":\"INT\"},"
                                + "{\"id\":2,\"name\":\"c\",\"type\":\"VARCHAR(2147483647)\"}], [], [\"a\"], "
                                + "{\"a.aa.aaa\":\"val1\",\"b.bb.bbb\":\"val2\"}, ], "
                                + "+I[1, [{\"id\":0,\"name\":\"a\",\"type\":\"INT NOT NULL\"},"
                                + "{\"id\":1,\"name\":\"b\",\"type\":\"INT\"},"
                                + "{\"id\":2,\"name\":\"c\",\"type\":\"VARCHAR(2147483647)\"}], [], [\"a\"], "
                                + "{\"a.aa.aaa\":\"val1\",\"snapshot.time-retained\":\"5 h\",\"b.bb.bbb\":\"val2\"}, ]]");
    }

    @Test
    public void testSnapshotsSchemasTable() throws Exception {
        sql("CREATE TABLE T (a INT, b INT)");
        sql("INSERT INTO T VALUES (1, 2)");
        sql("INSERT INTO T VALUES (3, 4)");
        sql("ALTER TABLE T SET ('snapshot.time-retained' = '5 h')");
        sql("INSERT INTO T VALUES (5, 6)");
        sql("INSERT INTO T VALUES (7, 8)");

        List<Row> result =
                sql(
                        "SELECT s.snapshot_id, s.schema_id, t.fields FROM "
                                + "T$snapshots s JOIN T$schemas t ON s.schema_id=t.schema_id");
        assertThat(result.stream().map(Row::toString).collect(Collectors.toList()))
                .containsExactlyInAnyOrder(
                        "+I[1, 0, [{\"id\":0,\"name\":\"a\",\"type\":\"INT\"},{\"id\":1,\"name\":\"b\",\"type\":\"INT\"}]]",
                        "+I[2, 0, [{\"id\":0,\"name\":\"a\",\"type\":\"INT\"},{\"id\":1,\"name\":\"b\",\"type\":\"INT\"}]]",
                        "+I[3, 1, [{\"id\":0,\"name\":\"a\",\"type\":\"INT\"},{\"id\":1,\"name\":\"b\",\"type\":\"INT\"}]]",
                        "+I[4, 1, [{\"id\":0,\"name\":\"a\",\"type\":\"INT\"},{\"id\":1,\"name\":\"b\",\"type\":\"INT\"}]]");
    }
}
