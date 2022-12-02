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

import static org.apache.flink.table.store.file.catalog.Catalog.METADATA_TABLE_SPLITTER;
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
                                "T$snapshots", METADATA_TABLE_SPLITTER));
        assertThatThrownBy(() -> sql("CREATE TABLE T$aa$bb (a INT, b INT)"))
                .hasRootCauseMessage(
                        String.format(
                                "Table name[%s] cannot contain '%s' separator",
                                "T$aa$bb", METADATA_TABLE_SPLITTER));
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
                                + "  `fields` ARRAY<ROW<`id` INT NOT NULL, `name` VARCHAR(2147483647) NOT NULL, `type` VARCHAR(2147483647) NOT NULL, `description` VARCHAR(2147483647)>>,\n"
                                + "  `partition_keys` ARRAY<VARCHAR(2147483647) NOT NULL>,\n"
                                + "  `primary_keys` ARRAY<VARCHAR(2147483647) NOT NULL>,\n"
                                + "  `options` MAP<VARCHAR(2147483647) NOT NULL, VARCHAR(2147483647) NOT NULL>,\n"
                                + "  `comment` VARCHAR(2147483647)\n"
                                + ") ]]");

        List<Row> result = sql("SELECT * FROM T$schemas order by schema_id");

        assertThat(result.toString())
                .isEqualTo(
                        "[+I[0, [+I[0, a, INT NOT NULL, null], +I[1, b, INT, null], +I[2, c, STRING, null]], [], [a], {a.aa.aaa=val1, b.bb.bbb=val2}, ], "
                                + "+I[1, [+I[0, a, INT NOT NULL, null], +I[1, b, INT, null], +I[2, c, STRING, null]], [], [a], {a.aa.aaa=val1, snapshot.time-retained=5 h, b.bb.bbb=val2}, ]]");
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
                        "SELECT s.snapshot_id, s.schema_id, t.fields FROM T$snapshots s JOIN T$schemas t ON s.schema_id=t.schema_id");
        assertThat(result)
                .containsExactlyInAnyOrder(
                        Row.of(
                                1L,
                                0L,
                                new Row[] {
                                        Row.of(0, "a", "INT", null), Row.of(1, "b", "INT", null)
                                }),
                        Row.of(
                                2L,
                                0L,
                                new Row[] {
                                        Row.of(0, "a", "INT", null), Row.of(1, "b", "INT", null)
                                }),
                        Row.of(
                                3L,
                                1L,
                                new Row[] {
                                        Row.of(0, "a", "INT", null), Row.of(1, "b", "INT", null)
                                }),
                        Row.of(
                                4L,
                                1L,
                                new Row[] {
                                        Row.of(0, "a", "INT", null), Row.of(1, "b", "INT", null)
                                }));
    }
}
