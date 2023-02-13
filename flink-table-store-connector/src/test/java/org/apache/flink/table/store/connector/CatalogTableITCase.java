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

import org.apache.flink.table.store.file.schema.SchemaChange;
import org.apache.flink.table.store.file.schema.SchemaManager;
import org.apache.flink.table.store.fs.Path;
import org.apache.flink.table.store.fs.local.LocalFileIO;
import org.apache.flink.table.store.types.IntType;
import org.apache.flink.types.Row;

import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;

import java.util.Arrays;
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
                                + "{\"id\":2,\"name\":\"c\",\"type\":\"STRING\"}], [], [\"a\"], "
                                + "{\"a.aa.aaa\":\"val1\",\"b.bb.bbb\":\"val2\"}, ], "
                                + "+I[1, [{\"id\":0,\"name\":\"a\",\"type\":\"INT NOT NULL\"},"
                                + "{\"id\":1,\"name\":\"b\",\"type\":\"INT\"},"
                                + "{\"id\":2,\"name\":\"c\",\"type\":\"STRING\"}], [], [\"a\"], "
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

    @Test
    public void testCreateTableLike() throws Exception {
        sql("CREATE TABLE T (a INT)");
        sql("CREATE TABLE T1 LIKE T");
        List<Row> result = sql("SELECT * FROM T1$schemas s");
        assertThat(result.toString())
                .isEqualTo("[+I[0, [{\"id\":0,\"name\":\"a\",\"type\":\"INT\"}], [], [], {}, ]]");
    }

    @Test
    public void testFilesTable() throws Exception {
        sql(
                "CREATE TABLE T_VALUE_COUNT (a INT, p INT, b BIGINT, c STRING) "
                        + "PARTITIONED BY (p) "
                        + "WITH ('write-mode'='change-log')"); // change log with value count table
        assertFilesTable("T_VALUE_COUNT");

        sql(
                "CREATE TABLE T_WITH_KEY (a INT, p INT, b BIGINT, c STRING, PRIMARY KEY (a, p) NOT ENFORCED) "
                        + "PARTITIONED BY (p) "
                        + "WITH ('write-mode'='change-log')"); // change log with key table
        assertFilesTable("T_WITH_KEY");

        sql(
                "CREATE TABLE T_APPEND_ONLY (a INT, p INT, b BIGINT, c STRING) "
                        + "PARTITIONED BY (p) "
                        + "WITH ('write-mode'='append-only')"); // append only table
        assertFilesTable("T_APPEND_ONLY");
    }

    private void assertFilesTable(String tableName) throws Exception {
        assertThat(sql(String.format("SELECT * FROM %s$files", tableName))).isEmpty();

        // TODO should use sql for schema evolution after flink supports it.
        SchemaManager schemaManager =
                new SchemaManager(
                        LocalFileIO.create(),
                        new Path(path, String.format("default.db/%s", tableName)));
        sql(String.format("INSERT INTO %s VALUES (3, 1, 4, 'S2'), (1, 1, 2, 'S1')", tableName));

        // The result fields are [a->INT, p -> INT, b->BIGINT, c->STRING, d->INT, e->INT, f->INT]
        // after
        // evolution
        schemaManager.commitChanges(
                Arrays.asList(
                        SchemaChange.addColumn("d", new IntType()),
                        SchemaChange.addColumn("e", new IntType()),
                        SchemaChange.addColumn("f", new IntType())));
        sql(
                String.format(
                        "INSERT INTO %s VALUES "
                                + "(5, 1, 6, 'S3', 7, 8, 9), "
                                + "(10, 1, 11, 'S4', 12, 13, 14)",
                        tableName));

        schemaManager.commitChanges(
                Arrays.asList(
                        SchemaChange.dropColumn("c"),
                        SchemaChange.dropColumn("e"),
                        SchemaChange.renameColumn("b", "bb"),
                        SchemaChange.renameColumn("d", "dd")));
        // The result fields are [a->INT, p -> INT, bb->BIGINT, dd->INT, f->INT] after evolution
        sql(
                String.format(
                        "INSERT INTO %s VALUES "
                                + "(19, 1, 20, 21, 22), "
                                + "(15, 1, 16, 17, 18), "
                                + "(23, 2, 24, 25, 26), "
                                + "(27, 2, 28, 29, 30)",
                        tableName));

        // Get files with latest snapshot
        List<Row> rows1 = sql(String.format("SELECT * FROM %s$files", tableName));
        for (Row row : rows1) {
            assertThat(StringUtils.endsWith((String) row.getField(3), ".orc"))
                    .isTrue(); // check file name
            assertThat((long) row.getField(8)).isGreaterThan(0L); // check file size
        }
        assertThat(getRowStringList(rows1))
                .containsExactlyInAnyOrder(
                        String.format(
                                "3,[2],0,orc,2,0,2,%s,{a=0, bb=0, dd=0, f=0, p=0},{a=23, bb=24, dd=25, f=26, p=2},{a=27, bb=28, dd=29, f=30, p=2}",
                                StringUtils.endsWith(tableName, "VALUE_COUNT")
                                        // value count table use all fields as min/max key
                                        ? "[23, 2, 24, 25, 26],[27, 2, 28, 29, 30]"
                                        : (StringUtils.endsWith(tableName, "APPEND_ONLY")
                                                // append only table has no min/max key
                                                ? ","
                                                // with key table use primary key trimmed partition
                                                : "[23],[27]")),
                        String.format(
                                "3,[1],0,orc,0,0,2,%s,{a=0, bb=0, dd=2, f=2, p=0},{a=1, bb=2, dd=null, f=null, p=1},{a=3, bb=4, dd=null, f=null, p=1}",
                                StringUtils.endsWith(tableName, "VALUE_COUNT")
                                        ? "[1, 1, 2, S1],[3, 1, 4, S2]"
                                        : (StringUtils.endsWith(tableName, "APPEND_ONLY")
                                                ? ","
                                                : "[1],[3]")),
                        String.format(
                                "3,[1],0,orc,1,0,2,%s,{a=0, bb=0, dd=0, f=0, p=0},{a=5, bb=6, dd=7, f=9, p=1},{a=10, bb=11, dd=12, f=14, p=1}",
                                StringUtils.endsWith(tableName, "VALUE_COUNT")
                                        ? "[5, 1, 6, S3, 7, 8, 9],[10, 1, 11, S4, 12, 13, 14]"
                                        : (StringUtils.endsWith(tableName, "APPEND_ONLY")
                                                ? ","
                                                : "[5],[10]")),
                        String.format(
                                "3,[1],0,orc,2,0,2,%s,{a=0, bb=0, dd=0, f=0, p=0},{a=15, bb=16, dd=17, f=18, p=1},{a=19, bb=20, dd=21, f=22, p=1}",
                                StringUtils.endsWith(tableName, "VALUE_COUNT")
                                        ? "[15, 1, 16, 17, 18],[19, 1, 20, 21, 22]"
                                        : (StringUtils.endsWith(tableName, "APPEND_ONLY")
                                                ? ","
                                                : "[15],[19]")));

        // Get files with snapshot id 2
        List<Row> rows2 =
                sql(
                        String.format(
                                "SELECT * FROM %s$files /*+ OPTIONS('scan.snapshot-id'='2') */",
                                tableName));
        for (Row row : rows2) {
            assertThat(StringUtils.endsWith((String) row.getField(3), ".orc"))
                    .isTrue(); // check file name
            assertThat((long) row.getField(8)).isGreaterThan(0L); // check file size
        }
        assertThat(getRowStringList(rows2))
                .containsExactlyInAnyOrder(
                        String.format(
                                "2,[1],0,orc,0,0,2,%s,{a=0, b=0, c=0, d=2, e=2, f=2, p=0},{a=1, b=2, c=S1, d=null, e=null, f=null, p=1},{a=3, b=4, c=S2, d=null, e=null, f=null, p=1}",
                                StringUtils.endsWith(tableName, "VALUE_COUNT")
                                        ? "[1, 1, 2, S1],[3, 1, 4, S2]"
                                        : (StringUtils.endsWith(tableName, "APPEND_ONLY")
                                                ? ","
                                                : "[1],[3]")),
                        String.format(
                                "2,[1],0,orc,1,0,2,%s,{a=0, b=0, c=0, d=0, e=0, f=0, p=0},{a=5, b=6, c=S3, d=7, e=8, f=9, p=1},{a=10, b=11, c=S4, d=12, e=13, f=14, p=1}",
                                StringUtils.endsWith(tableName, "VALUE_COUNT")
                                        ? "[5, 1, 6, S3, 7, 8, 9],[10, 1, 11, S4, 12, 13, 14]"
                                        : (StringUtils.endsWith(tableName, "APPEND_ONLY")
                                                ? ","
                                                : "[5],[10]")));
    }

    @Nonnull
    private List<String> getRowStringList(List<Row> rows) {
        return rows.stream()
                .map(
                        v ->
                                StringUtils.join(
                                        new Object[] {
                                            v.getField(0),
                                            v.getField(1),
                                            v.getField(2),
                                            v.getField(4),
                                            v.getField(5),
                                            v.getField(6),
                                            v.getField(7),
                                            v.getField(9),
                                            v.getField(10),
                                            v.getField(11),
                                            v.getField(12),
                                            v.getField(13)
                                        },
                                        ","))
                .collect(Collectors.toList());
    }
}
