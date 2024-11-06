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

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.table.system.AllTableOptionsTable;
import org.apache.paimon.table.system.CatalogOptionsTable;
import org.apache.paimon.table.system.SinkTableLineageTable;
import org.apache.paimon.table.system.SourceTableLineageTable;
import org.apache.paimon.utils.BlockingIterator;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.table.catalog.CatalogPartition;
import org.apache.flink.table.catalog.CatalogPartitionSpec;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.PartitionNotExistException;
import org.apache.flink.table.catalog.exceptions.TableNotPartitionedException;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.flink.table.api.config.TableConfigOptions.TABLE_DML_SYNC;
import static org.apache.paimon.catalog.Catalog.LAST_UPDATE_TIME_PROP;
import static org.apache.paimon.catalog.Catalog.NUM_FILES_PROP;
import static org.apache.paimon.catalog.Catalog.NUM_ROWS_PROP;
import static org.apache.paimon.catalog.Catalog.TOTAL_SIZE_PROP;
import static org.apache.paimon.testutils.assertj.PaimonAssertions.anyCauseMatches;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** ITCase for catalog tables. */
public class CatalogTableITCase extends CatalogITCaseBase {

    @Override
    protected boolean inferScanParallelism() {
        return true;
    }

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
        sql("INSERT INTO T VALUES (5, 6)");

        List<Row> result = sql("SELECT snapshot_id, schema_id, commit_kind FROM T$snapshots");
        assertThat(result)
                .containsExactly(
                        Row.of(1L, 0L, "APPEND"),
                        Row.of(2L, 0L, "APPEND"),
                        Row.of(3L, 0L, "APPEND"));

        result =
                sql(
                        "SELECT snapshot_id, schema_id, commit_kind FROM T$snapshots WHERE schema_id = 0");
        assertThat(result)
                .containsExactly(
                        Row.of(1L, 0L, "APPEND"),
                        Row.of(2L, 0L, "APPEND"),
                        Row.of(3L, 0L, "APPEND"));

        result =
                sql(
                        "SELECT snapshot_id, schema_id, commit_kind FROM T$snapshots WHERE snapshot_id = 2");
        assertThat(result).containsExactly(Row.of(2L, 0L, "APPEND"));

        result =
                sql(
                        "SELECT snapshot_id, schema_id, commit_kind FROM T$snapshots WHERE snapshot_id > 1");
        assertThat(result).containsExactly(Row.of(2L, 0L, "APPEND"), Row.of(3L, 0L, "APPEND"));

        result =
                sql(
                        "SELECT snapshot_id, schema_id, commit_kind FROM T$snapshots WHERE snapshot_id < 2");
        assertThat(result).containsExactly(Row.of(1L, 0L, "APPEND"));

        result =
                sql(
                        "SELECT snapshot_id, schema_id, commit_kind FROM T$snapshots WHERE snapshot_id >= 1");
        assertThat(result)
                .contains(
                        Row.of(1L, 0L, "APPEND"),
                        Row.of(2L, 0L, "APPEND"),
                        Row.of(3L, 0L, "APPEND"));

        result =
                sql(
                        "SELECT snapshot_id, schema_id, commit_kind FROM T$snapshots WHERE snapshot_id <= 2");
        assertThat(result).contains(Row.of(1L, 0L, "APPEND"), Row.of(2L, 0L, "APPEND"));

        result =
                sql(
                        "SELECT snapshot_id, schema_id, commit_kind FROM T$snapshots WHERE snapshot_id in (1, 2)");
        assertThat(result).contains(Row.of(1L, 0L, "APPEND"), Row.of(2L, 0L, "APPEND"));

        result =
                sql(
                        "SELECT snapshot_id, schema_id, commit_kind FROM T$snapshots WHERE snapshot_id in (1, 2) or schema_id=0");
        assertThat(result)
                .contains(
                        Row.of(1L, 0L, "APPEND"),
                        Row.of(2L, 0L, "APPEND"),
                        Row.of(3L, 0L, "APPEND"));
    }

    @Test
    public void testSnapshotsTableWithRecordCount() throws Exception {
        sql("CREATE TABLE T (a INT, b INT)");
        sql("INSERT INTO T VALUES (1, 2)");
        sql("INSERT INTO T VALUES (3, 4)");

        List<Row> result =
                sql(
                        "SELECT snapshot_id, total_record_count, delta_record_count, changelog_record_count FROM T$snapshots");
        assertThat(result)
                .containsExactlyInAnyOrder(Row.of(1L, 1L, 1L, 0L), Row.of(2L, 2L, 1L, 0L));
    }

    @Test
    public void testOptionsTable() throws Exception {
        sql("CREATE TABLE T (a INT, b INT)");
        sql("ALTER TABLE T SET ('snapshot.time-retained' = '5 h')");

        List<Row> result = sql("SELECT * FROM T$options");
        assertThat(result).containsExactly(Row.of("snapshot.time-retained", "5 h"));
    }

    @Test
    public void testAllTableOptions() {
        sql("CREATE TABLE T (a INT, b INT) with ('a.aa.aaa'='val1', 'b.bb.bbb'='val2')");
        sql("ALTER TABLE T SET ('c.cc.ccc' = 'val3')");

        List<Row> result = sql("SELECT * FROM sys.all_table_options");
        assertThat(result)
                .containsExactly(
                        Row.of("default", "T", "a.aa.aaa", "val1"),
                        Row.of("default", "T", "b.bb.bbb", "val2"),
                        Row.of("default", "T", "c.cc.ccc", "val3"));
    }

    @Test
    public void testCatalogOptionsTable() {
        List<Row> result = sql("SELECT * FROM sys.catalog_options");
        assertThat(result).containsExactly(Row.of("warehouse", path));
    }

    @Test
    public void testDropSystemDatabase() {
        assertThatCode(() -> sql("DROP DATABASE sys"))
                .hasRootCauseMessage("Can't do operation on system database.");
    }

    @Test
    public void testCreateSystemDatabase() {
        assertThatCode(() -> sql("CREATE DATABASE sys"))
                .hasRootCauseMessage("Can't do operation on system database.");
    }

    @Test
    public void testChangeTableInSystemDatabase() {
        sql("USE sys");
        assertThatCode(() -> sql("ALTER TABLE all_table_options SET ('bucket-num' = '5')"))
                .hasRootCauseMessage("Can't alter system table.");
    }

    @Test
    public void testSystemDatabase() {
        sql("USE " + Catalog.SYSTEM_DATABASE_NAME);
        assertThat(sql("SHOW TABLES"))
                .containsExactlyInAnyOrder(
                        Row.of(AllTableOptionsTable.ALL_TABLE_OPTIONS),
                        Row.of(CatalogOptionsTable.CATALOG_OPTIONS),
                        Row.of(SourceTableLineageTable.SOURCE_TABLE_LINEAGE),
                        Row.of(SinkTableLineageTable.SINK_TABLE_LINEAGE));
    }

    @Test
    public void testCreateSystemTable() {
        assertThatThrownBy(() -> sql("CREATE TABLE T$snapshots (a INT, b INT)"))
                .hasRootCauseMessage(
                        "Cannot 'createTable' for system table "
                                + "'Identifier{database='default', object='T$snapshots'}', please use data table.");
        assertThatThrownBy(() -> sql("CREATE TABLE T$aa$bb (a INT, b INT)"))
                .hasRootCauseMessage(
                        "System table can only contain one '$' separator, but this is: T$aa$bb");
    }

    @Test
    public void testManifestsTable() {
        sql("CREATE TABLE T (a INT, b INT)");
        sql("INSERT INTO T VALUES (1, 2)");

        List<Row> result = sql("SELECT schema_id, file_name, file_size FROM T$manifests");

        result.forEach(
                row -> {
                    assertThat((long) row.getField(0)).isEqualTo(0L);
                    assertThat(StringUtils.startsWith((String) row.getField(1), "manifest"))
                            .isTrue();
                    assertThat((long) row.getField(2)).isGreaterThan(0L);
                });
    }

    @Test
    public void testManifestsTableWithFileCount() {
        sql("CREATE TABLE T (a INT, b INT)");
        sql("INSERT INTO T VALUES (1, 2)");
        sql("INSERT INTO T VALUES (3, 4)");

        List<Row> result = sql("SELECT num_added_files, num_deleted_files FROM T$manifests");
        assertThat(result).containsExactlyInAnyOrder(Row.of(1L, 0L), Row.of(1L, 0L));
    }

    @Test
    public void testSchemasTable() {
        sql(
                "CREATE TABLE T(a INT, b INT, c STRING, PRIMARY KEY (a) NOT ENFORCED) with ('a.aa.aaa'='val1', 'b.bb.bbb'='val2')");
        sql("ALTER TABLE T SET ('snapshot.time-retained' = '5 h')");
        sql("ALTER TABLE T SET ('snapshot.num-retained.max' = '20')");
        sql("ALTER TABLE T SET ('snapshot.num-retained.min' = '18')");
        sql("ALTER TABLE T SET ('manifest.format' = 'avro')");

        assertThat(sql("SHOW CREATE TABLE T$schemas").toString())
                .isEqualTo(
                        "[+I[CREATE TABLE `PAIMON`.`default`.`T$schemas` (\n"
                                + "  `schema_id` BIGINT NOT NULL,\n"
                                + "  `fields` VARCHAR(2147483647) NOT NULL,\n"
                                + "  `partition_keys` VARCHAR(2147483647) NOT NULL,\n"
                                + "  `primary_keys` VARCHAR(2147483647) NOT NULL,\n"
                                + "  `options` VARCHAR(2147483647) NOT NULL,\n"
                                + "  `comment` VARCHAR(2147483647),\n"
                                + "  `update_time` TIMESTAMP(3) NOT NULL\n"
                                + ") ]]");

        List<Row> result =
                sql(
                        "SELECT schema_id, fields, partition_keys, "
                                + "primary_keys, options, `comment` FROM T$schemas order by schema_id");

        assertThat(result.toString())
                .isEqualTo(
                        "[+I[0, [{\"id\":0,\"name\":\"a\",\"type\":\"INT NOT NULL\"},"
                                + "{\"id\":1,\"name\":\"b\",\"type\":\"INT\"},{\"id\":2,\"name\":\"c\",\"type\":\"STRING\"}], [], [\"a\"], "
                                + "{\"a.aa.aaa\":\"val1\",\"b.bb.bbb\":\"val2\"}, ], "
                                + "+I[1, [{\"id\":0,\"name\":\"a\",\"type\":\"INT NOT NULL\"},"
                                + "{\"id\":1,\"name\":\"b\",\"type\":\"INT\"},{\"id\":2,\"name\":\"c\",\"type\":\"STRING\"}], [], [\"a\"], "
                                + "{\"a.aa.aaa\":\"val1\",\"snapshot.time-retained\":\"5 h\",\"b.bb.bbb\":\"val2\"}, ], "
                                + "+I[2, [{\"id\":0,\"name\":\"a\",\"type\":\"INT NOT NULL\"},"
                                + "{\"id\":1,\"name\":\"b\",\"type\":\"INT\"},{\"id\":2,\"name\":\"c\",\"type\":\"STRING\"}], [], [\"a\"], "
                                + "{\"a.aa.aaa\":\"val1\",\"snapshot.time-retained\":\"5 h\",\"b.bb.bbb\":\"val2\",\"snapshot.num-retained.max\":\"20\"}, ], "
                                + "+I[3, [{\"id\":0,\"name\":\"a\",\"type\":\"INT NOT NULL\"},{\"id\":1,\"name\":\"b\",\"type\":\"INT\"},"
                                + "{\"id\":2,\"name\":\"c\",\"type\":\"STRING\"}], [], [\"a\"], "
                                + "{\"a.aa.aaa\":\"val1\",\"snapshot.time-retained\":\"5 h\",\"b.bb.bbb\":\"val2\",\"snapshot.num-retained.max\":\"20\",\"snapshot.num-retained.min\":\"18\"}, ], "
                                + "+I[4, [{\"id\":0,\"name\":\"a\",\"type\":\"INT NOT NULL\"},{\"id\":1,\"name\":\"b\",\"type\":\"INT\"},{\"id\":2,\"name\":\"c\",\"type\":\"STRING\"}], [], [\"a\"], "
                                + "{\"a.aa.aaa\":\"val1\",\"snapshot.time-retained\":\"5 h\",\"b.bb.bbb\":\"val2\",\"snapshot.num-retained.max\":\"20\",\"manifest.format\":\"avro\","
                                + "\"snapshot.num-retained.min\":\"18\"}, ]]");

        result =
                sql(
                        "SELECT schema_id, fields, partition_keys, "
                                + "primary_keys, options, `comment` FROM T$schemas where schema_id = 0");
        assertThat(result.toString())
                .isEqualTo(
                        "[+I[0, [{\"id\":0,\"name\":\"a\",\"type\":\"INT NOT NULL\"},"
                                + "{\"id\":1,\"name\":\"b\",\"type\":\"INT\"},"
                                + "{\"id\":2,\"name\":\"c\",\"type\":\"STRING\"}], [], [\"a\"], "
                                + "{\"a.aa.aaa\":\"val1\",\"b.bb.bbb\":\"val2\"}, ]]");

        result =
                sql(
                        "SELECT schema_id, fields, partition_keys, "
                                + "primary_keys, options, `comment` FROM T$schemas where schema_id>0 and schema_id<3");
        assertThat(result.toString())
                .isEqualTo(
                        "[+I[1, [{\"id\":0,\"name\":\"a\",\"type\":\"INT NOT NULL\"},"
                                + "{\"id\":1,\"name\":\"b\",\"type\":\"INT\"},{\"id\":2,\"name\":\"c\",\"type\":\"STRING\"}], [], [\"a\"], "
                                + "{\"a.aa.aaa\":\"val1\",\"snapshot.time-retained\":\"5 h\",\"b.bb.bbb\":\"val2\"}, ], "
                                + "+I[2, [{\"id\":0,\"name\":\"a\",\"type\":\"INT NOT NULL\"},{\"id\":1,\"name\":\"b\",\"type\":\"INT\"},"
                                + "{\"id\":2,\"name\":\"c\",\"type\":\"STRING\"}], [], [\"a\"], {\"a.aa.aaa\":\"val1\",\"snapshot.time-retained\":\"5 h\","
                                + "\"b.bb.bbb\":\"val2\",\"snapshot.num-retained.max\":\"20\"}, ]]");

        // test for IN filter
        result =
                sql(
                        "SELECT schema_id, fields, partition_keys, "
                                + "primary_keys, options, `comment` FROM T$schemas where schema_id in (1, 3)");
        assertThat(result.toString())
                .isEqualTo(
                        "[+I[1, [{\"id\":0,\"name\":\"a\",\"type\":\"INT NOT NULL\"},"
                                + "{\"id\":1,\"name\":\"b\",\"type\":\"INT\"},{\"id\":2,\"name\":\"c\",\"type\":\"STRING\"}], [], [\"a\"], "
                                + "{\"a.aa.aaa\":\"val1\",\"snapshot.time-retained\":\"5 h\",\"b.bb.bbb\":\"val2\"}, ], "
                                + "+I[3, [{\"id\":0,\"name\":\"a\",\"type\":\"INT NOT NULL\"},{\"id\":1,\"name\":\"b\",\"type\":\"INT\"},"
                                + "{\"id\":2,\"name\":\"c\",\"type\":\"STRING\"}], [], [\"a\"], "
                                + "{\"a.aa.aaa\":\"val1\",\"snapshot.time-retained\":\"5 h\",\"b.bb.bbb\":\"val2\",\"snapshot.num-retained.max\":\"20\",\"snapshot.num-retained.min\":\"18\"}, ]]");

        result =
                sql(
                        "SELECT schema_id, fields, partition_keys, "
                                + "primary_keys, options, `comment` FROM T$schemas where schema_id in (1, 3) or fields='[{\"id\":0,\"name\":\"a\",\"type\":\"INT NOT NULL\"},{\"id\":1,\"name\":\"b\",\"type\":\"INT\"},{\"id\":2,\"name\":\"c\",\"type\":\"STRING\"}]' order by schema_id");
        assertThat(result.toString())
                .isEqualTo(
                        "[+I[0, [{\"id\":0,\"name\":\"a\",\"type\":\"INT NOT NULL\"},"
                                + "{\"id\":1,\"name\":\"b\",\"type\":\"INT\"},{\"id\":2,\"name\":\"c\",\"type\":\"STRING\"}], [], [\"a\"], "
                                + "{\"a.aa.aaa\":\"val1\",\"b.bb.bbb\":\"val2\"}, ], "
                                + "+I[1, [{\"id\":0,\"name\":\"a\",\"type\":\"INT NOT NULL\"},"
                                + "{\"id\":1,\"name\":\"b\",\"type\":\"INT\"},{\"id\":2,\"name\":\"c\",\"type\":\"STRING\"}], [], [\"a\"], "
                                + "{\"a.aa.aaa\":\"val1\",\"snapshot.time-retained\":\"5 h\",\"b.bb.bbb\":\"val2\"}, ], "
                                + "+I[2, [{\"id\":0,\"name\":\"a\",\"type\":\"INT NOT NULL\"},"
                                + "{\"id\":1,\"name\":\"b\",\"type\":\"INT\"},{\"id\":2,\"name\":\"c\",\"type\":\"STRING\"}], [], [\"a\"], "
                                + "{\"a.aa.aaa\":\"val1\",\"snapshot.time-retained\":\"5 h\",\"b.bb.bbb\":\"val2\",\"snapshot.num-retained.max\":\"20\"}, ], "
                                + "+I[3, [{\"id\":0,\"name\":\"a\",\"type\":\"INT NOT NULL\"},{\"id\":1,\"name\":\"b\",\"type\":\"INT\"},"
                                + "{\"id\":2,\"name\":\"c\",\"type\":\"STRING\"}], [], [\"a\"], "
                                + "{\"a.aa.aaa\":\"val1\",\"snapshot.time-retained\":\"5 h\",\"b.bb.bbb\":\"val2\",\"snapshot.num-retained.max\":\"20\",\"snapshot.num-retained.min\":\"18\"}, ], "
                                + "+I[4, [{\"id\":0,\"name\":\"a\",\"type\":\"INT NOT NULL\"},{\"id\":1,\"name\":\"b\",\"type\":\"INT\"},{\"id\":2,\"name\":\"c\",\"type\":\"STRING\"}], [], [\"a\"], "
                                + "{\"a.aa.aaa\":\"val1\",\"snapshot.time-retained\":\"5 h\",\"b.bb.bbb\":\"val2\",\"snapshot.num-retained.max\":\"20\",\"manifest.format\":\"avro\","
                                + "\"snapshot.num-retained.min\":\"18\"}, ]]");

        // check with not exist schema id
        assertThatThrownBy(
                        () ->
                                sql(
                                        "SELECT schema_id, fields, partition_keys, "
                                                + "primary_keys, options, `comment` FROM T$schemas where schema_id = 5"))
                .hasCauseInstanceOf(RuntimeException.class)
                .hasRootCauseMessage("schema id: 5 should not greater than max schema id: 4");

        // check with not exist schema id
        assertThatThrownBy(
                        () ->
                                sql(
                                        "SELECT schema_id, fields, partition_keys, "
                                                + "primary_keys, options, `comment` FROM T$schemas where schema_id>=6"))
                .hasCauseInstanceOf(RuntimeException.class)
                .hasRootCauseMessage("schema id: 6 should not greater than max schema id: 4");
    }

    @Test
    public void testSnapshotsSchemasTable() {
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
    public void testCreateTableLike() {
        sql("CREATE TABLE T (a INT)");
        sql("CREATE TABLE T1 LIKE T (EXCLUDING OPTIONS)");
        List<Row> result =
                sql(
                        "SELECT schema_id, fields, partition_keys, "
                                + "primary_keys, options, `comment` FROM T1$schemas s");
        assertThat(result.toString())
                .isEqualTo("[+I[0, [{\"id\":0,\"name\":\"a\",\"type\":\"INT\"}], [], [], {}, ]]");
    }

    @Test
    public void testCreateTableAs() {
        sql("CREATE TABLE t (a INT)");
        sql("INSERT INTO t VALUES(1),(2)");
        sql("CREATE TABLE t1 AS SELECT * FROM t");
        List<Row> result =
                sql(
                        "SELECT schema_id, fields, partition_keys, "
                                + "primary_keys, options, `comment` FROM t1$schemas s");
        assertThat(result.toString())
                .isEqualTo("[+I[0, [{\"id\":0,\"name\":\"a\",\"type\":\"INT\"}], [], [], {}, ]]");
        List<Row> data = sql("SELECT * FROM t1");
        assertThat(data).containsExactlyInAnyOrder(Row.of(1), Row.of(2));

        // partition table
        sql(
                "CREATE TABLE t_p (\n"
                        + "    user_id BIGINT,\n"
                        + "    item_id BIGINT,\n"
                        + "    behavior STRING,\n"
                        + "    dt STRING,\n"
                        + "    hh STRING\n"
                        + ") PARTITIONED BY (dt, hh)");
        sql("INSERT INTO t_p SELECT 1,2,'a','2023-02-19','12'");
        sql("CREATE TABLE t1_p WITH ('partition' = 'dt' ) AS SELECT * FROM t_p");
        List<Row> resultPartition =
                sql(
                        "SELECT schema_id, fields, partition_keys, "
                                + "primary_keys, options, `comment` FROM t1_p$schemas s");
        assertThat(resultPartition.toString())
                .isEqualTo(
                        "[+I[0, [{\"id\":0,\"name\":\"user_id\",\"type\":\"BIGINT\"},{\"id\":1,\"name\":\"item_id\",\"type\":\"BIGINT\"},"
                                + "{\"id\":2,\"name\":\"behavior\",\"type\":\"STRING\"},{\"id\":3,\"name\":\"dt\",\"type\":\"STRING\"},"
                                + "{\"id\":4,\"name\":\"hh\",\"type\":\"STRING\"}], [\"dt\"], [], {}, ]]");
        List<Row> dataPartition = sql("SELECT * FROM t1_p");
        assertThat(dataPartition.toString()).isEqualTo("[+I[1, 2, a, 2023-02-19, 12]]");

        // change options
        sql("CREATE TABLE t_option (a INT) WITH ('file.format' = 'orc')");
        sql("INSERT INTO t_option VALUES(1),(2)");
        sql("CREATE TABLE t1_option WITH ('file.format' = 'parquet') AS SELECT * FROM t_option");
        List<Row> resultOption = sql("SELECT * FROM t1_option$options");
        assertThat(resultOption).containsExactly(Row.of("file.format", "parquet"));
        List<Row> dataOption = sql("SELECT * FROM t1_option");
        assertThat(dataOption).containsExactlyInAnyOrder(Row.of(1), Row.of(2));

        // primary key
        sql(
                "CREATE TABLE t_pk (\n"
                        + "    user_id BIGINT,\n"
                        + "    item_id BIGINT,\n"
                        + "    behavior STRING,\n"
                        + "    dt STRING,\n"
                        + "    hh STRING,\n"
                        + "    PRIMARY KEY (dt, hh, user_id) NOT ENFORCED\n"
                        + ")");
        sql("INSERT INTO t_pk VALUES(1,2,'aaa','2020-01-02','09')");
        sql("CREATE TABLE t_pk_as WITH ('primary-key' = 'dt') AS SELECT * FROM t_pk");
        List<Row> resultPk = sql("SHOW CREATE TABLE t_pk_as");
        assertThat(resultPk.toString()).contains("PRIMARY KEY (`dt`)");
        List<Row> dataPk = sql("SELECT * FROM t_pk_as");
        assertThat(dataPk.toString()).isEqualTo("[+I[1, 2, aaa, 2020-01-02, 09]]");

        // primary key + partition
        sql(
                "CREATE TABLE t_all (\n"
                        + "    user_id BIGINT,\n"
                        + "    item_id BIGINT,\n"
                        + "    behavior STRING,\n"
                        + "    dt STRING,\n"
                        + "    hh STRING,\n"
                        + "    PRIMARY KEY (dt, hh, user_id) NOT ENFORCED\n"
                        + ") PARTITIONED BY (dt, hh)");
        sql("INSERT INTO t_all VALUES(1,2,'login','2020-01-02','09')");
        sql(
                "CREATE TABLE t_all_as WITH ('primary-key' = 'dt,hh' , 'partition' = 'dt' ) AS SELECT * FROM t_all");
        List<Row> resultAll = sql("SHOW CREATE TABLE t_all_as");
        assertThat(resultAll.toString()).contains("PRIMARY KEY (`dt`, `hh`)");
        assertThat(resultAll.toString()).contains("PARTITIONED BY (`dt`)");
        List<Row> dataAll = sql("SELECT * FROM t_all_as");
        assertThat(dataAll.toString()).isEqualTo("[+I[1, 2, login, 2020-01-02, 09]]");

        // primary key do not exist.
        sql(
                "CREATE TABLE t_pk_not_exist (\n"
                        + "    user_id BIGINT,\n"
                        + "    item_id BIGINT,\n"
                        + "    behavior STRING,\n"
                        + "    dt STRING,\n"
                        + "    hh STRING,\n"
                        + "    PRIMARY KEY (dt, hh, user_id) NOT ENFORCED\n"
                        + ")");

        assertThatThrownBy(
                        () ->
                                sql(
                                        "CREATE TABLE t_pk_not_exist_as WITH ('primary-key' = 'aaa') AS SELECT * FROM t_pk_not_exist"))
                .hasRootCauseMessage(
                        "Table column [user_id, item_id, behavior, dt, hh] should include all primary key constraint [aaa]");

        // primary key in option and DDL.
        assertThatThrownBy(
                        () ->
                                sql(
                                        "CREATE TABLE t_pk_ddl_option ("
                                                + "user_id BIGINT,"
                                                + "item_id BIGINT,"
                                                + "behavior STRING,"
                                                + "dt STRING,"
                                                + "hh STRING,"
                                                + "PRIMARY KEY (dt, hh, user_id) NOT ENFORCED"
                                                + ") WITH ('primary-key' = 'dt')"))
                .hasRootCauseMessage(
                        "Cannot define primary key on DDL and table options at the same time.");

        // partition do not exist.
        sql(
                "CREATE TABLE t_partition_not_exist (\n"
                        + "    user_id BIGINT,\n"
                        + "    item_id BIGINT,\n"
                        + "    behavior STRING,\n"
                        + "    dt STRING,\n"
                        + "    hh STRING\n"
                        + ") PARTITIONED BY (dt, hh) ");

        assertThatThrownBy(
                        () ->
                                sql(
                                        "CREATE TABLE t_partition_not_exist_as WITH ('partition' = 'aaa') AS SELECT * FROM t_partition_not_exist"))
                .hasRootCauseMessage(
                        "Table column [user_id, item_id, behavior, dt, hh] should include all partition fields [aaa]");

        // partition in option and DDL.
        assertThatThrownBy(
                        () ->
                                sql(
                                        "CREATE TABLE t_partition_ddl_option ("
                                                + "user_id BIGINT,"
                                                + "item_id BIGINT,"
                                                + "behavior STRING,"
                                                + "dt STRING,"
                                                + "hh STRING"
                                                + ") PARTITIONED BY (dt, hh)  WITH ('partition' = 'dt')"))
                .hasRootCauseMessage(
                        "Cannot define partition on DDL and table options at the same time.");
    }

    @Test
    public void testConflictOption() {
        assertThatThrownBy(
                        () -> sql("CREATE TABLE T (a INT) WITH ('changelog-producer' = 'input')"))
                .rootCause()
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining(
                        "Can not set changelog-producer on table without primary keys");

        sql("CREATE TABLE T (a INT)");
        assertThatThrownBy(() -> sql("ALTER TABLE T SET ('changelog-producer'='input')"))
                .rootCause()
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining(
                        "Can not set changelog-producer on table without primary keys");
    }

    @Test
    public void testShowPartitions() {
        sql(
                "CREATE TABLE NoPartitionTable (\n"
                        + "    user_id BIGINT,\n"
                        + "    item_id BIGINT,\n"
                        + "    behavior STRING,\n"
                        + "    dt STRING,\n"
                        + "    hh STRING,\n"
                        + "    PRIMARY KEY (dt, hh, user_id) NOT ENFORCED\n"
                        + ")");
        assertThatThrownBy(() -> sql("SHOW PARTITIONS NoPartitionTable"))
                .rootCause()
                .isInstanceOf(TableNotPartitionedException.class)
                .hasMessage("Table default.NoPartitionTable in catalog PAIMON is not partitioned.");

        sql(
                "CREATE TABLE PartitionTable (\n"
                        + "    user_id BIGINT,\n"
                        + "    item_id BIGINT,\n"
                        + "    behavior STRING,\n"
                        + "    dt STRING,\n"
                        + "    hh STRING,\n"
                        + "    PRIMARY KEY (dt, hh, user_id) NOT ENFORCED\n"
                        + ") PARTITIONED BY (dt, hh)");
        sql("INSERT INTO PartitionTable select 1,1,'a','2020-01-01','10'");
        sql("INSERT INTO PartitionTable select 2,2,'b','2020-01-02','11'");
        sql("INSERT INTO PartitionTable select 3,3,'c','2020-01-03','11'");
        List<Row> result = sql("SHOW PARTITIONS PartitionTable");
        assertThat(result)
                .containsExactlyInAnyOrder(
                        Row.of("dt=2020-01-01/hh=10"),
                        Row.of("dt=2020-01-02/hh=11"),
                        Row.of("dt=2020-01-03/hh=11"));

        result = sql("SHOW PARTITIONS PartitionTable partition (hh='11')");
        assertThat(result)
                .containsExactlyInAnyOrder(
                        Row.of("dt=2020-01-02/hh=11"), Row.of("dt=2020-01-03/hh=11"));

        result = sql("SHOW PARTITIONS PartitionTable partition (dt='2020-01-02', hh='11')");
        assertThat(result).containsExactlyInAnyOrder(Row.of("dt=2020-01-02/hh=11"));
    }

    @Test
    void testPKTableGetPartition() throws Exception {
        sql(
                "CREATE TABLE IF NOT EXISTS PK_T (id INT PRIMARY KEY NOT ENFORCED, par STRING, word STRING) PARTITIONED BY(par)");
        FlinkCatalog flinkCatalog = flinkCatalog();
        ObjectPath tablePath = new ObjectPath(flinkCatalog.getDefaultDatabase(), "PK_T");
        sql("INSERT INTO PK_T VALUES (1, 'p1', 'a'),(2, 'p1', 'b'),(3, 'p2', 'a'),(4, 'p2', 'b')");
        List<CatalogPartitionSpec> partitions = flinkCatalog.listPartitions(tablePath);
        Map<String, Map<String, String>> partitionPropertiesMap1 =
                getPartitionProperties(flinkCatalog, tablePath, partitions);
        assertThat(partitionPropertiesMap1)
                .allSatisfy(
                        (par, properties) -> {
                            assertThat(properties.get(NUM_ROWS_PROP)).isEqualTo("2");
                            assertThat(properties.get(LAST_UPDATE_TIME_PROP)).isNotBlank();
                            assertThat(properties.get(NUM_FILES_PROP)).isEqualTo("1");
                            assertThat(properties.get(TOTAL_SIZE_PROP)).isNotBlank();
                        });
        // update p1 data
        sql("UPDATE PK_T SET word = 'c' WHERE id = 2");
        Map<String, Map<String, String>> partitionPropertiesMap2 =
                getPartitionProperties(flinkCatalog, tablePath, partitions);
        assertPartitionNotUpdate("p2", partitionPropertiesMap1, partitionPropertiesMap2);
        // we compute the count of changelog data, not distinct count(*).
        assertPartitionUpdateTo("p1", partitionPropertiesMap1, partitionPropertiesMap2, 3L, 2L);

        // delete data from p2
        sql("DELETE FROM PK_T WHERE id = 3");
        Map<String, Map<String, String>> partitionPropertiesMap3 =
                getPartitionProperties(flinkCatalog, tablePath, partitions);
        assertPartitionNotUpdate("p1", partitionPropertiesMap2, partitionPropertiesMap3);
        // we compute the count of changelog data, not distinct count(*).
        assertPartitionUpdateTo("p2", partitionPropertiesMap2, partitionPropertiesMap3, 3L, 2L);
    }

    @Test
    void testNonPKTableGetPartition() throws Exception {
        FlinkCatalog flinkCatalog = flinkCatalog();
        ObjectPath tablePath = new ObjectPath(flinkCatalog.getDefaultDatabase(), "NON_PK_T");
        sql("CREATE TABLE IF NOT EXISTS NON_PK_T (par STRING, a INT, b INT) PARTITIONED BY(par)");
        sql("INSERT INTO NON_PK_T VALUES ('p1', 2, 3),('p2', 4, 5)");
        List<CatalogPartitionSpec> partitions = flinkCatalog.listPartitions(tablePath);
        Map<String, Map<String, String>> partitionPropertiesMap1 =
                getPartitionProperties(flinkCatalog, tablePath, partitions);

        assertThat(partitionPropertiesMap1)
                .allSatisfy(
                        (par, properties) -> {
                            assertThat(properties.get(NUM_ROWS_PROP)).isEqualTo("1");
                            assertThat(properties.get(LAST_UPDATE_TIME_PROP)).isNotBlank();
                        });

        // append data to p1
        sql("INSERT INTO NON_PK_T VALUES ('p1', 6, 7), ('p1', 8, 9)");
        Map<String, Map<String, String>> partitionPropertiesMap2 =
                getPartitionProperties(flinkCatalog, tablePath, partitions);

        assertPartitionNotUpdate("p2", partitionPropertiesMap1, partitionPropertiesMap2);
        assertPartitionUpdateTo("p1", partitionPropertiesMap1, partitionPropertiesMap2, 3L, 2L);
    }

    @Test
    public void testDropPartition() {
        sql(
                "CREATE TABLE PartitionTable (\n"
                        + "    user_id BIGINT,\n"
                        + "    item_id BIGINT,\n"
                        + "    behavior STRING,\n"
                        + "    dt STRING,\n"
                        + "    hh STRING,\n"
                        + "    PRIMARY KEY (dt, hh, user_id) NOT ENFORCED\n"
                        + ") PARTITIONED BY (dt, hh)");
        sql("INSERT INTO PartitionTable select 1,1,'a','2020-01-01','10'");
        sql("INSERT INTO PartitionTable select 2,2,'b','2020-01-02','11'");
        sql("INSERT INTO PartitionTable select 3,3,'c','2020-01-03','11'");
        sql("INSERT INTO PartitionTable select 4,4,'d','2020-01-04','14'");
        sql("INSERT INTO PartitionTable select 5,5,'e','2020-01-05','15'");

        assertThatThrownBy(
                        () ->
                                sql(
                                        "ALTER TABLE PartitionTable DROP PARTITION (`dt` = '2020-10-10')"))
                .rootCause()
                .isInstanceOf(PartitionNotExistException.class)
                .hasMessage(
                        "Partition CatalogPartitionSpec{{dt=2020-10-10}} of table default.PartitionTable in catalog PAIMON does not exist.");

        assertThat(
                        sql("ALTER TABLE PartitionTable DROP IF EXISTS PARTITION (`dt` = '2020-10-10')")
                                .toString())
                .isEqualTo("[+I[OK]]");

        List<Row> result = sql("SHOW PARTITIONS PartitionTable");
        assertThat(result)
                .containsExactlyInAnyOrder(
                        Row.of("dt=2020-01-01/hh=10"),
                        Row.of("dt=2020-01-02/hh=11"),
                        Row.of("dt=2020-01-03/hh=11"),
                        Row.of("dt=2020-01-04/hh=14"),
                        Row.of("dt=2020-01-05/hh=15"));

        // drop a partition
        sql("ALTER TABLE PartitionTable DROP PARTITION (`dt` = '2020-01-01', `hh` = '10')");
        result = sql("SHOW PARTITIONS PartitionTable");
        assertThat(result)
                .containsExactlyInAnyOrder(
                        Row.of("dt=2020-01-02/hh=11"),
                        Row.of("dt=2020-01-03/hh=11"),
                        Row.of("dt=2020-01-04/hh=14"),
                        Row.of("dt=2020-01-05/hh=15"));

        // drop two partitions
        sql("ALTER TABLE PartitionTable DROP PARTITION (dt ='2020-01-04'), PARTITION (hh='11')");
        result = sql("SHOW PARTITIONS PartitionTable");
        assertThat(result).containsExactly(Row.of("dt=2020-01-05/hh=15"));
    }

    @Test
    public void testFileFormatPerLevel() {
        sql(
                "CREATE TABLE T1 (a INT PRIMARY KEY NOT ENFORCED, b STRING) "
                        + "WITH ('num-sorted-run.compaction-trigger'='2',"
                        + "'file.format.per.level' = '0:avro,3:parquet',"
                        + " 'num-levels' = '4')");
        sql("INSERT INTO T1 SELECT 1,'AAA'");
        sql("INSERT INTO T1 SELECT 2,'BBB'");
        sql("INSERT INTO T1 SELECT 3,'CCC'");
        List<Row> rows = sql("SELECT * FROM T1");
        assertThat(rows)
                .containsExactlyInAnyOrder(Row.of(1, "AAA"), Row.of(2, "BBB"), Row.of(3, "CCC"));

        rows = sql("SELECT level,file_format FROM T1$files");
        assertThat(rows).containsExactlyInAnyOrder(Row.of(3, "parquet"), Row.of(0, "avro"));
    }

    @Test
    public void testFilesTable() {
        sql(
                "CREATE TABLE T_WITH_KEY (a INT, p INT, b BIGINT, c STRING, PRIMARY KEY (a, p) NOT ENFORCED) "
                        + "PARTITIONED BY (p) ");
        assertFilesTable("T_WITH_KEY");

        sql(
                "CREATE TABLE T_APPEND_ONLY (a INT, p INT, b BIGINT, c STRING) "
                        + "PARTITIONED BY (p)");
        assertFilesTable("T_APPEND_ONLY");
    }

    private void assertFilesTable(String tableName) {
        assertThat(sql(String.format("SELECT * FROM %s$files", tableName))).isEmpty();

        sql(String.format("INSERT INTO %s VALUES (3, 1, 4, 'S2'), (1, 1, 2, 'S1')", tableName));

        // The result fields are [a->INT, p -> INT, b->BIGINT, c->STRING, d->INT, e->INT, f->INT]
        // after
        // evolution
        sql(String.format("ALTER TABLE %s ADD (d INT, e INT, f INT)", tableName));

        sql(
                String.format(
                        "INSERT INTO %s VALUES "
                                + "(5, 1, 6, 'S3', 7, 8, 9), "
                                + "(10, 1, 11, 'S4', 12, 13, 14)",
                        tableName));

        sql(String.format("ALTER TABLE %s DROP (c, e)", tableName));
        sql(String.format("ALTER TABLE %s RENAME d TO dd", tableName));
        sql(String.format("ALTER TABLE %s RENAME b TO bb", tableName));

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
            assertThat(StringUtils.endsWith((String) row.getField(2), ".parquet"))
                    .isTrue(); // check file name
            assertThat((long) row.getField(7)).isGreaterThan(0L); // check file size
        }
        assertThat(getRowStringList(rows1))
                .containsExactlyInAnyOrder(
                        String.format(
                                "[2],0,parquet,4,0,2,%s,{a=0, bb=0, dd=0, f=0, p=0},{a=23, bb=24, dd=25, f=26, p=2},{a=27, bb=28, dd=29, f=30, p=2}",
                                StringUtils.endsWith(tableName, "VALUE_COUNT")
                                        // value count table use all fields as min/max key
                                        ? "[23, 2, 24, 25, 26],[27, 2, 28, 29, 30]"
                                        : (StringUtils.endsWith(tableName, "APPEND_ONLY")
                                                // append only table has no min/max key
                                                ? ","
                                                // with key table use primary key trimmed partition
                                                : "[23],[27]")),
                        String.format(
                                "[1],0,parquet,0,0,2,%s,{a=0, bb=0, dd=2, f=2, p=0},{a=1, bb=2, dd=null, f=null, p=1},{a=3, bb=4, dd=null, f=null, p=1}",
                                StringUtils.endsWith(tableName, "VALUE_COUNT")
                                        ? "[1, 1, 2, S1],[3, 1, 4, S2]"
                                        : (StringUtils.endsWith(tableName, "APPEND_ONLY")
                                                ? ","
                                                : "[1],[3]")),
                        String.format(
                                "[1],0,parquet,1,0,2,%s,{a=0, bb=0, dd=0, f=0, p=0},{a=5, bb=6, dd=7, f=9, p=1},{a=10, bb=11, dd=12, f=14, p=1}",
                                StringUtils.endsWith(tableName, "VALUE_COUNT")
                                        ? "[5, 1, 6, S3, 7, 8, 9],[10, 1, 11, S4, 12, 13, 14]"
                                        : (StringUtils.endsWith(tableName, "APPEND_ONLY")
                                                ? ","
                                                : "[5],[10]")),
                        String.format(
                                "[1],0,parquet,4,0,2,%s,{a=0, bb=0, dd=0, f=0, p=0},{a=15, bb=16, dd=17, f=18, p=1},{a=19, bb=20, dd=21, f=22, p=1}",
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
            assertThat(StringUtils.endsWith((String) row.getField(2), ".parquet"))
                    .isTrue(); // check file name
            assertThat((long) row.getField(7)).isGreaterThan(0L); // check file size
        }
        assertThat(getRowStringList(rows2))
                .containsExactlyInAnyOrder(
                        String.format(
                                "[1],0,parquet,0,0,2,%s,{a=0, b=0, c=0, d=2, e=2, f=2, p=0},{a=1, b=2, c=S1, d=null, e=null, f=null, p=1},{a=3, b=4, c=S2, d=null, e=null, f=null, p=1}",
                                StringUtils.endsWith(tableName, "VALUE_COUNT")
                                        ? "[1, 1, 2, S1],[3, 1, 4, S2]"
                                        : (StringUtils.endsWith(tableName, "APPEND_ONLY")
                                                ? ","
                                                : "[1],[3]")),
                        String.format(
                                "[1],0,parquet,1,0,2,%s,{a=0, b=0, c=0, d=0, e=0, f=0, p=0},{a=5, b=6, c=S3, d=7, e=8, f=9, p=1},{a=10, b=11, c=S4, d=12, e=13, f=14, p=1}",
                                StringUtils.endsWith(tableName, "VALUE_COUNT")
                                        ? "[5, 1, 6, S3, 7, 8, 9],[10, 1, 11, S4, 12, 13, 14]"
                                        : (StringUtils.endsWith(tableName, "APPEND_ONLY")
                                                ? ","
                                                : "[5],[10]")));
    }

    @Test
    public void testFilesTableWithFilter() {
        tEnv.getConfig().set(TABLE_DML_SYNC, true);
        sql(
                "CREATE TABLE T_WITH_FILTER (k INT, p INT, v INT, PRIMARY KEY (k, p) NOT ENFORCED) "
                        + "PARTITIONED BY (p) WITH ('bucket'='2')");
        sql("INSERT INTO T_WITH_FILTER VALUES (1, 2, 3), (4, 5, 6)");
        sql("CALL sys.compact('default.T_WITH_FILTER')");
        sql("INSERT INTO T_WITH_FILTER VALUES (7, 8, 9)");

        assertThat(sql("SELECT `partition`, bucket, level FROM T_WITH_FILTER$files"))
                .containsExactlyInAnyOrder(
                        Row.of("[2]", 0, 5), Row.of("[5]", 0, 5), Row.of("[8]", 1, 0));

        assertThat(
                        sql(
                                "SELECT `partition`, bucket, level FROM T_WITH_FILTER$files WHERE `partition`='[2]'"))
                .containsExactlyInAnyOrder(Row.of("[2]", 0, 5));

        assertThat(sql("SELECT `partition`, bucket, level FROM T_WITH_FILTER$files WHERE bucket=0"))
                .containsExactlyInAnyOrder(Row.of("[2]", 0, 5), Row.of("[5]", 0, 5));

        assertThat(sql("SELECT `partition`, bucket, level FROM T_WITH_FILTER$files WHERE level=0"))
                .containsExactlyInAnyOrder(Row.of("[8]", 1, 0));
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
                                            v.getField(3),
                                            v.getField(4),
                                            v.getField(5),
                                            v.getField(6),
                                            v.getField(8),
                                            v.getField(9),
                                            v.getField(10),
                                            v.getField(11),
                                            v.getField(12)
                                        },
                                        ","))
                .collect(Collectors.toList());
    }

    @Test
    public void testTagsTable() throws Exception {
        sql("CREATE TABLE T (a INT, b INT)");
        sql("INSERT INTO T VALUES (1, 2)");
        sql("INSERT INTO T VALUES (3, 4)");
        sql("INSERT INTO T VALUES (5, 6)");

        paimonTable("T").createTag("tag1", 1);
        paimonTable("T").createTag("tag2", 2);
        paimonTable("T").createTag("tag3", 3);

        List<Row> result =
                sql(
                        "SELECT tag_name, snapshot_id, schema_id, record_count FROM T$tags ORDER BY tag_name");
        assertThat(result)
                .containsExactly(
                        Row.of("tag1", 1L, 0L, 1L),
                        Row.of("tag2", 2L, 0L, 2L),
                        Row.of("tag3", 3L, 0L, 3L));

        result =
                sql(
                        "SELECT tag_name, snapshot_id, schema_id, record_count FROM T$tags where tag_name = 'tag1' ");
        assertThat(result).containsExactly(Row.of("tag1", 1L, 0L, 1L));

        result =
                sql(
                        "SELECT tag_name, snapshot_id, schema_id, record_count FROM T$tags where tag_name in ('tag1', 'tag3')");
        assertThat(result).containsExactly(Row.of("tag1", 1L, 0L, 1L), Row.of("tag3", 3L, 0L, 3L));

        result =
                sql(
                        "SELECT tag_name, snapshot_id, schema_id, record_count FROM T$tags where tag_name in ('tag1') or snapshot_id=2");
        assertThat(result).containsExactly(Row.of("tag1", 1L, 0L, 1L), Row.of("tag2", 2L, 0L, 2L));
    }

    @Test
    public void testConsumersTable() throws Exception {
        batchSql("CREATE TABLE T (a INT, b INT)");
        batchSql("INSERT INTO T VALUES (1, 2)");
        batchSql("INSERT INTO T VALUES (3, 4)");

        BlockingIterator<Row, Row> iterator =
                BlockingIterator.of(
                        streamSqlIter(
                                "SELECT * FROM T /*+ OPTIONS('consumer-id'='my1','consumer.expiration-time'='3h') */"));

        batchSql("INSERT INTO T VALUES (5, 6), (7, 8)");
        assertThat(iterator.collect(2)).containsExactlyInAnyOrder(Row.of(1, 2), Row.of(3, 4));
        iterator.close();

        List<Row> result = sql("SELECT * FROM T$consumers");
        assertThat(result).hasSize(1);
        assertThat(result.get(0).getField(0)).isEqualTo("my1");
        assertThat((Long) result.get(0).getField(1)).isGreaterThanOrEqualTo(3);
    }

    @Test
    public void testConsumerIdExpInBatchMode() {
        batchSql("CREATE TABLE T (a INT, b INT)");
        batchSql("INSERT INTO T VALUES (1, 2)");
        batchSql("INSERT INTO T VALUES (3, 4)");
        batchSql("INSERT INTO T VALUES (5, 6), (7, 8)");
        assertThatThrownBy(
                        () ->
                                sql(
                                        "SELECT * FROM T /*+ OPTIONS('consumer-id' = 'test-id') */ WHERE a = 1"))
                .rootCause()
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("consumer.expiration-time should be specified when using consumer-id.");
    }

    @Test
    public void testConsumerIdExpInStreamingMode() {
        batchSql("CREATE TABLE T (a INT, b INT)");
        batchSql("INSERT INTO T VALUES (1, 2)");
        batchSql("INSERT INTO T VALUES (3, 4)");
        assertThatThrownBy(
                        () ->
                                streamSqlIter(
                                        "SELECT * FROM T /*+ OPTIONS('consumer-id'='test-id') */"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("consumer.expiration-time should be specified when using consumer-id.");
    }

    @Test
    public void testPartitionsTable() {
        String table = "PARTITIONS_TABLE";
        sql("CREATE TABLE %s (a INT, p INT, b BIGINT, c STRING) " + "PARTITIONED BY (p)", table);

        // assert empty
        assertThat(sql("SELECT * FROM %s$partitions", table)).isEmpty();

        // assert new partitions
        sql("INSERT INTO %s VALUES (3, 1, 4, 'S2'), (1, 2, 2, 'S1'), (1, 2, 2, 'S1')", table);
        sql("INSERT INTO %s VALUES (3, 1, 4, 'S3'), (1, 2, 2, 'S4')", table);
        List<Row> result =
                sql("SELECT `partition`, record_count, file_count FROM %s$partitions", table);
        assertThat(result).containsExactlyInAnyOrder(Row.of("[1]", 2L, 2L), Row.of("[2]", 3L, 2L));

        // assert new files in partition
        sql("INSERT INTO %s VALUES (3, 4, 4, 'S3'), (1, 3, 2, 'S4')", table);
        sql("INSERT INTO %s VALUES (3, 1, 4, 'S3'), (1, 2, 2, 'S4')", table);
        result =
                sql(
                        String.format(
                                "SELECT `partition`, record_count, file_count FROM %s$partitions",
                                table));
        assertThat(result)
                .containsExactlyInAnyOrder(
                        Row.of("[1]", 3L, 3L),
                        Row.of("[2]", 4L, 3L),
                        Row.of("[3]", 1L, 1L),
                        Row.of("[4]", 1L, 1L));

        // assert delete partitions
        sql("ALTER TABLE %s DROP PARTITION (p = 2)", table);
        result =
                sql(
                        String.format(
                                "SELECT `partition`, record_count, file_count FROM %s$partitions",
                                table));
        assertThat(result)
                .containsExactlyInAnyOrder(
                        Row.of("[1]", 3L, 3L), Row.of("[3]", 1L, 1L), Row.of("[4]", 1L, 1L));

        // add new file to p 2
        sql("INSERT INTO %s VALUES (1, 2, 2, 'S1')", table);
        result =
                sql(
                        String.format(
                                "SELECT `partition`, record_count, file_count FROM %s$partitions",
                                table));
        assertThat(result)
                .containsExactlyInAnyOrder(
                        Row.of("[1]", 3L, 3L),
                        Row.of("[2]", 1L, 1L),
                        Row.of("[3]", 1L, 1L),
                        Row.of("[4]", 1L, 1L));
    }

    @Test
    public void testInvalidStreamingReadOverwrite() {
        String ddl =
                "CREATE TABLE T (a INT PRIMARY KEY NOT ENFORCED, b STRING)"
                        + "WITH ('changelog-producer' = '%s', 'streaming-read-overwrite' = 'true')";

        assertThatThrownBy(() -> sql(ddl, "full-compaction"))
                .satisfies(
                        anyCauseMatches(
                                UnsupportedOperationException.class,
                                "Cannot set streaming-read-overwrite to true when changelog producer "
                                        + "is full-compaction or lookup because it will read duplicated changes."));

        assertThatThrownBy(() -> sql(ddl, "lookup"))
                .satisfies(
                        anyCauseMatches(
                                UnsupportedOperationException.class,
                                "Cannot set streaming-read-overwrite to true when changelog producer "
                                        + "is full-compaction or lookup because it will read duplicated changes."));
    }

    @Test
    public void testShowTableMetadataComment() {
        sql("CREATE TABLE T (a INT, name VARCHAR METADATA COMMENT 'header1', b INT)");
        List<Row> result = sql("SHOW CREATE TABLE T");
        assertThat(result.get(0).toString())
                .contains(
                        "CREATE TABLE `PAIMON`.`default`.`T` (\n"
                                + "  `a` INT,\n"
                                + "  `name` VARCHAR(2147483647) METADATA COMMENT 'header1',\n"
                                + "  `b` INT\n"
                                + ")")
                .doesNotContain("schema");
    }

    @Test
    public void testReadOptimizedTable() {
        sql("CREATE TABLE T (k INT, v INT, PRIMARY KEY (k) NOT ENFORCED) WITH ('bucket' = '1')");
        innerTestReadOptimizedTable();

        sql("DROP TABLE T");
        sql("CREATE TABLE T (k INT, v INT, PRIMARY KEY (k) NOT ENFORCED) WITH ('bucket' = '-1')");
        innerTestReadOptimizedTable();
    }

    private void innerTestReadOptimizedTable() {
        // full compaction will always be performed at the end of batch jobs, as long as
        // full-compaction.delta-commits is set, regardless of its value
        sql(
                "INSERT INTO T /*+ OPTIONS('full-compaction.delta-commits' = '100') */ VALUES (1, 10), (2, 20)");
        List<Row> result = sql("SELECT k, v FROM T$ro ORDER BY k");
        assertThat(result).containsExactly(Row.of(1, 10), Row.of(2, 20));

        // no compaction, so result of ro table does not change
        sql("INSERT INTO T VALUES (1, 11), (3, 30)");
        result = sql("SELECT k, v FROM T$ro ORDER BY k");
        assertThat(result).containsExactly(Row.of(1, 10), Row.of(2, 20));

        sql(
                "INSERT INTO T /*+ OPTIONS('full-compaction.delta-commits' = '100') */ VALUES (2, 21), (3, 31)");
        result = sql("SELECT k, v FROM T$ro ORDER BY k");
        assertThat(result).containsExactly(Row.of(1, 11), Row.of(2, 21), Row.of(3, 31));
    }

    private static Map<String, Map<String, String>> getPartitionProperties(
            FlinkCatalog flinkCatalog,
            ObjectPath tablePath,
            List<CatalogPartitionSpec> partitions) {
        Map<String, Map<String, String>> partitionPropertiesMap = new HashMap<>();
        partitions.forEach(
                p -> {
                    try {
                        String partition = p.getPartitionSpec().get("par");
                        CatalogPartition catalogPartition = flinkCatalog.getPartition(tablePath, p);
                        Map<String, String> properties = catalogPartition.getProperties();
                        partitionPropertiesMap.put(partition, properties);
                    } catch (PartitionNotExistException e) {
                        throw new RuntimeException(e);
                    }
                });
        return partitionPropertiesMap;
    }

    private static void assertPartitionNotUpdate(
            String partition,
            Map<String, Map<String, String>> oldProperties,
            Map<String, Map<String, String>> newProperties) {
        assertThat(oldProperties.get(partition)).isEqualTo(newProperties.get(partition));
    }

    private static void assertPartitionUpdateTo(
            String partition,
            Map<String, Map<String, String>> oldProperties,
            Map<String, Map<String, String>> newProperties,
            Long expectedNumRows,
            Long expectedNumFiles) {
        Map<String, String> newPartitionProperties = newProperties.get(partition);
        Map<String, String> oldPartitionProperties = oldProperties.get(partition);
        assertThat(newPartitionProperties.get(NUM_ROWS_PROP))
                .isEqualTo(String.valueOf(expectedNumRows));
        assertThat(Long.valueOf(newPartitionProperties.get(LAST_UPDATE_TIME_PROP)))
                .isGreaterThan(Long.valueOf(oldPartitionProperties.get(LAST_UPDATE_TIME_PROP)));
        assertThat(newPartitionProperties.get(NUM_FILES_PROP))
                .isEqualTo(String.valueOf(expectedNumFiles));
        assertThat(Long.valueOf(newPartitionProperties.get(TOTAL_SIZE_PROP)))
                .isGreaterThan(Long.valueOf(oldPartitionProperties.get(TOTAL_SIZE_PROP)));
    }
}
