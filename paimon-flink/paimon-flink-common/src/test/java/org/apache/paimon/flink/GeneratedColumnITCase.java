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

import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.utils.BlockingIterator;

import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledIf;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests physical generated columns written through Flink SQL. */
@EnabledIf("supportsTargetColumns")
class GeneratedColumnITCase extends CatalogITCaseBase {

    static boolean supportsTargetColumns() {
        return isFlinkVersionGreaterThanOrEqualTo("1.18");
    }

    @ParameterizedTest
    @ValueSource(strings = {"append", "primary-key", "data-evolution"})
    void testOmittedGeneratedColumns(String mode) throws Exception {
        Schema.Builder builder =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("name", DataTypes.STRING(), null, "default")
                        .column("ts", DataTypes.TIMESTAMP(3))
                        .generatedColumn("label", DataTypes.STRING(), "UPPER(name)")
                        .generatedColumn("dt", DataTypes.DATE(), "CAST(ts AS DATE)")
                        .partitionKeys("dt");
        if (mode.equals("primary-key")) {
            builder.primaryKey("id", "dt");
        }
        if (mode.equals("data-evolution")) {
            builder.option("row-tracking.enabled", "true")
                    .option("data-evolution.enabled", "true")
                    .option("bucket", "-1");
        }
        createTable("events", builder.build());

        tEnv.executeSql(
                        "INSERT INTO events (ts, name, id) VALUES "
                                + "(TIMESTAMP '2026-10-30 12:00:00', 'hello', 1)")
                .await();
        tEnv.executeSql("INSERT INTO events (id, ts) SELECT 2, TIMESTAMP '2026-10-31 23:59:59.999'")
                .await();

        assertThat(sql("SELECT id, name, label, dt FROM events"))
                .containsExactlyInAnyOrder(
                        Row.of(1, "hello", "HELLO", LocalDate.of(2026, 10, 30)),
                        Row.of(2, "default", "DEFAULT", LocalDate.of(2026, 10, 31)));
        assertThat(sql("SELECT id FROM events WHERE dt = DATE '2026-10-30'"))
                .containsExactly(Row.of(1));
    }

    @ParameterizedTest
    @CsvSource({
        "append, DATE",
        "append, STRING",
        "primary-key, DATE",
        "primary-key, STRING",
        "data-evolution, DATE",
        "data-evolution, STRING"
    })
    void testStringEventDayPartition(String mode, String partitionType) throws Exception {
        boolean datePartition = partitionType.equals("DATE");
        boolean primaryKey = mode.equals("primary-key");
        Schema.Builder builder =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("event_day", DataTypes.STRING())
                        .column("payload", DataTypes.STRING())
                        .generatedColumn(
                                "dt",
                                datePartition ? DataTypes.DATE() : DataTypes.STRING(),
                                datePartition
                                        ? "CAST(event_day AS DATE)"
                                        : "CAST(CAST(event_day AS DATE) AS STRING)")
                        .partitionKeys("dt");
        if (primaryKey) {
            builder.primaryKey("id", "dt");
        } else if (mode.equals("data-evolution")) {
            builder.option("row-tracking.enabled", "true")
                    .option("data-evolution.enabled", "true")
                    .option("bucket", "-1");
        }
        createTable("events", builder.build());

        // The same id on different business dates must stay in separate partitions.
        tEnv.executeSql(
                        "INSERT INTO events (event_day, payload, id) VALUES "
                                + "('2025-12-31', 'initial', 1), ('2026-1-1', 'next-day', 1)")
                .await();
        // A late record must return to its business-date partition, including for primary keys.
        tEnv.executeSql(
                        "INSERT INTO events (id, event_day, payload) "
                                + "SELECT 1, '2025-12-31', 'late'")
                .await();

        Object previousDay = datePartition ? LocalDate.of(2025, 12, 31) : "2025-12-31";
        Object nextDay = datePartition ? LocalDate.of(2026, 1, 1) : "2026-01-01";
        List<Row> previousPartition = new ArrayList<>();
        previousPartition.add(Row.of(1, "2025-12-31", "late", previousDay));
        if (!primaryKey) {
            previousPartition.add(Row.of(1, "2025-12-31", "initial", previousDay));
        }
        Row nextPartition = Row.of(1, "2026-1-1", "next-day", nextDay);
        List<Row> expected = new ArrayList<>(previousPartition);
        expected.add(nextPartition);
        assertThat(sql("SELECT * FROM events")).containsExactlyInAnyOrderElementsOf(expected);
        assertThat(
                        sql(
                                "SELECT * FROM events WHERE dt = %s'2025-12-31'",
                                datePartition ? "DATE " : ""))
                .containsExactlyInAnyOrderElementsOf(previousPartition);
        assertThat(
                        sql(
                                "SELECT * FROM events WHERE dt = %s'2026-01-01'",
                                datePartition ? "DATE " : ""))
                .containsExactly(nextPartition);
        assertThat(sql("SELECT `partition` FROM events$partitions"))
                .containsExactlyInAnyOrder(Row.of("dt=2025-12-31"), Row.of("dt=2026-01-01"));
    }

    @Test
    void testSuppliedGeneratedValuesAndNulls() throws Exception {
        createTable(
                "numbers",
                Schema.newBuilder()
                        .column("text", DataTypes.STRING())
                        .generatedColumn("number", DataTypes.INT(), "CAST(text AS INT)")
                        .generatedColumn("label", DataTypes.STRING(), "UPPER(text)")
                        .build());

        tEnv.executeSql(
                        "INSERT INTO numbers VALUES ('invalid', 42, 'supplied'), "
                                + "('invalid', CAST(NULL AS INT), CAST(NULL AS STRING))")
                .await();
        tEnv.executeSql("INSERT INTO numbers (text, number) VALUES ('still invalid', 7)").await();
        tEnv.executeSql("INSERT INTO numbers (text) VALUES ('123'), (CAST(NULL AS STRING))")
                .await();

        assertThat(sql("SELECT * FROM numbers"))
                .containsExactlyInAnyOrder(
                        Row.of("invalid", 42, "supplied"),
                        Row.of("invalid", null, null),
                        Row.of("still invalid", 7, "STILL INVALID"),
                        Row.of("123", 123, "123"),
                        Row.of(null, null, null));
    }

    @Test
    void testStreamingInsertAndUpdatePreserveSuppliedColumns() throws Exception {
        createTable(
                "events",
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("name", DataTypes.STRING())
                        .generatedColumn("label", DataTypes.STRING(), "UPPER(name)")
                        .primaryKey("id")
                        .build());

        sEnv.executeSql("INSERT INTO events (id, name) VALUES (1, 'hello')").await();
        assertThat(sql("SELECT * FROM events")).containsExactly(Row.of(1, "hello", "HELLO"));
        tEnv.executeSql("UPDATE events SET name = 'changed' WHERE id = 1").await();

        assertThat(sql("SELECT * FROM events"))
                .containsExactly(Row.ofKind(RowKind.UPDATE_AFTER, 1, "changed", "HELLO"));
    }

    @Test
    void testStaticGeneratedPartitionAndOverwrite() throws Exception {
        createTable(
                "events",
                Schema.newBuilder()
                        .column("name", DataTypes.STRING())
                        .generatedColumn("label", DataTypes.STRING(), "UPPER(name)")
                        .generatedColumn("part", DataTypes.STRING(), "LOWER(name)")
                        .partitionKeys("part")
                        .build());

        tEnv.executeSql("INSERT INTO events PARTITION (part = 'fixed') (name) VALUES ('Hello')")
                .await();
        assertThat(sql("SELECT * FROM events")).containsExactly(Row.of("Hello", "HELLO", "fixed"));
        tEnv.executeSql("INSERT OVERWRITE events PARTITION (part = 'fixed') (name) SELECT 'World'")
                .await();
        assertThat(sql("SELECT * FROM events")).containsExactly(Row.of("World", "WORLD", "fixed"));
    }

    @Test
    void testGeneratedNotNullConstraint() throws Exception {
        createTable(
                "events",
                Schema.newBuilder()
                        .column("name", DataTypes.STRING())
                        .generatedColumn("label", DataTypes.STRING().notNull(), "UPPER(name)")
                        .build());

        tEnv.executeSql("INSERT INTO events (name) VALUES ('hello')").await();
        assertThat(sql("SELECT * FROM events")).containsExactly(Row.of("hello", "HELLO"));
        sql("ALTER TABLE events MODIFY label STRING COMMENT 'generated label'");
        assertThat(paimonTable("events").rowType().getField("label").type())
                .isEqualTo(DataTypes.STRING().notNull());
        assertThatThrownBy(
                        () ->
                                tEnv.executeSql(
                                                "INSERT INTO events VALUES ('hello', CAST(NULL AS STRING))")
                                        .await())
                .hasStackTraceContaining("Cannot write null to non-null column(label)");
        assertThatThrownBy(
                        () ->
                                tEnv.executeSql(
                                                "INSERT INTO events (name) VALUES (CAST(NULL AS STRING))")
                                        .await())
                .hasStackTraceContaining("Cannot write null to non-null column(label)");
    }

    @Test
    void testGeneratedPrimaryKey() throws Exception {
        createTable(
                "events",
                Schema.newBuilder()
                        .column("name", DataTypes.STRING())
                        .generatedColumn("id", DataTypes.STRING(), "UPPER(name)")
                        .primaryKey("id")
                        .build());

        tEnv.executeSql("INSERT INTO events (name) VALUES ('hello')").await();
        tEnv.executeSql("INSERT INTO events (name) VALUES ('Hello')").await();
        tEnv.executeSql("INSERT INTO events VALUES ('hello', 'supplied')").await();
        assertThat(sql("SELECT * FROM events"))
                .containsExactlyInAnyOrder(Row.of("Hello", "HELLO"), Row.of("hello", "supplied"));
        assertThat(paimonTable("events").primaryKeys()).containsExactly("id");
        assertThat(paimonTable("events").rowType().getField("id").type().isNullable()).isFalse();
        sql("ALTER TABLE events MODIFY id STRING COMMENT 'generated key'");
        assertThat(paimonTable("events").primaryKeys()).containsExactly("id");
        assertThat(paimonTable("events").rowType().getField("id").type().isNullable()).isFalse();
    }

    @Test
    @Timeout(60)
    void testGeneratedPrimaryKeyStreamingRead() throws Exception {
        createTable(
                "events",
                Schema.newBuilder()
                        .column("name", DataTypes.STRING())
                        .column("score", DataTypes.INT())
                        .generatedColumn("id", DataTypes.STRING(), "UPPER(name)")
                        .primaryKey("id")
                        .build());

        assertThatCode(() -> sEnv.explainSql("SELECT score FROM events WHERE score < 10"))
                .doesNotThrowAnyException();
        tEnv.executeSql("INSERT INTO events (name, score) VALUES ('hello', 1)").await();
        try (BlockingIterator<Row, Row> iterator =
                streamSqlBlockIter("SELECT score FROM events WHERE score < 10")) {
            assertThat(iterator.collect(1, 20, TimeUnit.SECONDS)).containsExactly(Row.of(1));
            tEnv.executeSql("INSERT INTO events (name, score) VALUES ('hello', 20)").await();
            assertThat(iterator.collect(1, 20, TimeUnit.SECONDS))
                    .containsExactly(Row.ofKind(RowKind.UPDATE_BEFORE, 1));
        }
    }

    @Test
    void testUpdatingInsertRetractsOldGeneratedKeyAndPartition() throws Exception {
        createTable(
                "events",
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("cnt", DataTypes.BIGINT())
                        .generatedColumn("dt", DataTypes.STRING(), "CAST(cnt AS STRING)")
                        .partitionKeys("dt")
                        .primaryKey("id", "dt")
                        .build());
        String dataId = TestValuesTableFactory.registerData(Arrays.asList(Row.of(1), Row.of(1)));
        sEnv.executeSql(
                "CREATE TEMPORARY TABLE input_events (id INT) WITH ("
                        + "'connector' = 'values', 'bounded' = 'true', 'data-id' = '"
                        + dataId
                        + "')");

        sEnv.executeSql(
                        "INSERT INTO events (id, cnt) SELECT id, COUNT(*) FROM input_events GROUP BY id")
                .await();

        assertThat(sql("SELECT * FROM events")).containsExactly(Row.of(1, 2L, "2"));
    }

    @ParameterizedTest
    @CsvSource({
        "first-row, false",
        "first-row, true",
        "partial-update, false",
        "partial-update, true"
    })
    void testUpdatingInsertPreservesMergeEngineSemantics(
            String mergeEngine, boolean omitGeneratedKey) throws Exception {
        createTable(
                "events",
                Schema.newBuilder()
                        .column("name", DataTypes.STRING())
                        .column("cnt", DataTypes.BIGINT())
                        .generatedColumn("id", DataTypes.STRING(), "UPPER(name)")
                        .primaryKey("id")
                        .option("merge-engine", mergeEngine)
                        .build());
        String dataId =
                TestValuesTableFactory.registerData(
                        Arrays.asList(Row.of("hello"), Row.of("hello")));
        sEnv.executeSql(
                "CREATE TEMPORARY TABLE input_events (name STRING) WITH ("
                        + "'connector' = 'values', 'bounded' = 'true', 'data-id' = '"
                        + dataId
                        + "')");
        sEnv.executeSql(
                        omitGeneratedKey
                                ? "INSERT INTO events (name, cnt) SELECT name, COUNT(*) FROM input_events GROUP BY name"
                                : "INSERT INTO events SELECT name, COUNT(*), UPPER(name) FROM input_events GROUP BY name")
                .await();

        assertThat(sql("SELECT COUNT(*), MAX(cnt), MAX(id) FROM events"))
                .containsExactly(Row.of(1L, mergeEngine.equals("first-row") ? 1L : 2L, "HELLO"));
    }

    @Test
    void testGeneratedPrimaryKeyWithBlob() throws Exception {
        createTable(
                "events",
                Schema.newBuilder()
                        .column("name", DataTypes.STRING())
                        .column("payload", DataTypes.BLOB())
                        .generatedColumn("id", DataTypes.STRING(), "UPPER(name)")
                        .primaryKey("id")
                        .option("blob-field", "payload")
                        .build());

        assertThatCode(() -> tEnv.explainSql("SELECT name FROM events")).doesNotThrowAnyException();
    }

    @Test
    void testGeneratedPrimaryKeyRejectsExplicitAndGeneratedNull() throws Exception {
        createTable(
                "events",
                Schema.newBuilder()
                        .column("name", DataTypes.STRING())
                        .generatedColumn("id", DataTypes.STRING(), "UPPER(name)")
                        .primaryKey("id")
                        .build());

        assertThatThrownBy(
                        () ->
                                tEnv.executeSql(
                                                "INSERT INTO events VALUES ('hello', CAST(NULL AS STRING))")
                                        .await())
                .isInstanceOf(Exception.class);
        assertThatThrownBy(
                        () ->
                                tEnv.executeSql(
                                                "INSERT INTO events (name) VALUES (CAST(NULL AS STRING))")
                                        .await())
                .isInstanceOf(Exception.class);
        assertThat(sql("SELECT * FROM events")).isEmpty();
    }

    @ParameterizedTest
    @CsvSource({"DATE, false", "STRING, false", "DATE, true", "STRING, true"})
    void testExplicitNullGeneratedPartition(String partitionType, boolean primaryKey)
            throws Exception {
        boolean datePartition = partitionType.equals("DATE");
        Schema.Builder builder =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("event_day", DataTypes.STRING())
                        .generatedColumn(
                                "dt",
                                datePartition ? DataTypes.DATE() : DataTypes.STRING(),
                                datePartition
                                        ? "CAST(event_day AS DATE)"
                                        : "CAST(CAST(event_day AS DATE) AS STRING)")
                        .partitionKeys("dt");
        if (primaryKey) {
            builder.primaryKey("id", "dt").option("primary-key.nullable", "true");
        }
        createTable("events", builder.build());
        tEnv.executeSql("INSERT INTO events (id, event_day) VALUES (1, '2026-11-02')").await();
        tEnv.executeSql(
                        "INSERT INTO events (id, event_day, dt) VALUES (2, 'invalid', CAST(NULL AS "
                                + partitionType
                                + "))")
                .await();
        assertThat(sql("SELECT id, dt FROM events"))
                .containsExactlyInAnyOrder(
                        Row.of(1, datePartition ? LocalDate.of(2026, 11, 2) : "2026-11-02"),
                        Row.of(2, null));
        assertThat(sql("SELECT id FROM events WHERE dt IS NULL")).containsExactly(Row.of(2));
        assertThat(sql("SELECT `partition` FROM events$partitions"))
                .containsExactlyInAnyOrder(
                        Row.of("dt=2026-11-02"), Row.of("dt=__DEFAULT_PARTITION__"));
    }

    @Test
    void testVirtualColumnBeforePhysicalColumns() throws Exception {
        createTable(
                "events",
                Schema.newBuilder()
                        .column("name", DataTypes.STRING())
                        .generatedColumn("label", DataTypes.STRING(), "UPPER(name)")
                        .build());
        sql("ALTER TABLE events ADD virtual_name AS LOWER(name) FIRST");

        tEnv.executeSql("INSERT INTO events (name) VALUES ('Hello')").await();

        assertThat(sql("SELECT name, label, virtual_name FROM events"))
                .containsExactly(Row.of("Hello", "HELLO", "hello"));
    }

    private void createTable(String name, Schema schema) throws Exception {
        flinkCatalog()
                .catalog()
                .createTable(Identifier.create(tEnv.getCurrentDatabase(), name), schema, false);
    }
}
