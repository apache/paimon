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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.utils.BlockingIterator;

import org.apache.flink.table.api.config.TableConfigOptions;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.Arrays;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** IT cases for nullable primary-key tables. */
@Timeout(180)
public class NullablePrimaryKeyITCase extends CatalogITCaseBase {

    private void createTable(
            String tableName, String fields, String primaryKey, String... additionalOptions) {
        String options =
                Arrays.stream(additionalOptions)
                        .map(option -> ", " + option)
                        .collect(Collectors.joining());
        sql(
                "CREATE TABLE %s (%s) WITH ("
                        + "'primary-key' = '%s', "
                        + "'primary-key.nullable' = 'true', "
                        + "'bucket' = '1'%s)",
                tableName, fields, primaryKey, options);
    }

    @Test
    public void testCatalogKeepsNullablePrimaryKeyAsTableOption() throws Exception {
        createTable("T", "id INT, v STRING", "id");

        DataCatalogTable catalogTable = (DataCatalogTable) table("T");
        assertThat(catalogTable.getUnresolvedSchema().getPrimaryKey()).isEmpty();
        assertThat(catalogTable.getOptions())
                .containsEntry(CoreOptions.PRIMARY_KEY.key(), "id")
                .containsEntry(CoreOptions.PRIMARY_KEY_NULLABLE.key(), "true");
        assertThat(catalogTable.table().primaryKeys()).containsExactly("id");
        assertThat(catalogTable.table().rowType().getTypeAt(0).isNullable()).isTrue();
    }

    @Test
    public void testSingleNullablePrimaryKeyDeduplicateAcrossCommits() {
        createTable("T", "id INT, v STRING", "id");

        sql("INSERT INTO T VALUES (CAST(NULL AS INT), 'null-v1'), (1, 'one-v1')");
        sql("INSERT INTO T VALUES " + "(CAST(NULL AS INT), 'null-v2'), (1, 'one-v2'), (2, 'two')");

        assertThat(sql("SELECT * FROM T"))
                .containsExactlyInAnyOrder(
                        Row.of(null, "null-v2"), Row.of(1, "one-v2"), Row.of(2, "two"));
        assertThat(sql("SELECT v FROM T WHERE id IS NULL")).containsExactly(Row.of("null-v2"));
    }

    @Test
    public void testDuplicateNullablePrimaryKeyWithinSingleCommit() {
        createTable("T", "id INT, v STRING", "id");

        sql(
                "INSERT INTO T VALUES "
                        + "(CAST(NULL AS INT), 'null-v1'), "
                        + "(CAST(NULL AS INT), 'null-v2'), "
                        + "(1, 'one')");

        assertThat(sql("SELECT * FROM T"))
                .hasSize(2)
                .contains(Row.of(1, "one"))
                .anySatisfy(
                        row -> {
                            assertThat(row.getField(0)).isNull();
                            assertThat(row.getField(1)).isIn("null-v1", "null-v2");
                        });
    }

    @Test
    public void testCompositeNullablePrimaryKeyDeduplicateAcrossCommits() {
        createTable("T", "k1 INT, k2 INT, v STRING", "k1,k2");

        sql(
                "INSERT INTO T VALUES "
                        + "(CAST(NULL AS INT), CAST(NULL AS INT), 'nn-v1'), "
                        + "(CAST(NULL AS INT), 1, 'n1-v1'), "
                        + "(1, CAST(NULL AS INT), '1n-v1'), "
                        + "(1, 1, '11-v1')");
        sql(
                "INSERT INTO T VALUES "
                        + "(CAST(NULL AS INT), CAST(NULL AS INT), 'nn-v2'), "
                        + "(CAST(NULL AS INT), 1, 'n1-v2'), "
                        + "(1, CAST(NULL AS INT), '1n-v2'), "
                        + "(1, 1, '11-v2')");

        assertThat(sql("SELECT * FROM T"))
                .containsExactlyInAnyOrder(
                        Row.of(null, null, "nn-v2"),
                        Row.of(null, 1, "n1-v2"),
                        Row.of(1, null, "1n-v2"),
                        Row.of(1, 1, "11-v2"));
    }

    @Test
    public void testPartitionedNullablePrimaryKey() {
        createTable("T", "dt STRING, id INT, v STRING", "dt,id", "'partition' = 'dt'");

        sql(
                "INSERT INTO T VALUES "
                        + "(CAST(NULL AS STRING), 1, 'null-partition-v1'), "
                        + "('A', CAST(NULL AS INT), 'a-null-v1')");
        sql(
                "INSERT INTO T VALUES "
                        + "(CAST(NULL AS STRING), 1, 'null-partition-v2'), "
                        + "('A', CAST(NULL AS INT), 'a-null-v2'), "
                        + "('B', 2, 'b-two')");

        assertThat(sql("SELECT * FROM T"))
                .containsExactlyInAnyOrder(
                        Row.of(null, 1, "null-partition-v2"),
                        Row.of("A", null, "a-null-v2"),
                        Row.of("B", 2, "b-two"));
        assertThat(sql("SELECT id, v FROM T WHERE dt IS NULL"))
                .containsExactly(Row.of(1, "null-partition-v2"));
    }

    @Test
    public void testSequenceFieldWithNullablePrimaryKey() {
        createTable("T", "id INT, v STRING, seq BIGINT", "id", "'sequence.field' = 'seq'");

        sql("INSERT INTO T VALUES (CAST(NULL AS INT), 'newer', 2)");
        sql("INSERT INTO T VALUES (CAST(NULL AS INT), 'older', 1), (1, 'one', 1)");

        assertThat(sql("SELECT * FROM T"))
                .containsExactlyInAnyOrder(Row.of(null, "newer", 2L), Row.of(1, "one", 1L));
    }

    @Test
    public void testPartialUpdateWithNullablePrimaryKey() {
        createTable("T", "id INT, a INT, b STRING", "id", "'merge-engine' = 'partial-update'");

        sql(
                "INSERT INTO T VALUES "
                        + "(CAST(NULL AS INT), 1, CAST(NULL AS STRING)), "
                        + "(1, 10, 'one')");
        sql("INSERT INTO T VALUES " + "(CAST(NULL AS INT), CAST(NULL AS INT), 'merged')");

        assertThat(sql("SELECT * FROM T"))
                .containsExactlyInAnyOrder(Row.of(null, 1, "merged"), Row.of(1, 10, "one"));
    }

    @Test
    public void testAggregationWithNullablePrimaryKey() {
        createTable(
                "T",
                "id INT, total BIGINT",
                "id",
                "'merge-engine' = 'aggregation'",
                "'fields.total.aggregate-function' = 'sum'");

        sql("INSERT INTO T VALUES (CAST(NULL AS INT), 1), (1, 10)");
        sql("INSERT INTO T VALUES (CAST(NULL AS INT), 2), (1, 20)");

        assertThat(sql("SELECT * FROM T"))
                .containsExactlyInAnyOrder(Row.of(null, 3L), Row.of(1, 30L));
    }

    @Test
    public void testFirstRowWithNullablePrimaryKey() {
        createTable("T", "id INT, v STRING", "id", "'merge-engine' = 'first-row'");

        sql("INSERT INTO T VALUES (CAST(NULL AS INT), 'first'), (1, 'one-first')");
        sql("INSERT INTO T VALUES (CAST(NULL AS INT), 'ignored'), (1, 'one-ignored')");

        assertThat(sql("SELECT * FROM T"))
                .containsExactlyInAnyOrder(Row.of(null, "first"), Row.of(1, "one-first"));
    }

    @Test
    public void testSchemaEvolutionPreservesNullablePrimaryKey() throws Exception {
        createTable("T", "id INT, v STRING", "id");
        sql("INSERT INTO T VALUES (CAST(NULL AS INT), 'before')");

        sql("ALTER TABLE T ADD extra INT");
        sql("INSERT INTO T VALUES (CAST(NULL AS INT), 'after', 2)");

        assertThat(sql("SELECT * FROM T")).containsExactly(Row.of(null, "after", 2));
        assertThat(((DataCatalogTable) table("T")).table().rowType().getTypeAt(0).isNullable())
                .isTrue();
    }

    @Test
    public void testUpdateAndDeleteNullablePrimaryKey() {
        createTable("T", "id INT, v STRING", "id");
        sql("INSERT INTO T VALUES " + "(CAST(NULL AS INT), 'null-v1'), (1, 'one'), (2, 'two')");

        sql("UPDATE T SET v = 'null-v2' WHERE id IS NULL");
        assertThat(sql("SELECT * FROM T WHERE id IS NULL"))
                .containsExactly(Row.of(null, "null-v2"));

        sql("DELETE FROM T WHERE v = 'null-v2' OR id = 1");
        assertThat(sql("SELECT * FROM T")).containsExactly(Row.of(2, "two"));
    }

    @Test
    public void testCompactionPreservesNullablePrimaryKey() {
        createTable(
                "T",
                "id INT, v STRING",
                "id",
                "'write-only' = 'true'",
                "'num-sorted-run.compaction-trigger' = '10'");

        sql("INSERT INTO T VALUES (CAST(NULL AS INT), 'null-v1'), (1, 'one-v1')");
        sql("INSERT INTO T VALUES (CAST(NULL AS INT), 'null-v2'), (1, 'one-v2')");
        sql("INSERT INTO T VALUES (CAST(NULL AS INT), 'null-v3'), (2, 'two')");
        tEnv.getConfig().set(TableConfigOptions.TABLE_DML_SYNC, true);
        sql("CALL sys.compact(`table` => 'default.T', compact_strategy => 'full')");

        assertThat(findLatestSnapshot("T").commitKind()).isEqualTo(Snapshot.CommitKind.COMPACT);
        assertThat(sql("SELECT * FROM T"))
                .containsExactlyInAnyOrder(
                        Row.of(null, "null-v3"), Row.of(1, "one-v2"), Row.of(2, "two"));
    }

    @Test
    public void testNullablePrimaryKeyOptionIsImmutableAfterWrite() {
        createTable("T", "id INT, v STRING", "id");
        sql("INSERT INTO T VALUES (CAST(NULL AS INT), 'null-value')");

        assertThatThrownBy(() -> sql("ALTER TABLE T SET (" + "'primary-key.nullable' = 'false')"))
                .hasStackTraceContaining("Change 'primary-key.nullable' is not supported yet.");
        assertThat(sql("SELECT * FROM T")).containsExactly(Row.of(null, "null-value"));
    }

    @Test
    public void testStreamingReadRequiresFullChangelog() {
        createTable("T_DEFAULT", "id INT, v STRING", "id");
        createTable("T_INPUT", "id INT, v STRING", "id", "'changelog-producer' = 'input'");

        assertThatThrownBy(() -> sEnv.explainSql("SELECT * FROM T_DEFAULT"))
                .hasStackTraceContaining("nullable primary keys require a full changelog");
        assertThatCode(() -> sEnv.explainSql("SELECT * FROM T_INPUT")).doesNotThrowAnyException();
    }

    @Test
    public void testLookupChangelogStreamingReadWithNullablePrimaryKey() throws Exception {
        createTable(
                "T",
                "id INT, v STRING",
                "id",
                "'changelog-producer' = 'lookup'",
                "'continuous.discovery-interval' = '100ms'");

        try (BlockingIterator<Row, Row> iterator = streamSqlBlockIter("SELECT * FROM T")) {
            sql("INSERT INTO T VALUES (CAST(NULL AS INT), 'null-v1'), (1, 'one')");
            assertThat(iterator.collect(2))
                    .containsExactlyInAnyOrder(Row.of(null, "null-v1"), Row.of(1, "one"));

            sql("INSERT INTO T VALUES (CAST(NULL AS INT), 'null-v2')");
            assertThat(iterator.collect(2))
                    .containsExactlyInAnyOrder(
                            Row.ofKind(RowKind.UPDATE_BEFORE, null, "null-v1"),
                            Row.ofKind(RowKind.UPDATE_AFTER, null, "null-v2"));
        }
    }

    @Test
    public void testInputChangelogStreamingReadWithNullablePrimaryKey() throws Exception {
        createTable(
                "T",
                "id INT, v STRING",
                "id",
                "'changelog-producer' = 'input'",
                "'continuous.discovery-interval' = '100ms'");

        try (BlockingIterator<Row, Row> iterator = streamSqlBlockIter("SELECT * FROM T")) {
            sql("INSERT INTO T VALUES (CAST(NULL AS INT), 'null-v1')");
            assertThat(iterator.collect(1)).containsExactly(Row.of(null, "null-v1"));

            sql("INSERT INTO T VALUES (CAST(NULL AS INT), 'null-v2')");
            assertThat(iterator.collect(1)).containsExactly(Row.of(null, "null-v2"));
        }
    }

    @Test
    public void testFullCompactionStreamingReadWithNullablePrimaryKey() throws Exception {
        createTable(
                "T",
                "id INT, v STRING",
                "id",
                "'changelog-producer' = 'full-compaction'",
                "'changelog-producer.compaction-interval' = '1s'",
                "'continuous.discovery-interval' = '100ms'");

        try (BlockingIterator<Row, Row> iterator = streamSqlBlockIter("SELECT * FROM T")) {
            sql("INSERT INTO T VALUES (CAST(NULL AS INT), 'null-v1'), (1, 'one')");
            assertThat(iterator.collect(2))
                    .containsExactlyInAnyOrder(Row.of(null, "null-v1"), Row.of(1, "one"));

            sql("INSERT INTO T VALUES (CAST(NULL AS INT), 'null-v2')");
            assertThat(iterator.collect(2))
                    .containsExactlyInAnyOrder(
                            Row.ofKind(RowKind.UPDATE_BEFORE, null, "null-v1"),
                            Row.ofKind(RowKind.UPDATE_AFTER, null, "null-v2"));
        }
    }
}
