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

import org.apache.paimon.utils.BlockingIterator;

import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.stream.Stream;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

/** ITCase for deletion vector table. */
public class DeletionVectorITCase extends CatalogITCaseBase {

    private static Stream<Arguments> parameters1() {
        // parameters: changelogProducer, dvBitmap64
        return Stream.of(
                Arguments.of("none", true),
                Arguments.of("none", false),
                Arguments.of("lookup", true),
                Arguments.of("lookup", false));
    }

    private static Stream<Arguments> parameters2() {
        // parameters: changelogProducer, dvVersion
        return Stream.of(Arguments.of("input", true), Arguments.of("input", false));
    }

    @ParameterizedTest
    @MethodSource("parameters2")
    public void testStreamingReadDVTableWhenChangelogProducerIsInput(
            String changelogProducer, boolean dvBitmap64) throws Exception {
        sql(
                String.format(
                        "CREATE TABLE T (id INT PRIMARY KEY NOT ENFORCED, name STRING) "
                                + "WITH ('deletion-vectors.enabled' = 'true', 'changelog-producer' = '%s', "
                                + "'deletion-vectors.bitmap64' = '%s')",
                        changelogProducer, dvBitmap64));

        sql("INSERT INTO T VALUES (1, '111111111'), (2, '2'), (3, '3'), (4, '4')");

        sql("INSERT INTO T VALUES (2, '2_1'), (3, '3_1')");

        sql("INSERT INTO T VALUES (2, '2_2'), (4, '4_1')");

        // test read from APPEND snapshot
        try (BlockingIterator<Row, Row> iter =
                streamSqlBlockIter(
                        "SELECT * FROM T /*+ OPTIONS('scan.mode'='from-snapshot-full','scan.snapshot-id' = '3') */")) {
            assertThat(iter.collect(8))
                    .containsExactlyInAnyOrder(
                            Row.ofKind(RowKind.INSERT, 1, "111111111"),
                            Row.ofKind(RowKind.INSERT, 2, "2"),
                            Row.ofKind(RowKind.INSERT, 3, "3"),
                            Row.ofKind(RowKind.INSERT, 4, "4"),
                            Row.ofKind(RowKind.INSERT, 2, "2_1"),
                            Row.ofKind(RowKind.INSERT, 3, "3_1"),
                            Row.ofKind(RowKind.INSERT, 2, "2_2"),
                            Row.ofKind(RowKind.INSERT, 4, "4_1"));
        }

        // test read from COMPACT snapshot
        try (BlockingIterator<Row, Row> iter =
                streamSqlBlockIter(
                        "SELECT * FROM T /*+ OPTIONS('scan.mode'='from-snapshot-full','scan.snapshot-id' = '4') */")) {
            assertThat(iter.collect(6))
                    .containsExactlyInAnyOrder(
                            Row.ofKind(RowKind.INSERT, 1, "111111111"),
                            Row.ofKind(RowKind.INSERT, 2, "2_1"),
                            Row.ofKind(RowKind.INSERT, 3, "3_1"),
                            Row.ofKind(RowKind.INSERT, 4, "4"),
                            Row.ofKind(RowKind.INSERT, 2, "2_2"),
                            Row.ofKind(RowKind.INSERT, 4, "4_1"));
        }
    }

    @ParameterizedTest
    @MethodSource("parameters1")
    public void testStreamingReadDVTable(String changelogProducer, boolean dvBitmap64)
            throws Exception {
        sql(
                String.format(
                        "CREATE TABLE T (id INT PRIMARY KEY NOT ENFORCED, name STRING) "
                                + "WITH ('deletion-vectors.enabled' = 'true', 'changelog-producer' = '%s', "
                                + "'deletion-vectors.bitmap64' = '%s')",
                        changelogProducer, dvBitmap64));

        sql("INSERT INTO T VALUES (1, '111111111'), (2, '2'), (3, '3'), (4, '4')");

        sql("INSERT INTO T VALUES (2, '2_1'), (3, '3_1')");

        sql("INSERT INTO T VALUES (2, '2_2'), (4, '4_1')");

        // test read from APPEND snapshot
        try (BlockingIterator<Row, Row> iter =
                streamSqlBlockIter(
                        "SELECT * FROM T /*+ OPTIONS('scan.mode'='from-snapshot-full','scan.snapshot-id' = '3') */")) {
            assertThat(iter.collect(12))
                    .containsExactlyInAnyOrder(
                            Row.ofKind(RowKind.INSERT, 1, "111111111"),
                            Row.ofKind(RowKind.INSERT, 2, "2"),
                            Row.ofKind(RowKind.INSERT, 3, "3"),
                            Row.ofKind(RowKind.INSERT, 4, "4"),
                            Row.ofKind(RowKind.UPDATE_BEFORE, 2, "2"),
                            Row.ofKind(RowKind.UPDATE_AFTER, 2, "2_1"),
                            Row.ofKind(RowKind.UPDATE_BEFORE, 3, "3"),
                            Row.ofKind(RowKind.UPDATE_AFTER, 3, "3_1"),
                            Row.ofKind(RowKind.UPDATE_BEFORE, 2, "2_1"),
                            Row.ofKind(RowKind.UPDATE_AFTER, 2, "2_2"),
                            Row.ofKind(RowKind.UPDATE_BEFORE, 4, "4"),
                            Row.ofKind(RowKind.UPDATE_AFTER, 4, "4_1"));
        }

        // test read from COMPACT snapshot
        try (BlockingIterator<Row, Row> iter =
                streamSqlBlockIter(
                        "SELECT * FROM T /*+ OPTIONS('scan.mode'='from-snapshot-full','scan.snapshot-id' = '4') */")) {
            assertThat(iter.collect(8))
                    .containsExactlyInAnyOrder(
                            Row.ofKind(RowKind.INSERT, 1, "111111111"),
                            Row.ofKind(RowKind.INSERT, 2, "2_1"),
                            Row.ofKind(RowKind.INSERT, 3, "3_1"),
                            Row.ofKind(RowKind.INSERT, 4, "4"),
                            Row.ofKind(RowKind.UPDATE_BEFORE, 2, "2_1"),
                            Row.ofKind(RowKind.UPDATE_AFTER, 2, "2_2"),
                            Row.ofKind(RowKind.UPDATE_BEFORE, 4, "4"),
                            Row.ofKind(RowKind.UPDATE_AFTER, 4, "4_1"));
        }
    }

    @ParameterizedTest
    @MethodSource("parameters1")
    public void testBatchReadDVTable(String changelogProducer, boolean dvBitmap64) {
        sql(
                String.format(
                        "CREATE TABLE T (id INT PRIMARY KEY NOT ENFORCED, name STRING) "
                                + "WITH ('deletion-vectors.enabled' = 'true', 'changelog-producer' = '%s', "
                                + "'deletion-vectors.bitmap64' = '%s')",
                        changelogProducer, dvBitmap64));

        sql("INSERT INTO T VALUES (1, '111111111'), (2, '2'), (3, '3'), (4, '4')");

        sql("INSERT INTO T VALUES (2, '2_1'), (3, '3_1')");

        sql("INSERT INTO T VALUES (2, '2_2'), (4, '4_1')");

        assertThat(batchSql("SELECT * FROM T"))
                .containsExactlyInAnyOrder(
                        Row.of(1, "111111111"),
                        Row.of(2, "2_2"),
                        Row.of(3, "3_1"),
                        Row.of(4, "4_1"));

        // batch read dv table will filter level 0 and there will be data delay
        assertThat(batchSql("SELECT * FROM T /*+ OPTIONS('scan.snapshot-id'='3') */"))
                .containsExactlyInAnyOrder(
                        Row.of(1, "111111111"), Row.of(2, "2"), Row.of(3, "3"), Row.of(4, "4"));

        assertThat(batchSql("SELECT * FROM T /*+ OPTIONS('scan.snapshot-id'='4') */"))
                .containsExactlyInAnyOrder(
                        Row.of(1, "111111111"), Row.of(2, "2_1"), Row.of(3, "3_1"), Row.of(4, "4"));
    }

    @ParameterizedTest
    @MethodSource("parameters1")
    public void testDVTableWithAggregationMergeEngine(String changelogProducer, boolean dvBitmap64)
            throws Exception {
        sql(
                String.format(
                        "CREATE TABLE T (id INT PRIMARY KEY NOT ENFORCED, v INT) "
                                + "WITH ('deletion-vectors.enabled' = 'true', 'changelog-producer' = '%s', 'deletion-vectors.bitmap64' = '%s', "
                                + "'merge-engine'='aggregation', 'fields.v.aggregate-function'='sum')",
                        changelogProducer, dvBitmap64));

        sql("INSERT INTO T VALUES (1, 111111111), (2, 2), (3, 3), (4, 4)");

        sql("INSERT INTO T VALUES (2, 1), (3, 1)");

        sql("INSERT INTO T VALUES (2, 1), (4, 1)");

        // test batch read
        assertThat(batchSql("SELECT * FROM T"))
                .containsExactlyInAnyOrder(
                        Row.of(1, 111111111), Row.of(2, 4), Row.of(3, 4), Row.of(4, 5));

        // test streaming read
        if (changelogProducer.equals("lookup")) {
            try (BlockingIterator<Row, Row> iter =
                    streamSqlBlockIter(
                            "SELECT * FROM T /*+ OPTIONS('scan.mode'='from-snapshot-full','scan.snapshot-id' = '4') */")) {
                assertThat(iter.collect(8))
                        .containsExactlyInAnyOrder(
                                Row.ofKind(RowKind.INSERT, 1, 111111111),
                                Row.ofKind(RowKind.INSERT, 2, 3),
                                Row.ofKind(RowKind.INSERT, 3, 4),
                                Row.ofKind(RowKind.INSERT, 4, 4),
                                Row.ofKind(RowKind.UPDATE_BEFORE, 2, 3),
                                Row.ofKind(RowKind.UPDATE_AFTER, 2, 4),
                                Row.ofKind(RowKind.UPDATE_BEFORE, 4, 4),
                                Row.ofKind(RowKind.UPDATE_AFTER, 4, 5));
            }
        }
    }

    @ParameterizedTest
    @MethodSource("parameters1")
    public void testDVTableWithPartialUpdateMergeEngine(
            String changelogProducer, boolean dvBitmap64) throws Exception {
        sql(
                String.format(
                        "CREATE TABLE T (id INT PRIMARY KEY NOT ENFORCED, v1 STRING, v2 STRING) "
                                + "WITH ('deletion-vectors.enabled' = 'true', 'changelog-producer' = '%s', "
                                + "'deletion-vectors.bitmap64' = '%s', 'merge-engine'='partial-update')",
                        changelogProducer, dvBitmap64));

        sql(
                "INSERT INTO T VALUES (1, '111111111', '1'), (2, '2', CAST(NULL AS STRING)), (3, '3', '3'), (4, CAST(NULL AS STRING), '4')");

        sql("INSERT INTO T VALUES (2, CAST(NULL AS STRING), '2'), (3, '3_1', '3_1')");

        sql(
                "INSERT INTO T VALUES (2, '2_1', CAST(NULL AS STRING)), (4, '4', CAST(NULL AS STRING))");

        // test batch read
        assertThat(batchSql("SELECT * FROM T"))
                .containsExactlyInAnyOrder(
                        Row.of(1, "111111111", "1"),
                        Row.of(2, "2_1", "2"),
                        Row.of(3, "3_1", "3_1"),
                        Row.of(4, "4", "4"));

        // test streaming read
        if (changelogProducer.equals("lookup")) {
            try (BlockingIterator<Row, Row> iter =
                    streamSqlBlockIter(
                            "SELECT * FROM T /*+ OPTIONS('scan.mode'='from-snapshot-full','scan.snapshot-id' = '4') */")) {
                assertThat(iter.collect(8))
                        .containsExactlyInAnyOrder(
                                Row.ofKind(RowKind.INSERT, 1, "111111111", "1"),
                                Row.ofKind(RowKind.INSERT, 2, "2", "2"),
                                Row.ofKind(RowKind.INSERT, 3, "3_1", "3_1"),
                                Row.ofKind(RowKind.INSERT, 4, null, "4"),
                                Row.ofKind(RowKind.UPDATE_BEFORE, 2, "2", "2"),
                                Row.ofKind(RowKind.UPDATE_AFTER, 2, "2_1", "2"),
                                Row.ofKind(RowKind.UPDATE_BEFORE, 4, null, "4"),
                                Row.ofKind(RowKind.UPDATE_AFTER, 4, "4", "4"));
            }
        }
    }

    @ParameterizedTest
    @MethodSource("parameters1")
    public void testBatchReadDVTableWithSequenceField(
            String changelogProducer, boolean dvBitmap64) {
        sql(
                String.format(
                        "CREATE TABLE T (id INT PRIMARY KEY NOT ENFORCED, sequence INT, name STRING) "
                                + "WITH ('deletion-vectors.enabled' = 'true', 'sequence.field' = 'sequence', 'changelog-producer' = '%s', "
                                + "'deletion-vectors.bitmap64' = '%s')",
                        changelogProducer, dvBitmap64));

        sql("INSERT INTO T VALUES (1, 1, '1'), (2, 1, '2')");
        sql("INSERT INTO T VALUES (1, 2, '1_1'), (2, 2, '2_1')");
        sql("INSERT INTO T VALUES (1, 3, '1_2'), (2, 1, '2_2')");

        assertThat(batchSql("SELECT * FROM T"))
                .containsExactlyInAnyOrder(Row.of(1, 3, "1_2"), Row.of(2, 2, "2_1"));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testReadTagWithDv(boolean dvBitmap64) {
        sql(
                "CREATE TABLE T (id INT PRIMARY KEY NOT ENFORCED, name STRING) WITH ("
                        + "'deletion-vectors.enabled' = 'true', "
                        + "'deletion-vectors.bitmap64' = '"
                        + dvBitmap64
                        + "', "
                        + "'snapshot.num-retained.min' = '1', "
                        + "'snapshot.num-retained.max' = '1')");

        sql("INSERT INTO T VALUES (1, '1'), (2, '2')");
        sql("CALL sys.create_tag('default.T', 'my_tag')");
        sql("INSERT INTO T VALUES (3, '3'), (4, '4')");

        assertThat(batchSql("SELECT * FROM T /*+ OPTIONS('scan.tag-name'='my_tag') */"))
                .containsExactlyInAnyOrder(Row.of(1, "1"), Row.of(2, "2"));
    }

    @Test
    public void testChangeToDv64() throws Exception {
        sql(
                "CREATE TABLE T (id INT PRIMARY KEY NOT ENFORCED, name STRING) "
                        + "WITH ('deletion-vectors.enabled' = 'true', 'changelog-producer' = 'lookup', "
                        + "'deletion-vectors.bitmap64' = 'false', 'bucket' = '1')");

        sql("INSERT INTO T VALUES (1, '1'), (2, '2'), (3, '3'), (4, '4')");
        sql("INSERT INTO T VALUES (2, '2_1'), (3, '3_1')");
        sql("INSERT INTO T VALUES (5, '5'), (6, '6'), (7, '8')");

        // change dv to bitmap64
        sql("ALTER TABLE T SET('deletion-vectors.bitmap64' = 'true')");
        sql("INSERT INTO T VALUES (2, '2_2'),(6, '6_1'), (7, '7_1')");

        assertThat(batchSql("SELECT * FROM T"))
                .containsExactlyInAnyOrder(
                        Row.of(1, "1"),
                        Row.of(2, "2_2"),
                        Row.of(3, "3_1"),
                        Row.of(4, "4"),
                        Row.of(5, "5"),
                        Row.of(6, "6_1"),
                        Row.of(7, "7_1"));

        assertThat(batchSql("SELECT * FROM T /*+ OPTIONS('scan.snapshot-id'='4') */"))
                .containsExactlyInAnyOrder(
                        Row.of(1, "1"), Row.of(2, "2_1"), Row.of(3, "3_1"), Row.of(4, "4"));
    }

    @Test
    public void testRemoveDvsAfterFullCompaction() throws Exception {
        sql(
                "CREATE TABLE T (id INT PRIMARY KEY NOT ENFORCED, name STRING) "
                        + "WITH ('deletion-vectors.enabled' = 'true', 'changelog-producer' = 'lookup', "
                        + "'bucket' = '1')");

        // one small file in level 5
        sql("INSERT INTO T VALUES (1, '1'), (2, '2'), (3, '3'), (4, '4')");
        sql("DELETE FROM T WHERE id=1");
        assertThat(sql("SELECT * FROM T").size()).isEqualTo(3);

        // full compact
        tEnv.getConfig().set("table.dml-sync", "true");
        sql("CALL sys.compact(`table` => 'default.T')");
        // disable dv and select
        sql("ALTER TABLE T SET('deletion-vectors.enabled' = 'false')");
        assertThat(sql("SELECT * FROM T").size()).isEqualTo(3);

        // ***************** another table ******************
        sql(
                "CREATE TABLE TT (id INT PRIMARY KEY NOT ENFORCED, name STRING) "
                        + "WITH ('deletion-vectors.enabled' = 'true', 'changelog-producer' = 'lookup', "
                        + "'target-file-size' = '1000 B', 'bucket' = '1')");

        // two large files in level 5
        sql("INSERT INTO TT VALUES (1, '1'), (2, '2'), (3, '3'), (4, '4')");
        sql("INSERT INTO TT VALUES (5, '5'), (6, '6'), (7, '7')");
        sql("CALL sys.compact(`table` => 'default.TT')");

        sql("DELETE FROM TT WHERE id = 1");
        sql("DELETE FROM TT WHERE id = 7");
        assertThat(sql("SELECT * FROM TT").size()).isEqualTo(5);

        // full compact
        sql("CALL sys.compact(`table` => 'default.TT')");
        // disable dv and select
        sql("ALTER TABLE TT SET('deletion-vectors.enabled' = 'false')");
        assertThat(sql("SELECT * FROM TT").size()).isEqualTo(5);
    }
}
