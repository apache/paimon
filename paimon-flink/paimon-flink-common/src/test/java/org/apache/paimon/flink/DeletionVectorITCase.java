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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

/** ITCase for deletion vector table. */
public class DeletionVectorITCase extends CatalogITCaseBase {

    @ParameterizedTest
    @ValueSource(strings = {"input"})
    public void testStreamingReadDVTableWhenChangelogProducerIsInput(String changelogProducer)
            throws Exception {
        sql(
                String.format(
                        "CREATE TABLE T (id INT PRIMARY KEY NOT ENFORCED, name STRING) "
                                + "WITH ('deletion-vectors.enabled' = 'true', 'changelog-producer' = '%s')",
                        changelogProducer));

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
    @ValueSource(strings = {"none", "lookup"})
    public void testStreamingReadDVTable(String changelogProducer) throws Exception {
        sql(
                String.format(
                        "CREATE TABLE T (id INT PRIMARY KEY NOT ENFORCED, name STRING) "
                                + "WITH ('deletion-vectors.enabled' = 'true', 'changelog-producer' = '%s')",
                        changelogProducer));

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
    @ValueSource(strings = {"none", "lookup"})
    public void testBatchReadDVTable(String changelogProducer) {
        sql(
                String.format(
                        "CREATE TABLE T (id INT PRIMARY KEY NOT ENFORCED, name STRING) "
                                + "WITH ('deletion-vectors.enabled' = 'true', 'changelog-producer' = '%s')",
                        changelogProducer));

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
    @ValueSource(strings = {"none", "lookup"})
    public void testDVTableWithAggregationMergeEngine(String changelogProducer) throws Exception {
        sql(
                String.format(
                        "CREATE TABLE T (id INT PRIMARY KEY NOT ENFORCED, v INT) "
                                + "WITH ('deletion-vectors.enabled' = 'true', 'changelog-producer' = '%s', "
                                + "'merge-engine'='aggregation', 'fields.v.aggregate-function'='sum')",
                        changelogProducer));

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
    @ValueSource(strings = {"none", "lookup"})
    public void testDVTableWithPartialUpdateMergeEngine(String changelogProducer) throws Exception {
        sql(
                String.format(
                        "CREATE TABLE T (id INT PRIMARY KEY NOT ENFORCED, v1 STRING, v2 STRING) "
                                + "WITH ('deletion-vectors.enabled' = 'true', 'changelog-producer' = '%s', "
                                + "'merge-engine'='partial-update')",
                        changelogProducer));

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
}
