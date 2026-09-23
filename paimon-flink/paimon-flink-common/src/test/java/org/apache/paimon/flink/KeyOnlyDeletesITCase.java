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

import org.apache.paimon.shade.guava30.com.google.common.collect.ImmutableList;

import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * ITCase for FLIP-510 key-only (partial) deletes on a primary-key table with {@code merge-engine =
 * deduplicate}, {@code changelog-producer = none} and {@code sink.key-only-deletes.enabled = true}.
 *
 * <p>Verifies data integrity end-to-end when the upstream source emits:
 *
 * <ul>
 *   <li>updates as {@code +U} only (no {@code -U}/UPDATE_BEFORE), and
 *   <li>deletes carrying only the primary key ({@code -D} with the value columns null).
 * </ul>
 *
 * <p>The delete-by-key and update-without-before shapes are guaranteed by how the input is built (a
 * {@code values} source with {@code changelog-mode = 'I,UA,D'} and DELETE rows populated only on
 * the key). Enabling {@code sink.key-only-deletes.enabled} lets the Flink planner drop the upstream
 * ChangelogNormalize so those records reach the Paimon sink as-is. The API backing this capability
 * only exists in Flink 2.1+, so the test is gated on that.
 */
public class KeyOnlyDeletesITCase extends CatalogITCaseBase {

    /**
     * Streams +I / +U (no -U) / key-only -D over two keys:
     *
     * <ul>
     *   <li>k1: +I (1,1) → +U (1,2) → -D (1) — deleted, must be gone;
     *   <li>k2: +I (2,20) → -D (2) → +I (2,21) — deleted then re-inserted, latest value kept.
     * </ul>
     *
     * <p>Final state must be exactly (2,21), proving the key-only delete removes the row and leaves
     * no tombstone stuck to the key on re-insert.
     */
    @Test
    @EnabledIf("isFlink2_1OrAbove")
    public void testKeyOnlyDeletesIntegrity() throws Exception {
        sql(
                "CREATE TABLE T (k INT PRIMARY KEY NOT ENFORCED, v INT) WITH ("
                        + " 'merge-engine' = 'deduplicate',"
                        + " 'changelog-producer' = 'none',"
                        + " 'sink.key-only-deletes.enabled' = 'true')");

        List<Row> input =
                ImmutableList.of(
                        Row.ofKind(RowKind.INSERT, 1, 1), // +I (k1, v1)
                        Row.ofKind(RowKind.UPDATE_AFTER, 1, 2), // +U (k1, v2), no -U
                        Row.ofKind(RowKind.DELETE, 1, null), // -D (k1), key-only → gone
                        Row.ofKind(RowKind.INSERT, 2, 20), // +I (k2, v20)
                        Row.ofKind(RowKind.DELETE, 2, null), // -D (k2), key-only
                        Row.ofKind(RowKind.INSERT, 2, 21)); // +I (k2, v21), re-insert

        String id = TestValuesTableFactory.registerData(input);
        streamSqlIter(
                        "CREATE TEMPORARY TABLE input (k INT PRIMARY KEY NOT ENFORCED, v INT) WITH ("
                                + " 'connector' = 'values',"
                                + " 'bounded' = 'true',"
                                + " 'data-id' = '%s',"
                                + " 'changelog-mode' = 'I,UA,D')",
                        id)
                .close();
        sEnv.executeSql("INSERT INTO T SELECT * FROM input").await();

        assertThat(sql("SELECT * FROM T")).containsExactlyInAnyOrder(Row.of(2, 21));
    }

    private static Stream<Arguments> reconstructParameters() {
        // parameters: mergeEngine, changelogProducer
        return Stream.of(
                Arguments.of("deduplicate", "lookup"),
                Arguments.of("deduplicate", "full-compaction"),
                Arguments.of("partial-update", "lookup"),
                Arguments.of("partial-update", "full-compaction"));
    }

    /**
     * Same key-only-delete / no-before-update write path as {@link #testKeyOnlyDeletesIntegrity()},
     * but with a changelog-producer ({@code lookup} / {@code full-compaction}) that materializes a
     * changelog. Uses a partial schema (k, a, b) so partial-update exercises real column merging.
     *
     * <p>Input over key 1: {@code +I (1,1,1)} → {@code +U (1,2,1)} (no {@code -U}) → {@code -D (1)}
     * (key-only, value columns null). Each phase is written in its own commit (multiple changes to
     * the same key within a single commit would be merged before the producer runs, so the
     * intermediate transition would not surface). Two verifications:
     *
     * <ul>
     *   <li>Batch read: final state is empty (the key-only delete removed the row).
     *   <li>Streaming read: even though the write was key-only / before-less, the producer
     *       reconstructs a full changelog — the update surfaces as a {@code -U}/{@code +U} pair
     *       carrying the before-image, and the delete surfaces as a {@code -D} with the full row
     *       (all columns), not just the key.
     * </ul>
     */
    @ParameterizedTest(name = "mergeEngine={0}, changelogProducer={1}")
    @MethodSource("reconstructParameters")
    @EnabledIf("isFlink2_1OrAbove")
    public void testKeyOnlyDeletesReconstructChangelog(String mergeEngine, String changelogProducer)
            throws Exception {
        sql(
                "CREATE TABLE R (k INT PRIMARY KEY NOT ENFORCED, a INT, b INT) WITH ("
                        + " 'merge-engine' = '%s',"
                        + " 'partial-update.remove-record-on-delete' = 'true',"
                        + " 'changelog-producer' = '%s',"
                        + " 'changelog-producer.compaction-interval' = '1s',"
                        + " 'sink.key-only-deletes.enabled' = 'true')",
                mergeEngine, changelogProducer);

        // Open the streaming changelog read before writing so we observe the reconstructed stream.
        BlockingIterator<Row, Row> iterator = BlockingIterator.of(streamSqlIter("SELECT * FROM R"));

        // Commit 1: insert.
        streamInto(ImmutableList.of(Row.ofKind(RowKind.INSERT, 1, 1, 1))); // +I (1, a=1, b=1)
        assertThat(iterator.collect(1)).containsExactly(Row.ofKind(RowKind.INSERT, 1, 1, 1));

        // Commit 2: update as +U only (no -U); reconstructed as a -U/+U pair with the before-image.
        streamInto(ImmutableList.of(Row.ofKind(RowKind.UPDATE_AFTER, 1, 2, 1))); // +U (1, a=2)
        assertThat(iterator.collect(2))
                .containsExactlyInAnyOrder(
                        Row.ofKind(RowKind.UPDATE_BEFORE, 1, 1, 1),
                        Row.ofKind(RowKind.UPDATE_AFTER, 1, 2, 1));

        // Commit 3: key-only delete (only the primary key is populated).
        streamInto(ImmutableList.of(Row.ofKind(RowKind.DELETE, 1, null, null)));

        // The reconstructed delete carries the full row (all columns), not just the key.
        assertThat(iterator.collect(1)).containsExactly(Row.ofKind(RowKind.DELETE, 1, 2, 1));
        iterator.close();

        // Batch read: the key-only delete removed the row.
        assertThat(sql("SELECT * FROM R")).isEmpty();
    }

    /**
     * Streams {@code rows} into table {@code R} through a bounded {@code values} changelog source
     * that carries only INSERT / UPDATE_AFTER / DELETE (no UPDATE_BEFORE).
     */
    private void streamInto(List<Row> rows) throws Exception {
        String id = TestValuesTableFactory.registerData(rows);
        streamSqlIter(
                        "CREATE TEMPORARY TABLE src_%s (k INT PRIMARY KEY NOT ENFORCED, a INT, b INT) WITH ("
                                + " 'connector' = 'values',"
                                + " 'bounded' = 'true',"
                                + " 'data-id' = '%s',"
                                + " 'changelog-mode' = 'I,UA,D')",
                        id, id)
                .close();
        sEnv.executeSql(String.format("INSERT INTO R SELECT * FROM src_%s", id)).await();
    }
}
