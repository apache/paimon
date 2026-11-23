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

package org.apache.paimon.table;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.sink.StreamTableCommit;
import org.apache.paimon.table.sink.StreamTableWrite;
import org.apache.paimon.types.DataTypes;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.SoftAssertions.assertSoftly;

/** Tests for distinct listagg tokens across primary-key writes and compaction. */
class PrimaryKeyListaggDistinctTest extends TableTestBase {

    static Stream<Arguments> listaggCases() {
        return Stream.of(false, true)
                .flatMap(
                        partial ->
                                Stream.of(false, true)
                                        .flatMap(
                                                separate ->
                                                        Stream.of(",", "::")
                                                                .map(
                                                                        delimiter ->
                                                                                Arguments.of(
                                                                                        partial,
                                                                                        separate,
                                                                                        delimiter))));
    }

    @ParameterizedTest
    @MethodSource("listaggCases")
    void testDistinctListaggFirstInput(
            boolean partialUpdate, boolean separateCommits, String delimiter) throws Exception {
        FileStoreTable table = createTable(partialUpdate, delimiter, true);
        try (StreamTableWrite write = table.newWrite("contract");
                StreamTableCommit commit = table.newCommit("contract")) {
            write.write(GenericRow.of(1, BinaryString.fromString("a" + delimiter + "a"), 1L));
            if (separateCommits) {
                commit.commit(0, write.prepareCommit(false, 0));
            }
            write.write(GenericRow.of(1, BinaryString.fromString("b"), 2L));
            commit.commit(1, write.prepareCommit(false, 1));
            List<String> before =
                    read(table).stream()
                            .map(row -> row.getString(1).toString())
                            .collect(Collectors.toList());
            write.compact(BinaryRow.EMPTY_ROW, 0, true);
            commit.commit(2, write.prepareCommit(true, 2));
            List<String> after =
                    read(table).stream()
                            .map(row -> row.getString(1).toString())
                            .collect(Collectors.toList());
            assertSoftly(
                    softly -> {
                        softly.assertThat(before).containsExactly("a" + delimiter + "b");
                        softly.assertThat(after).containsExactly("a" + delimiter + "b");
                    });
        }
    }

    static Stream<Arguments> reversedCases() {
        return Stream.of(
                Arguments.of(false, false),
                Arguments.of(false, true),
                Arguments.of(true, false),
                Arguments.of(true, true));
    }

    @ParameterizedTest
    @MethodSource("reversedCases")
    void testReversedSequenceGroup(boolean separateCommits, boolean firstNull) throws Exception {
        FileStoreTable table = createTable(true, ",", true);
        try (StreamTableWrite write = table.newWrite("contract");
                StreamTableCommit commit = table.newCommit("contract")) {
            write.write(GenericRow.of(1, firstNull ? null : BinaryString.fromString("b"), 2L));
            if (separateCommits) {
                commit.commit(0, write.prepareCommit(false, 0));
            }
            write.write(GenericRow.of(1, BinaryString.fromString("a,a"), 1L));
            commit.commit(1, write.prepareCommit(false, 1));
            List<String> before =
                    read(table).stream()
                            .map(row -> row.getString(1).toString())
                            .collect(Collectors.toList());
            write.compact(BinaryRow.EMPTY_ROW, 0, true);
            commit.commit(2, write.prepareCommit(true, 2));
            List<String> after =
                    read(table).stream()
                            .map(row -> row.getString(1).toString())
                            .collect(Collectors.toList());
            String expected = firstNull ? "a" : "a,b";
            assertSoftly(
                    softly -> {
                        softly.assertThat(before).containsExactly(expected);
                        softly.assertThat(after).containsExactly(expected);
                    });
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testNonDistinctPreservesTokens(boolean partialUpdate) throws Exception {
        FileStoreTable table = createTable(partialUpdate, ",", false);
        try (StreamTableWrite write = table.newWrite("contract");
                StreamTableCommit commit = table.newCommit("contract")) {
            write.write(GenericRow.of(1, BinaryString.fromString("a,a"), 1L));
            write.write(GenericRow.of(1, BinaryString.fromString("b"), 2L));
            commit.commit(0, write.prepareCommit(false, 0));
            assertThat(read(table))
                    .extracting(row -> row.getString(1).toString())
                    .containsExactly("a,a,b");
        }
    }

    private FileStoreTable createTable(boolean partialUpdate, String delimiter, boolean distinct)
            throws Exception {
        Schema.Builder schema =
                Schema.newBuilder()
                        .column("k", DataTypes.INT())
                        .column("v", DataTypes.STRING())
                        .column("seq", DataTypes.BIGINT())
                        .primaryKey("k")
                        .option(CoreOptions.BUCKET.key(), "1")
                        .option("merge-engine", partialUpdate ? "partial-update" : "aggregation")
                        .option("fields.v.aggregate-function", "listagg")
                        .option("fields.v.distinct", String.valueOf(distinct))
                        .option("fields.v.list-agg-delimiter", delimiter)
                        .option(CoreOptions.NUM_SORTED_RUNS_COMPACTION_TRIGGER.key(), "99");
        if (partialUpdate) {
            schema.option("fields.seq.sequence-group", "v");
        } else {
            schema.option("fields.seq.aggregate-function", "max");
        }
        catalog.createTable(identifier(), schema.build(), false);
        return (FileStoreTable) catalog.getTable(identifier());
    }

    static Stream<Arguments> emptyDelimiterCases() {
        return Stream.of(
                Arguments.of(false, false),
                Arguments.of(false, true),
                Arguments.of(true, false),
                Arguments.of(true, true));
    }

    @ParameterizedTest
    @MethodSource("emptyDelimiterCases")
    void testEmptyDelimiterPreservesTokens(boolean partialUpdate, boolean separateCommits)
            throws Exception {
        FileStoreTable table = createTable(partialUpdate, "", true);
        try (StreamTableWrite write = table.newWrite("contract");
                StreamTableCommit commit = table.newCommit("contract")) {
            write.write(GenericRow.of(1, BinaryString.fromString("a b"), 1L));
            if (separateCommits) {
                commit.commit(0, write.prepareCommit(false, 0));
            }
            write.write(GenericRow.of(1, BinaryString.fromString("b"), 2L));
            commit.commit(1, write.prepareCommit(false, 1));
            List<String> before =
                    read(table).stream()
                            .map(row -> row.getString(1).toString())
                            .collect(Collectors.toList());
            write.compact(BinaryRow.EMPTY_ROW, 0, true);
            commit.commit(2, write.prepareCommit(true, 2));
            List<String> after =
                    read(table).stream()
                            .map(row -> row.getString(1).toString())
                            .collect(Collectors.toList());
            assertSoftly(
                    softly -> {
                        softly.assertThat(before).containsExactly("a b");
                        softly.assertThat(after).containsExactly("a b");
                    });
        }
    }
}
