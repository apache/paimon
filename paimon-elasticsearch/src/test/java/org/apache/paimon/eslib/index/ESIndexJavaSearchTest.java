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

package org.apache.paimon.eslib.index;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.globalindex.EsSearchQuery;
import org.apache.paimon.globalindex.GlobalIndexIOMeta;
import org.apache.paimon.globalindex.GlobalIndexResult;
import org.apache.paimon.globalindex.ResultEntry;
import org.apache.paimon.globalindex.ScoredGlobalIndexResult;
import org.apache.paimon.globalindex.io.GlobalIndexFileWriter;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.FieldRef;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Java archives support ES DSL and existing search APIs with persisted field mappings. */
class ESIndexJavaSearchTest {
    @TempDir Path directory;

    @Test
    void defaultJavaArchiveSupportsDslAndLegacyReaders() throws Exception {
        List<DataField> fields =
                List.of(
                        new DataField(0, "id", DataTypes.BIGINT()),
                        new DataField(1, "text", DataTypes.STRING()));
        ESIndexOptions options = new ESIndexOptions(fields, new Options());
        ResultEntry entry;
        try (ESIndexGlobalIndexWriter writer = writer(fields, options)) {
            writer.writeRow(0, GenericRow.of(Long.MIN_VALUE, BinaryString.fromString("blue lake")));
            writer.writeRow(1, GenericRow.of(7L, BinaryString.fromString("green lake")));
            writer.writeRow(2, GenericRow.of(Long.MAX_VALUE, BinaryString.fromString("blue sky")));
            writer.writeRow(3, GenericRow.of(null, null));
            entry = writer.finish().get(0);
        }
        assertThat(ByteBuffer.wrap(entry.meta()).getInt(4)).isEqualTo(2);
        Options changed = new Options();
        changed.setString("global-index.es-index.fields.text.type", "keyword");
        try (ESIndexGlobalIndexReader reader =
                reader(entry, fields, new ESIndexOptions(fields, changed))) {
            assertThat(
                            rows(
                                    reader.visitEsSearch(
                                                    new EsSearchQuery(
                                                            "{\"match\":{\"text\":\"blue\"}}", 4))
                                            .join()))
                    .containsExactlyInAnyOrder(0L, 2L);
            assertThat(
                            rows(
                                    reader.visitEsSearch(
                                                    new EsSearchQuery(
                                                            "{\"bool\":{\"must_not\":{\"term\":{\"id\":7}}}}",
                                                            4))
                                            .join()))
                    .containsExactlyInAnyOrder(0L, 2L, 3L);
            assertThat(
                            rows(
                                    reader.visitEsSearch(
                                                    new EsSearchQuery(
                                                            "{\"bool\":{\"should\":[{\"match\":{\"text\":\"blue\"}},{\"match\":{\"text\":\"lake\"}}],\"minimum_should_match\":2}}",
                                                            4))
                                            .join()))
                    .containsExactly(0L);
            assertThat(
                            rows(
                                    reader.visitEsSearch(
                                                    new EsSearchQuery(
                                                            "{\"range\":{\"id\":{\"gt\":9223372036854775807}}}",
                                                            4))
                                            .join()))
                    .isEmpty();
            assertThat(
                            rows(
                                    reader.visitEsSearch(
                                                    new EsSearchQuery(
                                                            "{\"range\":{\"id\":{\"lt\":-9223372036854775808}}}",
                                                            4))
                                            .join()))
                    .isEmpty();
            assertThat(
                            rows(
                                    reader.visitEsSearch(
                                                    new EsSearchQuery(
                                                            "{\"exists\":{\"field\":\"text\"}}", 4))
                                            .join()))
                    .containsExactlyInAnyOrder(0L, 1L, 2L);
            // Opening the existing SDK filter reader after the DSL reader must not steal its IO.
            assertThat(
                            rows(
                                    reader.visitEqual(new FieldRef(0, "id", DataTypes.BIGINT()), 7L)
                                            .join()
                                            .orElseThrow()))
                    .containsExactly(1L);
            assertThat(
                            rows(
                                    reader.visitEsSearch(new EsSearchQuery("{\"match_all\":{}}", 4))
                                            .join()))
                    .hasSize(4);
            assertThatThrownBy(
                            () ->
                                    reader.visitEsSearch(
                                                    new EsSearchQuery("{\"term\":{\"id\":7.5}}", 4))
                                            .join())
                    .hasRootCauseInstanceOf(ArithmeticException.class);
        }
    }

    @Test
    void preservesSignedZeroAndKeywordScores() throws Exception {
        List<DataField> fields =
                List.of(
                        new DataField(0, "number", DataTypes.FLOAT()),
                        new DataField(1, "label", DataTypes.STRING()));
        Options configured = new Options();
        configured.setString("global-index.es-index.fields.label.type", "keyword");
        ESIndexOptions options = new ESIndexOptions(fields, configured);
        ResultEntry entry;
        try (ESIndexGlobalIndexWriter writer = writer(fields, options)) {
            writer.writeRow(0, GenericRow.of(-0.0f, BinaryString.fromString("lake")));
            writer.writeRow(1, GenericRow.of(0.0f, BinaryString.fromString("lake")));
            writer.writeRow(2, GenericRow.of(1.0f, BinaryString.fromString("sky")));
            entry = writer.finish().get(0);
        }
        try (ESIndexGlobalIndexReader reader = reader(entry, fields, options)) {
            assertThat(
                            rows(
                                    reader.visitEsSearch(
                                                    new EsSearchQuery(
                                                            "{\"term\":{\"number\":-0.0}}", 3))
                                            .join()))
                    .containsExactly(0L);
            assertThat(
                            rows(
                                    reader.visitEsSearch(
                                                    new EsSearchQuery(
                                                            "{\"range\":{\"number\":{\"gte\":-0.0,\"lt\":0.0}}}",
                                                            3))
                                            .join()))
                    .containsExactly(0L);
            org.apache.paimon.globalindex.ScoredGlobalIndexResult match =
                    reader.visitEsSearch(new EsSearchQuery("{\"match\":{\"label\":\"lake\"}}", 3))
                            .join();
            assertThat(rows(match)).containsExactlyInAnyOrder(0L, 1L);
            assertThat(match.scoreGetter().score(0)).isEqualTo(1.0f);
            assertThat(match.scoreGetter().score(1)).isEqualTo(1.0f);
        }
    }

    @Test
    void pureNegativeClausesDoNotContributeToTopKScore() throws Exception {
        List<DataField> fields =
                List.of(
                        new DataField(0, "n", DataTypes.INT()),
                        new DataField(1, "tag", DataTypes.STRING()));
        Options configured = new Options();
        configured.setString("global-index.es-index.fields.tag.type", "keyword");
        ESIndexOptions options = new ESIndexOptions(fields, configured);
        ResultEntry entry;
        try (ESIndexGlobalIndexWriter writer = writer(fields, options)) {
            writer.writeRow(0, GenericRow.of(1, BinaryString.fromString("x")));
            writer.writeRow(1, GenericRow.of(2, BinaryString.fromString("y")));
            entry = writer.finish().get(0);
        }
        String query =
                "{\"bool\":{\"should\":[{\"bool\":{\"must_not\":{\"term\":{\"tag\":\"x\"}}}},"
                        + "{\"bool\":{\"must_not\":{\"term\":{\"n\":1}}}},{\"term\":{\"n\":1}}]}}";
        try (ESIndexGlobalIndexReader reader = reader(entry, fields, options)) {
            ScoredGlobalIndexResult result =
                    reader.visitEsSearch(new EsSearchQuery(query, 2)).join();
            assertThat(rows(result)).containsExactlyInAnyOrder(0L, 1L);
            assertThat(result.scoreGetter().score(0)).isEqualTo(1.0f);
            assertThat(result.scoreGetter().score(1)).isZero();
            assertThat(rows(reader.visitEsSearch(new EsSearchQuery(query, 1)).join()))
                    .containsExactly(0L);
            ScoredGlobalIndexResult negative =
                    reader.visitEsSearch(
                                    new EsSearchQuery(
                                            "{\"bool\":{\"must_not\":{\"term\":{\"n\":1}}}}", 2))
                            .join();
            assertThat(rows(negative)).containsExactly(1L);
            assertThat(negative.scoreGetter().score(1)).isZero();
        }
    }

    @ParameterizedTest
    @MethodSource("temporalTypes")
    void temporalQueriesUsePersistedLogicalTypeUnits(
            DataType type, Object first, Object second, long firstBound, long secondBound)
            throws Exception {
        List<DataField> fields = List.of(new DataField(0, "value", type));
        ESIndexOptions options = new ESIndexOptions(fields, new Options());
        ResultEntry entry;
        try (ESIndexGlobalIndexWriter writer = writer(fields, options)) {
            writer.writeRow(0, GenericRow.of(first));
            writer.writeRow(1, GenericRow.of(second));
            entry = writer.finish().get(0);
        }
        try (ESIndexGlobalIndexReader reader = reader(entry, fields, options)) {
            assertThat(
                            rows(
                                    reader.visitEsSearch(
                                                    new EsSearchQuery(
                                                            "{\"term\":{\"value\":"
                                                                    + firstBound
                                                                    + "}}",
                                                            2))
                                            .join()))
                    .containsExactly(0L);
            assertThat(
                            rows(
                                    reader.visitEsSearch(
                                                    new EsSearchQuery(
                                                            "{\"range\":{\"value\":{\"gt\":"
                                                                    + firstBound
                                                                    + ",\"lte\":"
                                                                    + secondBound
                                                                    + "}}}",
                                                            2))
                                            .join()))
                    .containsExactly(1L);
        }
    }

    private static Stream<Arguments> temporalTypes() {
        long epochMillis = 1_761_177_600_000L;
        return Stream.of(
                Arguments.of(DataTypes.DATE(), 20383, 20384, 20383L, 20384L),
                Arguments.of(DataTypes.TIME(), 1000, 2000, 1000L, 2000L),
                Arguments.of(
                        DataTypes.TIMESTAMP(3),
                        Timestamp.fromEpochMillis(epochMillis),
                        Timestamp.fromEpochMillis(epochMillis + 1000),
                        epochMillis,
                        epochMillis + 1000),
                Arguments.of(
                        DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3),
                        Timestamp.fromEpochMillis(epochMillis),
                        Timestamp.fromEpochMillis(epochMillis + 1000),
                        epochMillis,
                        epochMillis + 1000));
    }

    private ESIndexGlobalIndexWriter writer(List<DataField> fields, ESIndexOptions options)
            throws IOException {
        return new ESIndexGlobalIndexWriter(
                new GlobalIndexFileWriter() {
                    @Override
                    public String newFileName(String prefix) {
                        return prefix + "-" + UUID.randomUUID();
                    }

                    @Override
                    public PositionOutputStream newOutputStream(String name) throws IOException {
                        return LocalFileIO.create()
                                .newOutputStream(
                                        new org.apache.paimon.fs.Path(
                                                directory.resolve(name).toString()),
                                        false);
                    }
                },
                fields,
                options);
    }

    private ESIndexGlobalIndexReader reader(
            ResultEntry entry, List<DataField> fields, ESIndexOptions options) throws IOException {
        Path archive = directory.resolve(entry.fileName());
        GlobalIndexIOMeta meta =
                new GlobalIndexIOMeta(
                        new org.apache.paimon.fs.Path(archive.toString()),
                        Files.size(archive),
                        entry.meta());
        return new ESIndexGlobalIndexReader(
                file -> LocalFileIO.create().newInputStream(file.filePath()),
                List.of(meta),
                fields,
                options);
    }

    private static Set<Long> rows(GlobalIndexResult result) {
        Set<Long> rows = new HashSet<>();
        for (long row : result.results()) {
            rows.add(row);
        }
        return rows;
    }
}
