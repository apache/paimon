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

import org.apache.paimon.globalindex.GlobalIndexIOMeta;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.VectorSearch;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;

import org.elasticsearch.eslib.api.model.BuiltinAnalyzer;
import org.elasticsearch.eslib.api.model.FieldIndexConfig;
import org.elasticsearch.eslib.api.model.ScalarFieldType;
import org.elasticsearch.eslib.api.model.VectorAlgorithm;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ESIndexFileMetaTest {

    @Test
    void rejectsMetadataWithoutLuceneFiles() {
        assertThatThrownBy(
                        () ->
                                ESIndexFileMeta.write(
                                        new java.io.File[0],
                                        List.of("id"),
                                        List.of("INT"),
                                        java.util.Collections.emptyMap()))
                .isInstanceOf(java.io.IOException.class)
                .hasMessageContaining("without Lucene files");
    }

    @Test
    void roundTripsBuildConfigurationAndArchiveOffsets(@TempDir Path tempDir) throws Exception {
        Path first = tempDir.resolve("segments_1");
        Path second = tempDir.resolve("_0.cfs");
        Files.write(first, new byte[] {1, 2, 3});
        Files.write(second, new byte[] {4, 5});

        Map<String, String> parameters = new LinkedHashMap<>();
        parameters.put("m", "32");
        parameters.put("ef_construction", "200");
        Map<String, FieldIndexConfig> configs = new LinkedHashMap<>();
        configs.put(
                "embedding",
                FieldIndexConfig.builder("embedding", FieldIndexConfig.IndexType.VECTOR)
                        .algorithm(VectorAlgorithm.HNSW)
                        .dimension(4)
                        .metric("cosine")
                        .algorithmParams(parameters)
                        .build());

        ESIndexFileMeta.Parsed parsed =
                ESIndexFileMeta.read(
                        ESIndexFileMeta.write(
                                new java.io.File[] {first.toFile(), second.toFile()},
                                List.of("embedding"),
                                List.of("ARRAY<FLOAT>"),
                                configs));

        assertThat(parsed.hasFieldConfigs()).isTrue();
        assertThat(parsed.indexedFieldNames()).containsExactly("embedding");
        assertThat(parsed.indexedFieldTypes()).containsExactly("ARRAY<FLOAT>");
        FieldIndexConfig vector = parsed.fieldConfigs().get("embedding");
        assertThat(vector.algorithm()).isEqualTo(VectorAlgorithm.HNSW);
        assertThat(vector.dimension()).isEqualTo(4);
        assertThat(vector.metric()).isEqualTo("cosine");
        assertThat(vector.algorithmParams()).containsAllEntriesOf(parameters);

        long firstOffset = 4L + 4L + "segments_1".getBytes(StandardCharsets.UTF_8).length + 8L;
        assertThat(parsed.fileOffsets().get("segments_1")).containsExactly(firstOffset, 3L);
        long secondOffset =
                firstOffset + 3L + 4L + "_0.cfs".getBytes(StandardCharsets.UTF_8).length + 8L;
        assertThat(parsed.fileOffsets().get("_0.cfs")).containsExactly(secondOffset, 2L);
    }

    @Test
    void readsLegacyOffsetOnlyMetadata() throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(bytes);
        byte[] name = "segments_1".getBytes(StandardCharsets.UTF_8);
        out.writeInt(1);
        out.writeInt(name.length);
        out.write(name);
        out.writeLong(17L);
        out.writeLong(23L);
        out.flush();

        ESIndexFileMeta.Parsed parsed = ESIndexFileMeta.read(bytes.toByteArray());
        assertThat(parsed.hasFieldConfigs()).isFalse();
        assertThat(parsed.indexedFieldTypes()).isEmpty();
        assertThat(parsed.fieldConfigs()).isEmpty();
        assertThat(parsed.fileOffsets().get("segments_1")).containsExactly(17L, 23L);
    }

    @Test
    void readsVersionOnePrimaryFieldOrder() throws Exception {
        Map<String, FieldIndexConfig> configs = new LinkedHashMap<>();
        configs.put(
                "title",
                FieldIndexConfig.builder("title", FieldIndexConfig.IndexType.FULLTEXT)
                        .analyzer(BuiltinAnalyzer.STANDARD)
                        .build());
        configs.put(
                "title.keyword",
                FieldIndexConfig.builder("title.keyword", FieldIndexConfig.IndexType.KEYWORD)
                        .scalarType(ScalarFieldType.KEYWORD)
                        .build());
        configs.put(
                "price",
                FieldIndexConfig.builder("price", FieldIndexConfig.IndexType.SCALAR)
                        .scalarType(ScalarFieldType.INT)
                        .build());

        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(bytes);
        out.writeInt(0x45534D31); // ESM1
        out.writeInt(1);
        out.writeInt(configs.size());
        for (Map.Entry<String, FieldIndexConfig> entry : configs.entrySet()) {
            writeConfig(out, entry.getKey(), entry.getValue());
        }
        out.writeInt(1);
        writeString(out, "segments_1");
        out.writeLong(17L);
        out.writeLong(23L);
        out.flush();

        ESIndexFileMeta.Parsed parsed = ESIndexFileMeta.read(bytes.toByteArray());
        assertThat(parsed.indexedFieldNames()).containsExactly("title", "price");
        assertThat(parsed.indexedFieldTypes()).isEmpty();
        assertThat(parsed.fileOffsets().get("segments_1")).containsExactly(17L, 23L);
    }

    @Test
    void versionOneVectorMetadataRejectsChangedFixedDimension() throws Exception {
        FieldIndexConfig config =
                FieldIndexConfig.builder("embedding", FieldIndexConfig.IndexType.VECTOR)
                        .algorithm(VectorAlgorithm.HNSW)
                        .dimension(2)
                        .metric("cosine")
                        .build();
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(bytes);
        out.writeInt(0x45534D31); // ESM1
        out.writeInt(1);
        out.writeInt(1);
        writeConfig(out, "embedding", config);
        out.writeInt(1);
        writeString(out, "segments_1");
        out.writeLong(0L);
        out.writeLong(1L);
        out.flush();

        List<DataField> currentFields =
                List.of(new DataField(0, "embedding", DataTypes.VECTOR(3, DataTypes.FLOAT())));
        ESIndexGlobalIndexReader reader =
                new ESIndexGlobalIndexReader(
                        meta -> null,
                        List.of(
                                new GlobalIndexIOMeta(
                                        new org.apache.paimon.fs.Path("/tmp/legacy.index"),
                                        1L,
                                        bytes.toByteArray())),
                        currentFields,
                        new ESIndexOptions(currentFields, new Options()));
        try {
            assertThatThrownBy(
                            () ->
                                    reader.visitVectorSearch(
                                                    new VectorSearch(
                                                            new float[] {1.0f, 2.0f, 3.0f},
                                                            1,
                                                            "embedding"))
                                            .join())
                    .hasCauseInstanceOf(IllegalStateException.class)
                    .hasRootCauseMessage(
                            "The persisted es-index for field 'embedding' is incompatible with "
                                    + "the current schema and cannot safely answer vector search. "
                                    + "Rebuild the es-index after the schema change.");
        } finally {
            reader.close();
        }
    }

    @Test
    void rejectsDuplicateFilesInLegacyMetadata() throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(bytes);
        byte[] name = "segments_1".getBytes(StandardCharsets.UTF_8);
        out.writeInt(2);
        for (int i = 0; i < 2; i++) {
            out.writeInt(name.length);
            out.write(name);
            out.writeLong(17L + i);
            out.writeLong(23L);
        }
        out.flush();

        assertThatThrownBy(() -> ESIndexFileMeta.read(bytes.toByteArray()))
                .isInstanceOf(java.io.IOException.class)
                .hasMessageContaining("Duplicate file offset")
                .hasMessageContaining("segments_1");
    }

    @Test
    void rejectsDuplicateAlgorithmParameters() throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(bytes);
        out.writeInt(0x45534D31); // ESM1
        out.writeInt(1);
        out.writeInt(1);
        writeString(out, "embedding");
        writeString(out, FieldIndexConfig.IndexType.VECTOR.name());
        out.writeBoolean(true);
        writeString(out, VectorAlgorithm.HNSW.name());
        out.writeInt(4);
        out.writeBoolean(true);
        writeString(out, "cosine");
        out.writeBoolean(false); // analyzer
        out.writeBoolean(false); // scalar type
        out.writeInt(2);
        for (String value : new String[] {"16", "32"}) {
            writeString(out, "m");
            writeString(out, value);
        }
        out.writeInt(0); // files
        out.flush();

        assertThatThrownBy(() -> ESIndexFileMeta.read(bytes.toByteArray()))
                .isInstanceOf(java.io.IOException.class)
                .hasMessageContaining("Duplicate algorithm parameter")
                .hasMessageContaining("embedding")
                .hasMessageContaining("m");
    }

    @Test
    void rejectsTrailingMetadataBytes() throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(bytes);
        out.writeInt(0);
        out.writeByte(1);
        out.flush();

        assertThatThrownBy(() -> ESIndexFileMeta.read(bytes.toByteArray()))
                .isInstanceOf(java.io.IOException.class)
                .hasMessageContaining("Trailing bytes");
    }

    @Test
    void readerRejectsFileRangesOutsideArchive() throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(bytes);
        writeString(out, "segments_1");
        out.writeLong(4L);
        out.writeLong(10L);
        out.flush();
        byte[] offsetEntry = bytes.toByteArray();
        bytes.reset();
        out = new DataOutputStream(bytes);
        out.writeInt(1);
        out.write(offsetEntry);
        out.flush();

        List<DataField> fields = List.of(new DataField(0, "id", DataTypes.INT()));
        GlobalIndexIOMeta archive =
                new GlobalIndexIOMeta(
                        new org.apache.paimon.fs.Path("/tmp/invalid.index"),
                        5L,
                        bytes.toByteArray());
        assertThatThrownBy(
                        () ->
                                new ESIndexGlobalIndexReader(
                                        meta -> null,
                                        List.of(archive),
                                        fields,
                                        new ESIndexOptions(fields, new Options())))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid es-index metadata")
                .hasRootCauseMessage(
                        "File range exceeds es-index archive size for segments_1: "
                                + "archiveSize=5, range=[4, 10]");
    }

    @Test
    void readerRejectsMetadataWithoutLuceneFiles() {
        List<DataField> fields = List.of(new DataField(0, "id", DataTypes.INT()));
        GlobalIndexIOMeta archive =
                new GlobalIndexIOMeta(
                        new org.apache.paimon.fs.Path("/tmp/empty.index"),
                        4L,
                        new byte[] {0, 0, 0, 0});

        assertThatThrownBy(
                        () ->
                                new ESIndexGlobalIndexReader(
                                        meta -> null,
                                        List.of(archive),
                                        fields,
                                        new ESIndexOptions(fields, new Options())))
                .isInstanceOf(IllegalArgumentException.class)
                .hasRootCauseMessage("ES index metadata contains no Lucene files");
    }

    private static void writeString(DataOutputStream out, String value) throws Exception {
        byte[] encoded = value.getBytes(StandardCharsets.UTF_8);
        out.writeInt(encoded.length);
        out.write(encoded);
    }

    private static void writeConfig(DataOutputStream out, String fieldName, FieldIndexConfig config)
            throws Exception {
        writeString(out, fieldName);
        writeString(out, config.indexType().name());
        writeNullableEnum(out, config.algorithm());
        out.writeInt(config.dimension());
        writeNullableString(out, config.metric());
        writeNullableEnum(out, config.analyzer());
        writeNullableEnum(out, config.scalarType());
        out.writeInt(config.algorithmParams().size());
        for (Map.Entry<String, String> parameter : config.algorithmParams().entrySet()) {
            writeString(out, parameter.getKey());
            writeString(out, parameter.getValue());
        }
    }

    private static void writeNullableEnum(DataOutputStream out, Enum<?> value) throws Exception {
        writeNullableString(out, value == null ? null : value.name());
    }

    private static void writeNullableString(DataOutputStream out, String value) throws Exception {
        out.writeBoolean(value != null);
        if (value != null) {
            writeString(out, value);
        }
    }
}
