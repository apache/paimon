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

import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.globalindex.GlobalIndexIOMeta;
import org.apache.paimon.globalindex.GlobalIndexReader;
import org.apache.paimon.globalindex.GlobalIndexer;
import org.apache.paimon.globalindex.ResultEntry;
import org.apache.paimon.globalindex.ScoredGlobalIndexResult;
import org.apache.paimon.globalindex.VectorGlobalIndexer;
import org.apache.paimon.globalindex.io.GlobalIndexFileWriter;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.VectorSearch;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;

import org.elasticsearch.eslib.api.model.FieldIndexConfig;
import org.elasticsearch.eslib.api.model.VectorAlgorithm;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ESIndexVectorMetricTest {

    private static final List<DataField> VECTOR_FIELDS =
            List.of(new DataField(0, "embedding", DataTypes.VECTOR(2, DataTypes.FLOAT())));

    /** An ARRAY&lt;FLOAT&gt; column carries no dimension, so only the options can describe it. */
    private static final List<DataField> ARRAY_FIELDS =
            Collections.singletonList(
                    new DataField(0, "embedding", DataTypes.ARRAY(DataTypes.FLOAT())));

    @Test
    void exposesCanonicalPaimonMetric() {
        GlobalIndexer indexer = indexer("dp");
        assertThat(indexer).isInstanceOf(VectorGlobalIndexer.class);
        assertThat(((VectorGlobalIndexer) indexer).metric()).isEqualTo("inner_product");

        assertThat(indexer("euclidean").metric()).isEqualTo("l2");
        assertThat(indexer("cosine").metric()).isEqualTo("cosine");
        assertThat(indexer("dot_product").metric()).isEqualTo("inner_product");
        assertThat(indexer("inner_product").metric()).isEqualTo("inner_product");
        assertThat(indexer("mip").metric()).isEqualTo("inner_product");
        assertThat(indexer("maximum_inner_product").metric()).isEqualTo("inner_product");
    }

    @Test
    void persistedMetricOverridesCurrentTableConfiguration(@TempDir Path tempDir) throws Exception {
        ESIndexGlobalIndexer indexer = indexer("cosine");
        assertThat(indexer.metric()).isEqualTo("cosine");

        GlobalIndexReader reader =
                indexer.createReader(
                        meta -> null,
                        List.of(vectorFile(tempDir, "persisted-l2", "euclidean")),
                        1,
                        null);
        try {
            assertThat(((ESIndexGlobalIndexReader) reader).primaryVectorMetric()).isEqualTo("l2");
            assertThat(indexer.metric()).isEqualTo("l2");
        } finally {
            reader.close();
        }
    }

    @Test
    void rejectsShardsWithDifferentPersistedMetrics(@TempDir Path tempDir) throws Exception {
        ESIndexGlobalIndexer indexer = indexer("cosine");
        GlobalIndexReader first =
                indexer.createReader(
                        meta -> null, List.of(vectorFile(tempDir, "first", "euclidean")), 1, null);
        try {
            assertThatThrownBy(
                            () ->
                                    indexer.createReader(
                                            meta -> null,
                                            List.of(vectorFile(tempDir, "second", "cosine")),
                                            1,
                                            null))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("different vector metrics")
                    .hasMessageContaining("l2")
                    .hasMessageContaining("cosine");
        } finally {
            first.close();
        }
    }

    @Test
    void convertsLuceneScoresToPaimonScoreScale() {
        assertThat(ESIndexGlobalIndexReader.toPaimonVectorScore(0.25f, "l2")).isEqualTo(0.25f);
        assertThat(ESIndexGlobalIndexReader.toPaimonVectorScore(0.25f, "euclidean"))
                .isEqualTo(0.25f);

        assertThat(ESIndexGlobalIndexReader.toPaimonVectorScore(0.75f, "cosine")).isEqualTo(0.5f);
        assertThat(ESIndexGlobalIndexReader.toPaimonVectorScore(0.75f, "dot_product"))
                .isEqualTo(0.5f);
        assertThat(ESIndexGlobalIndexReader.toPaimonVectorScore(0.25f, "dp")).isEqualTo(-0.5f);

        assertThat(ESIndexGlobalIndexReader.toPaimonVectorScore(3.0f, "inner_product"))
                .isEqualTo(2.0f);
        assertThat(ESIndexGlobalIndexReader.toPaimonVectorScore(0.5f, "mip")).isEqualTo(-1.0f);
        assertThat(ESIndexGlobalIndexReader.toPaimonVectorScore(0.25f, "maximum_inner_product"))
                .isEqualTo(-3.0f);

        assertThatThrownBy(() -> ESIndexGlobalIndexReader.toPaimonVectorScore(Float.NaN, "cosine"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Non-finite");
        assertThatThrownBy(() -> ESIndexGlobalIndexReader.toPaimonVectorScore(0.0f, "mip"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must be positive");
    }

    @Test
    void readerDoesNotRequireCurrentOptionsToDescribeBuild(@TempDir Path tempDir) throws Exception {
        // Build time: the dimension only reaches the indexer through the procedure options.
        Map<String, String> buildOptions = new LinkedHashMap<>();
        buildOptions.put("global-index.es-index.fields.embedding.dimension", "2");
        buildOptions.put("global-index.es-index.fields.embedding.metric", "cosine");
        ESIndexGlobalIndexer buildIndexer =
                new ESIndexGlobalIndexer(ARRAY_FIELDS, Options.fromMap(buildOptions));
        Path archiveDir = tempDir.resolve("build");
        Files.createDirectories(archiveDir);
        ESIndexGlobalIndexWriter writer =
                (ESIndexGlobalIndexWriter)
                        buildIndexer.createWriter(new LocalDirWriter(archiveDir));
        writer.write(new float[] {1f, 0f}, 0);
        writer.write(new float[] {0f, 1f}, 1);
        writer.write(new float[] {-1f, 0f}, 2);
        ResultEntry entry = writer.finish().get(0);
        Path archive = archiveDir.resolve(entry.fileName());
        GlobalIndexIOMeta ioMeta =
                new GlobalIndexIOMeta(
                        new org.apache.paimon.fs.Path(archive.toString()),
                        Files.size(archive),
                        entry.meta());

        // Read time: core only passes the table options, which carry no dimension.
        ESIndexGlobalIndexer readIndexer = new ESIndexGlobalIndexer(ARRAY_FIELDS, new Options());
        GlobalIndexReader reader =
                readIndexer.createReader(
                        meta -> LocalFileIO.create().newInputStream(meta.filePath()),
                        Collections.singletonList(ioMeta),
                        3,
                        null);
        try {
            Optional<ScoredGlobalIndexResult> result =
                    reader.visitVectorSearch(new VectorSearch(new float[] {1f, 0f}, 1, "embedding"))
                            .join();
            assertThat(result).isPresent();
            assertThat(result.get().results().contains(0L)).isTrue();
            assertThat(readIndexer.metric()).isEqualTo("cosine");
        } finally {
            reader.close();
        }
    }

    @Test
    void writerStillRejectsOptionsThatCannotDescribeBuild(@TempDir Path tempDir) {
        ESIndexGlobalIndexer indexer = new ESIndexGlobalIndexer(ARRAY_FIELDS, new Options());
        assertThatThrownBy(() -> indexer.createWriter(new LocalDirWriter(tempDir)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("requires a positive dimension");
    }

    @Test
    void legacyMetadataStillRequiresCurrentOptions() throws Exception {
        // Offset-only metadata predates persisted field configs: [fileCount][name][offset][len].
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(bytes);
        out.writeInt(1);
        byte[] name = "segments_1".getBytes(StandardCharsets.UTF_8);
        out.writeInt(name.length);
        out.write(name);
        out.writeLong(0L);
        out.writeLong(1L);
        out.flush();
        GlobalIndexIOMeta legacy =
                new GlobalIndexIOMeta(
                        new org.apache.paimon.fs.Path("legacy.index"), 1L, bytes.toByteArray());

        ESIndexGlobalIndexer indexer = new ESIndexGlobalIndexer(ARRAY_FIELDS, new Options());
        assertThatThrownBy(
                        () ->
                                indexer.createReader(
                                        meta -> null, Collections.singletonList(legacy), 1, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Legacy es-index metadata");
    }

    private static ESIndexGlobalIndexer indexer(String metric) {
        Map<String, String> options = new LinkedHashMap<>();
        options.put("global-index.es-index.fields.embedding.metric", metric);
        return new ESIndexGlobalIndexer(VECTOR_FIELDS, Options.fromMap(options));
    }

    private static GlobalIndexIOMeta vectorFile(Path tempDir, String name, String metric)
            throws Exception {
        Path segment = tempDir.resolve("segments_" + name);
        Files.write(segment, new byte[] {1});

        Map<String, FieldIndexConfig> configs = new LinkedHashMap<>();
        configs.put(
                "embedding",
                FieldIndexConfig.builder("embedding", FieldIndexConfig.IndexType.VECTOR)
                        .algorithm(VectorAlgorithm.HNSW)
                        .dimension(2)
                        .metric(metric)
                        .build());
        byte[] metadata =
                ESIndexFileMeta.write(
                        new java.io.File[] {segment.toFile()},
                        List.of("embedding"),
                        List.of(VECTOR_FIELDS.get(0).type().copy(true).asSQLString()),
                        configs);

        long archiveSize = 0L;
        for (long[] range : ESIndexFileMeta.read(metadata).fileOffsets().values()) {
            archiveSize = Math.max(archiveSize, range[0] + range[1]);
        }
        return new GlobalIndexIOMeta(
                new org.apache.paimon.fs.Path(tempDir.resolve(name + ".index").toString()),
                archiveSize,
                metadata);
    }

    /** {@link GlobalIndexFileWriter} backed by a local directory. */
    private static final class LocalDirWriter implements GlobalIndexFileWriter {
        private final Path dir;
        private final LocalFileIO fio = LocalFileIO.create();

        LocalDirWriter(Path dir) {
            this.dir = dir;
        }

        @Override
        public String newFileName(String prefix) {
            return prefix + "-" + UUID.randomUUID() + ".index";
        }

        @Override
        public PositionOutputStream newOutputStream(String fileName) throws IOException {
            return fio.newOutputStream(
                    new org.apache.paimon.fs.Path(dir.resolve(fileName).toString()), true);
        }
    }
}
