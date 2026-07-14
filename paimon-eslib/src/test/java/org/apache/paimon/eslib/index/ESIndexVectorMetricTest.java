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
import org.apache.paimon.globalindex.GlobalIndexReader;
import org.apache.paimon.globalindex.GlobalIndexer;
import org.apache.paimon.globalindex.VectorGlobalIndexer;
import org.apache.paimon.options.Options;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;

import org.elasticsearch.eslib.api.model.FieldIndexConfig;
import org.elasticsearch.eslib.api.model.VectorAlgorithm;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ESIndexVectorMetricTest {

    private static final List<DataField> VECTOR_FIELDS =
            List.of(new DataField(0, "embedding", DataTypes.VECTOR(2, DataTypes.FLOAT())));

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
                        meta -> null, List.of(vectorFile(tempDir, "first", "euclidean")), null);
        try {
            assertThatThrownBy(
                            () ->
                                    indexer.createReader(
                                            meta -> null,
                                            List.of(vectorFile(tempDir, "second", "cosine")),
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
}
