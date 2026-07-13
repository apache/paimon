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

import org.apache.paimon.options.Options;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;

import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.perfield.PerFieldKnnVectorsFormat;
import org.elasticsearch.eslib.adapter.PaimonHnswVectorsFormat;
import org.elasticsearch.eslib.adapter.lucene9.PaimonLucene9Codec;
import org.elasticsearch.eslib.api.model.FieldIndexConfig;
import org.elasticsearch.eslib.api.model.ScalarFieldType;
import org.elasticsearch.eslib.api.model.VectorAlgorithm;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests option-key resolution in {@link ESIndexOptions}. */
class ESIndexOptionsTest {

    private static final List<DataField> FIELDS =
            Arrays.asList(
                    new DataField(0, "embedding", DataTypes.VECTOR(128, DataTypes.FLOAT())),
                    new DataField(1, "title", DataTypes.STRING()));

    @Test
    void fieldLevelKeysAreRead() {
        Map<String, String> m = new HashMap<>();
        m.put("global-index.es-index.fields.embedding.algorithm", "diskbbq");
        m.put("global-index.es-index.fields.embedding.dimension", "128");
        m.put("global-index.es-index.fields.title.analyzer", "standard");
        ESIndexOptions options = new ESIndexOptions(FIELDS, Options.fromMap(m));

        FieldIndexConfig emb = options.getConfig("embedding");
        assertThat(emb.indexType()).isEqualTo(FieldIndexConfig.IndexType.VECTOR);
        assertThat(emb.algorithm()).isEqualTo(VectorAlgorithm.DISKBBQ);
        assertThat(emb.dimension()).isEqualTo(128);
        assertThat(options.getConfig("title").indexType())
                .isEqualTo(FieldIndexConfig.IndexType.FULLTEXT);
    }

    @Test
    void nativeVectorAlgorithmIsRejectedUpFront() {
        Map<String, String> m = new HashMap<>();
        m.put("global-index.es-index.fields.embedding.algorithm", "native");

        assertThatThrownBy(() -> new ESIndexOptions(FIELDS, Options.fromMap(m)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Vector algorithm 'native' is not supported")
                .hasMessageContaining("hnsw")
                .hasMessageContaining("diskbbq");
    }

    @Test
    void hnswConstructionParametersAreRead() {
        Map<String, String> m = new HashMap<>();
        m.put("global-index.es-index.fields.embedding.algorithm", "hnsw");
        m.put("global-index.es-index.fields.embedding.dimension", "128");
        m.put("global-index.es-index.fields.embedding.m", "30");
        m.put("global-index.es-index.fields.embedding.ef_construction", "360");

        FieldIndexConfig emb =
                new ESIndexOptions(FIELDS, Options.fromMap(m)).getConfig("embedding");

        assertThat(emb.algorithm()).isEqualTo(VectorAlgorithm.HNSW);
        assertThat(emb.getIntParam("m", 16)).isEqualTo(30);
        assertThat(emb.getIntParam("ef_construction", 100)).isEqualTo(360);
    }

    @Test
    void hnswConstructionParametersReachLuceneCodec() {
        Map<String, String> m = new HashMap<>();
        m.put("global-index.es-index.fields.embedding.algorithm", "hnsw");
        m.put("global-index.es-index.fields.embedding.dimension", "128");
        m.put("global-index.es-index.fields.embedding.m", "30");
        m.put("global-index.es-index.fields.embedding.ef_construction", "360");

        ESIndexOptions options = new ESIndexOptions(FIELDS, Options.fromMap(m));
        PaimonLucene9Codec codec = new PaimonLucene9Codec(options.getFieldConfigs());
        PerFieldKnnVectorsFormat perField = (PerFieldKnnVectorsFormat) codec.knnVectorsFormat();
        KnnVectorsFormat format = perField.getKnnVectorsFormatForField("embedding");

        assertThat(format).isInstanceOf(PaimonHnswVectorsFormat.class);
        assertThat(format.toString()).contains("maxConn=30").contains("beamWidth=360");
    }

    @Test
    void invalidHnswConstructionParametersAreRejected() {
        Map<String, String> nonPositive = new HashMap<>();
        nonPositive.put("global-index.es-index.fields.embedding.algorithm", "hnsw");
        nonPositive.put("global-index.es-index.fields.embedding.m", "0");
        assertThatThrownBy(() -> new ESIndexOptions(FIELDS, Options.fromMap(nonPositive)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("between 1 and 512");

        Map<String, String> excessiveM = new HashMap<>();
        excessiveM.put("global-index.es-index.fields.embedding.algorithm", "hnsw");
        excessiveM.put("global-index.es-index.fields.embedding.m", "513");
        assertThatThrownBy(() -> new ESIndexOptions(FIELDS, Options.fromMap(excessiveM)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("between 1 and 512");

        Map<String, String> excessiveEfConstruction = new HashMap<>();
        excessiveEfConstruction.put("global-index.es-index.fields.embedding.algorithm", "hnsw");
        excessiveEfConstruction.put(
                "global-index.es-index.fields.embedding.ef_construction", "3201");
        assertThatThrownBy(
                        () -> new ESIndexOptions(FIELDS, Options.fromMap(excessiveEfConstruction)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("between 1 and 3200");

        Map<String, String> nonInteger = new HashMap<>();
        nonInteger.put("global-index.es-index.fields.embedding.algorithm", "hnsw");
        nonInteger.put("global-index.es-index.fields.embedding.ef_construction", "wide");
        assertThatThrownBy(() -> new ESIndexOptions(FIELDS, Options.fromMap(nonInteger)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("between 1 and 3200");

        Map<String, String> wrongAlgorithm = new HashMap<>();
        wrongAlgorithm.put("global-index.es-index.fields.embedding.algorithm", "diskbbq");
        wrongAlgorithm.put("global-index.es-index.fields.embedding.ef_construction", "360");
        assertThatThrownBy(() -> new ESIndexOptions(FIELDS, Options.fromMap(wrongAlgorithm)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("require algorithm=hnsw");
    }

    @Test
    void tableLevelAlgorithmParametersOnlyApplyToMatchingAlgorithms() {
        Map<String, String> diskOptions = new HashMap<>();
        diskOptions.put("global-index.es-index.m", "30");
        diskOptions.put("global-index.es-index.ef_construction", "360");
        diskOptions.put("global-index.es-index.fields.embedding.algorithm", "diskbbq");
        diskOptions.put("global-index.es-index.centroids_per_parent_cluster", "24");
        FieldIndexConfig diskConfig =
                new ESIndexOptions(FIELDS, Options.fromMap(diskOptions)).getConfig("embedding");
        assertThat(diskConfig.algorithmParams()).doesNotContainKeys("m", "ef_construction");
        assertThat(diskConfig.algorithmParams())
                .containsEntry("centroids_per_parent_cluster", "24");

        Map<String, String> hnswOptions = new HashMap<>();
        hnswOptions.put("global-index.es-index.vectors_per_cluster", "384");
        hnswOptions.put("global-index.es-index.centroids_per_parent_cluster", "24");
        hnswOptions.put("global-index.es-index.fields.embedding.algorithm", "hnsw");
        FieldIndexConfig hnswConfig =
                new ESIndexOptions(FIELDS, Options.fromMap(hnswOptions)).getConfig("embedding");
        assertThat(hnswConfig.algorithmParams())
                .doesNotContainKeys("vectors_per_cluster", "centroids_per_parent_cluster");
    }

    @Test
    void indexTypeLevelKeyIsDefaultAndFieldLevelOverrides() {
        Map<String, String> m = new HashMap<>();
        // index-type-level default for all vector fields ...
        m.put("global-index.es-index.metric", "l2");
        m.put("global-index.es-index.fields.embedding.algorithm", "diskbbq");
        m.put("global-index.es-index.fields.embedding.dimension", "128");
        // ... overridden per-field
        m.put("global-index.es-index.fields.embedding.metric", "cosine");
        ESIndexOptions options = new ESIndexOptions(FIELDS, Options.fromMap(m));

        FieldIndexConfig emb = options.getConfig("embedding");
        assertThat(emb.metric()).isEqualTo("cosine"); // field-level wins over es-index.metric=l2
    }

    @Test
    void globalIndexPrefixedKeysAreRead() {
        // Keys use the full global-index.es-index. prefix (the persisted table-property / ES-mount
        // convention). The parser reads the explicitly configured analyzer.
        Map<String, String> m = new HashMap<>();
        m.put("global-index.es-index.fields.title.analyzer", "standard");
        ESIndexOptions options = new ESIndexOptions(FIELDS, Options.fromMap(m));
        assertThat(options.getConfig("title").indexType())
                .isEqualTo(FieldIndexConfig.IndexType.FULLTEXT);
    }

    @Test
    void textFieldsGetComplementaryMultiField() {
        ESIndexOptions fulltext = new ESIndexOptions(FIELDS, Options.fromMap(new HashMap<>()));
        assertThat(fulltext.keywordSubField("title")).isEqualTo("title.keyword");
        assertThat(fulltext.fullTextSearchField("title")).isEqualTo("title");

        Map<String, String> keywordOptions = new HashMap<>();
        keywordOptions.put("global-index.es-index.fields.title.type", "keyword");
        ESIndexOptions keyword = new ESIndexOptions(FIELDS, Options.fromMap(keywordOptions));
        assertThat(keyword.fullTextSubField("title")).isEqualTo("title.fulltext");
        assertThat(keyword.fullTextSearchField("title")).isEqualTo("title.fulltext");
    }

    @Test
    void unsupportedScalarTypeIsRejectedUpFront() {
        // A BOOLEAN companion column is not a supported es-index scalar type. It must be rejected
        // when options are parsed, not silently mapped to KEYWORD and then blow up at build time in
        // ESIndexGlobalIndexWriter.extractScalar (getString -> ClassCastException).
        List<DataField> fields = Arrays.asList(new DataField(0, "flag", DataTypes.BOOLEAN()));
        assertThatThrownBy(() -> new ESIndexOptions(fields, Options.fromMap(new HashMap<>())))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unsupported scalar type");
    }

    @Test
    void textFieldDefaultsToFullTextWithKeywordSubField() {
        // A String column with no analyzer/type configured defaults to FULLTEXT (with a keyword
        // sub-field), so full-text search always works on text columns and exact filters use the
        // sub-field. This closes the coverage gap where a text column carried only as an extra
        // field would otherwise default to KEYWORD yet still be counted as full-text coverage.
        ESIndexOptions options = new ESIndexOptions(FIELDS, Options.fromMap(new HashMap<>()));
        assertThat(options.getConfig("title").indexType())
                .isEqualTo(FieldIndexConfig.IndexType.FULLTEXT);
        assertThat(options.keywordSubField("title")).isEqualTo("title.keyword");
    }

    @Test
    void textFieldConfiguredAsKeywordStillGetsFullTextSubField() {
        Map<String, String> m = new HashMap<>();
        m.put("global-index.es-index.fields.title.type", "keyword");
        ESIndexOptions options = new ESIndexOptions(FIELDS, Options.fromMap(m));
        assertThat(options.getConfig("title").indexType())
                .isEqualTo(FieldIndexConfig.IndexType.KEYWORD);
        assertThat(options.keywordSubField("title")).isNull();
        assertThat(options.fullTextSubField("title")).isEqualTo("title.fulltext");
    }

    @Test
    void temporalFieldsUseLongScalarForFiltering() {
        List<DataField> fields =
                Arrays.asList(
                        new DataField(0, "d", DataTypes.DATE()),
                        new DataField(1, "ts", DataTypes.TIMESTAMP(3)));
        ESIndexOptions options = new ESIndexOptions(fields, Options.fromMap(new HashMap<>()));

        assertThat(options.getConfig("d").indexType()).isEqualTo(FieldIndexConfig.IndexType.SCALAR);
        assertThat(options.getConfig("d").scalarType()).isEqualTo(ScalarFieldType.LONG);
        assertThat(options.getConfig("ts").indexType())
                .isEqualTo(FieldIndexConfig.IndexType.SCALAR);
        assertThat(options.getConfig("ts").scalarType()).isEqualTo(ScalarFieldType.LONG);

        Map<String, String> explicitDate = new HashMap<>();
        explicitDate.put("global-index.es-index.fields.d.type", "date");
        ESIndexOptions explicitOptions = new ESIndexOptions(fields, Options.fromMap(explicitDate));
        assertThat(explicitOptions.getConfig("d").indexType())
                .isEqualTo(FieldIndexConfig.IndexType.SCALAR);
        assertThat(explicitOptions.getConfig("d").scalarType()).isEqualTo(ScalarFieldType.LONG);
    }

    @Test
    void arrayVectorRequiresDimensionAndGetsPresenceField() {
        List<DataField> fields =
                Arrays.asList(new DataField(0, "embedding", DataTypes.ARRAY(DataTypes.FLOAT())));

        assertThatThrownBy(() -> new ESIndexOptions(fields, new Options()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("requires a positive dimension");

        Map<String, String> configured = new HashMap<>();
        configured.put("global-index.es-index.fields.embedding.dimension", "4");
        ESIndexOptions options = new ESIndexOptions(fields, Options.fromMap(configured));
        String presenceField = options.arrayPresenceField("embedding");
        assertThat(presenceField).isEqualTo("embedding.__paimon_array_present");
        assertThat(options.getConfig(presenceField).scalarType()).isEqualTo(ScalarFieldType.INT);
    }

    @Test
    void invalidVectorDimensionMetricAndDiskBBQParametersAreRejected() {
        Map<String, String> wrongDimension = new HashMap<>();
        wrongDimension.put("global-index.es-index.fields.embedding.dimension", "64");
        assertThatThrownBy(() -> new ESIndexOptions(FIELDS, Options.fromMap(wrongDimension)))
                .hasMessageContaining("does not match the VECTOR type dimension 128");

        List<DataField> oversizedVector =
                Arrays.asList(
                        new DataField(0, "embedding", DataTypes.VECTOR(4097, DataTypes.FLOAT())));
        assertThatThrownBy(() -> new ESIndexOptions(oversizedVector, new Options()))
                .hasMessageContaining("maximum supported dimension 4096");

        Map<String, String> badMetric = new HashMap<>();
        badMetric.put("global-index.es-index.fields.embedding.metric", "angular-ish");
        assertThatThrownBy(() -> new ESIndexOptions(FIELDS, Options.fromMap(badMetric)))
                .hasMessageContaining("Unknown vector metric 'angular-ish'");

        Map<String, String> badVpc = new HashMap<>();
        badVpc.put("global-index.es-index.fields.embedding.algorithm", "diskbbq");
        badVpc.put("global-index.es-index.fields.embedding.vectors_per_cluster", "many");
        assertThatThrownBy(() -> new ESIndexOptions(FIELDS, Options.fromMap(badVpc)))
                .hasMessageContaining("vectors_per_cluster")
                .hasMessageContaining("between 64 and 65536");

        Map<String, String> badCpc = new HashMap<>();
        badCpc.put("global-index.es-index.fields.embedding.algorithm", "diskbbq");
        badCpc.put("global-index.es-index.fields.embedding.centroids_per_parent_cluster", "1");
        assertThatThrownBy(() -> new ESIndexOptions(FIELDS, Options.fromMap(badCpc)))
                .hasMessageContaining("centroids_per_parent_cluster")
                .hasMessageContaining("between 2 and 384");

        Map<String, String> hnswVpc = new HashMap<>();
        hnswVpc.put("global-index.es-index.fields.embedding.vectors_per_cluster", "64");
        assertThatThrownBy(() -> new ESIndexOptions(FIELDS, Options.fromMap(hnswVpc)))
                .hasMessageContaining("requires algorithm=diskbbq");

        Map<String, String> hnswCpc = new HashMap<>();
        hnswCpc.put("global-index.es-index.fields.embedding.centroids_per_parent_cluster", "16");
        assertThatThrownBy(() -> new ESIndexOptions(FIELDS, Options.fromMap(hnswCpc)))
                .hasMessageContaining("requires algorithm=diskbbq");
    }

    @Test
    void unsupportedIkAnalyzerIsRejectedBeforeIndexBuild() {
        Map<String, String> options = new HashMap<>();
        options.put("global-index.es-index.fields.title.analyzer", "ik_smart");

        assertThatThrownBy(
                        () ->
                                new ESIndexOptions(
                                        Arrays.asList(
                                                new DataField(0, "title", DataTypes.STRING())),
                                        Options.fromMap(options)))
                .hasMessageContaining("not supported by paimon-eslib")
                .hasMessageContaining("standard, whitespace, simple, or keyword");
    }

    @Test
    void unknownAndIncompatibleExplicitTypesAreRejected() {
        List<DataField> number = Arrays.asList(new DataField(0, "id", DataTypes.INT()));

        Map<String, String> unknown = new HashMap<>();
        unknown.put("global-index.es-index.fields.id.type", "mystery");
        assertThatThrownBy(() -> new ESIndexOptions(number, Options.fromMap(unknown)))
                .hasMessageContaining("Unknown es-index type 'mystery'");

        Map<String, String> fulltextNumber = new HashMap<>();
        fulltextNumber.put("global-index.es-index.fields.id.type", "fulltext");
        assertThatThrownBy(() -> new ESIndexOptions(number, Options.fromMap(fulltextNumber)))
                .hasMessageContaining("incompatible")
                .hasMessageContaining("id");

        Map<String, String> geo = new HashMap<>();
        geo.put("global-index.es-index.fields.id.type", "geo_point");
        assertThatThrownBy(() -> new ESIndexOptions(number, Options.fromMap(geo)))
                .hasMessageContaining("geo_point")
                .hasMessageContaining("not supported");

        List<DataField> integerVector =
                Arrays.asList(new DataField(0, "embedding", DataTypes.VECTOR(4, DataTypes.INT())));
        assertThatThrownBy(() -> new ESIndexOptions(integerVector, new Options()))
                .hasMessageContaining("Unsupported scalar type")
                .hasMessageContaining("VECTOR");
    }

    @Test
    void generatedFieldNamesCannotCollideWithLogicalColumns() {
        List<DataField> keywordCollision =
                Arrays.asList(
                        new DataField(0, "title", DataTypes.STRING()),
                        new DataField(1, "title.keyword", DataTypes.STRING()));
        assertThatThrownBy(() -> new ESIndexOptions(keywordCollision, new Options()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("reserved es-index generated field")
                .hasMessageContaining("title.keyword");

        List<DataField> presenceCollision =
                Arrays.asList(
                        new DataField(0, "labels", DataTypes.ARRAY(DataTypes.INT())),
                        new DataField(1, "labels.__paimon_array_present", DataTypes.INT()));
        assertThatThrownBy(() -> new ESIndexOptions(presenceCollision, new Options()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("reserved es-index generated field")
                .hasMessageContaining("labels.__paimon_array_present");
    }

    @Test
    void fieldConfigViewIsImmutable() {
        ESIndexOptions options =
                new ESIndexOptions(
                        Arrays.asList(new DataField(0, "id", DataTypes.INT())), new Options());

        assertThatThrownBy(() -> options.getFieldConfigs().clear())
                .isInstanceOf(UnsupportedOperationException.class);
        assertThat(options.getConfig("id")).isNotNull();
    }
}
