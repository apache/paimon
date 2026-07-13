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
                    new DataField(0, "embedding", DataTypes.ARRAY(DataTypes.FLOAT())),
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
    void indexTypeLevelKeyIsDefaultAndFieldLevelOverrides() {
        Map<String, String> m = new HashMap<>();
        // index-type-level default for all vector fields ...
        m.put("global-index.es-index.metric", "l2");
        m.put("global-index.es-index.fields.embedding.algorithm", "diskbbq");
        m.put("global-index.es-index.fields.embedding.dimension", "8");
        // ... overridden per-field
        m.put("global-index.es-index.fields.embedding.metric", "cosine");
        ESIndexOptions options = new ESIndexOptions(FIELDS, Options.fromMap(m));

        FieldIndexConfig emb = options.getConfig("embedding");
        assertThat(emb.metric()).isEqualTo("cosine"); // field-level wins over es-index.metric=l2
    }

    @Test
    void globalIndexPrefixedKeysAreRead() {
        // Keys use the full global-index.es-index. prefix (the persisted table-property / ES-mount
        // convention). The parser reads them, so an explicit analyzer makes the field FULLTEXT
        // instead of the default KEYWORD.
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
}
