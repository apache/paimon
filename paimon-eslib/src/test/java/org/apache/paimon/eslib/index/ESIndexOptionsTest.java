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
import org.elasticsearch.eslib.api.model.VectorAlgorithm;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests option-key resolution in {@link ESIndexOptions}. */
class ESIndexOptionsTest {

    private static final List<DataField> FIELDS =
            Arrays.asList(
                    new DataField(0, "embedding", DataTypes.ARRAY(DataTypes.FLOAT())),
                    new DataField(1, "title", DataTypes.STRING()));

    @Test
    void fieldLevelKeysAreRead() {
        Map<String, String> m = new HashMap<>();
        m.put("fields.embedding.algorithm", "diskbbq");
        m.put("fields.embedding.dimension", "128");
        m.put("fields.title.analyzer", "standard");
        ESIndexOptions options = new ESIndexOptions(FIELDS, Options.fromMap(m));

        FieldIndexConfig emb = options.getConfig("embedding");
        assertThat(emb.indexType()).isEqualTo(FieldIndexConfig.IndexType.VECTOR);
        assertThat(emb.algorithm()).isEqualTo(VectorAlgorithm.DISKBBQ);
        assertThat(emb.dimension()).isEqualTo(128);
        assertThat(options.getConfig("title").indexType())
                .isEqualTo(FieldIndexConfig.IndexType.FULLTEXT);
    }

    @Test
    void indexTypeLevelKeyIsDefaultAndFieldLevelOverrides() {
        Map<String, String> m = new HashMap<>();
        // index-type-level default for all vector fields ...
        m.put("es-index.metric", "l2");
        m.put("fields.embedding.algorithm", "diskbbq");
        m.put("fields.embedding.dimension", "8");
        // ... overridden per-field
        m.put("fields.embedding.metric", "cosine");
        ESIndexOptions options = new ESIndexOptions(FIELDS, Options.fromMap(m));

        FieldIndexConfig emb = options.getConfig("embedding");
        assertThat(emb.metric()).isEqualTo("cosine"); // field-level wins over es-index.metric=l2
    }

    @Test
    void longFormGlobalIndexKeysAreNotMistakenlyRead() {
        // Keys written with the full global-index.es-index. prefix are NOT what the parser reads;
        // the field stays at its default (STRING with no analyzer -> KEYWORD).
        Map<String, String> m = new HashMap<>();
        m.put("global-index.es-index.fields.title.analyzer", "standard");
        ESIndexOptions options = new ESIndexOptions(FIELDS, Options.fromMap(m));
        assertThat(options.getConfig("title").indexType())
                .isEqualTo(FieldIndexConfig.IndexType.KEYWORD);
    }

    @Test
    void fulltextGetsKeywordSubFieldByDefaultAndCanBeDisabled() {
        Map<String, String> on = new HashMap<>();
        on.put("fields.title.analyzer", "standard");
        ESIndexOptions enabled = new ESIndexOptions(FIELDS, Options.fromMap(on));
        assertThat(enabled.keywordSubField("title")).isEqualTo("title.keyword");

        Map<String, String> off = new HashMap<>(on);
        off.put("fields.title.keyword_subfield", "false");
        ESIndexOptions disabled = new ESIndexOptions(FIELDS, Options.fromMap(off));
        assertThat(disabled.keywordSubField("title")).isNull();
    }
}
