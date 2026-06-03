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

package org.apache.paimon.tantivy.index;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link TantivyFullTextIndexOptions}. */
public class TantivyFullTextIndexOptionsTest {

    @Test
    public void testDeserializeEmptyMetaUsesDefaults() throws Exception {
        TantivyFullTextIndexOptions indexOptions =
                TantivyFullTextIndexOptions.deserialize(new byte[0]);

        assertThat(indexOptions.tokenizer()).isEqualTo("default");
        assertThat(indexOptions.ngramMinGram()).isEqualTo(2);
        assertThat(indexOptions.ngramMaxGram()).isEqualTo(2);
        assertThat(indexOptions.ngramPrefixOnly()).isFalse();
        assertThat(indexOptions.lowerCase()).isTrue();
        assertThat(indexOptions.maxTokenLength()).isEqualTo(40);
        assertThat(indexOptions.asciiFolding()).isFalse();
        assertThat(indexOptions.stem()).isFalse();
        assertThat(indexOptions.language()).isEqualTo("english");
        assertThat(indexOptions.removeStopWords()).isFalse();
        assertThat(indexOptions.stopWords()).isEmpty();
        assertThat(indexOptions.withPosition()).isTrue();
    }

    @Test
    public void testDefaultOptionsSerializeSparseJson() throws Exception {
        TantivyFullTextIndexOptions indexOptions = TantivyFullTextIndexOptions.defaults();

        assertThat(indexOptions.toNativeConfigJson()).isEqualTo("{}");
        assertThat(indexOptions.serialize()).isEqualTo("{}".getBytes(StandardCharsets.UTF_8));
        assertThat(TantivyFullTextIndexOptions.deserialize(indexOptions.serialize()).tokenizer())
                .isEqualTo("default");
    }

    @Test
    public void testSerializeDeserializeNgramOptions() throws Exception {
        Map<String, Object> options = new HashMap<>();
        options.put("tokenizer", " NGRAM ");
        options.put("ngram.min-gram", 2);
        options.put("ngram.max-gram", 3);
        options.put("ngram.prefix-only", true);
        options.put("lower-case", false);

        TantivyFullTextIndexOptions indexOptions =
                TantivyFullTextIndexOptions.deserialize(
                        new TantivyFullTextIndexOptions(options).serialize());

        assertThat(indexOptions.tokenizer()).isEqualTo("ngram");
        assertThat(indexOptions.ngramMinGram()).isEqualTo(2);
        assertThat(indexOptions.ngramMaxGram()).isEqualTo(3);
        assertThat(indexOptions.ngramPrefixOnly()).isTrue();
        assertThat(indexOptions.lowerCase()).isFalse();
        assertThat(indexOptions.toNativeConfigJson())
                .isEqualTo(
                        "{\"tokenizer\":\"ngram\",\"ngram.max-gram\":3,"
                                + "\"ngram.prefix-only\":true,\"lower-case\":false}");
    }

    @Test
    public void testSerializeDeserializeAnalyzerOptions() throws Exception {
        Map<String, Object> options = new HashMap<>();
        options.put("tokenizer", " WHITESPACE ");
        options.put("lower-case", false);
        options.put("max-token-length", 12);
        options.put("ascii-folding", true);
        options.put("stem", true);
        options.put("language", "English");
        options.put("remove-stop-words", true);
        options.put("stop-words", "paimon;lake");
        options.put("with-position", false);

        TantivyFullTextIndexOptions indexOptions =
                TantivyFullTextIndexOptions.deserialize(
                        new TantivyFullTextIndexOptions(options).serialize());

        assertThat(indexOptions.tokenizer()).isEqualTo("whitespace");
        assertThat(indexOptions.lowerCase()).isFalse();
        assertThat(indexOptions.maxTokenLength()).isEqualTo(12);
        assertThat(indexOptions.asciiFolding()).isTrue();
        assertThat(indexOptions.stem()).isTrue();
        assertThat(indexOptions.language()).isEqualTo("english");
        assertThat(indexOptions.removeStopWords()).isTrue();
        assertThat(indexOptions.stopWords()).isEqualTo("paimon;lake");
        assertThat(indexOptions.stopWordList()).containsExactly("paimon", "lake");
        assertThat(indexOptions.withPosition()).isFalse();
        assertThat(indexOptions.toNativeConfigJson()).contains("\"tokenizer\":\"whitespace\"");
        assertThat(indexOptions.toNativeConfigJson()).doesNotContain("\"ngram.min-gram\"");
        assertThat(indexOptions.toNativeConfigJson()).doesNotContain("\"ngram.max-gram\"");
        assertThat(indexOptions.serialize())
                .isEqualTo(indexOptions.toNativeConfigJson().getBytes(StandardCharsets.UTF_8));
    }

    @Test
    public void testSerializeDeserializeJiebaOptions() throws Exception {
        Map<String, Object> options = new HashMap<>();
        options.put("tokenizer", " JIEBA ");
        options.put("lower-case", true);

        TantivyFullTextIndexOptions indexOptions =
                TantivyFullTextIndexOptions.deserialize(
                        new TantivyFullTextIndexOptions(options).serialize());

        assertThat(indexOptions.tokenizer()).isEqualTo("jieba");
        assertThat(indexOptions.ngramMinGram()).isEqualTo(2);
        assertThat(indexOptions.ngramMaxGram()).isEqualTo(2);
        assertThat(indexOptions.ngramPrefixOnly()).isFalse();
        assertThat(indexOptions.lowerCase()).isTrue();
    }

    @Test
    public void testValidateTokenizerOptions() {
        Map<String, Object> unsupportedTokenizerOptions = new HashMap<>();
        unsupportedTokenizerOptions.put("tokenizer", "ik");
        assertThatThrownBy(() -> new TantivyFullTextIndexOptions(unsupportedTokenizerOptions))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unsupported Tantivy tokenizer");

        Map<String, Object> invalidNgramOptions = new HashMap<>();
        invalidNgramOptions.put("tokenizer", "ngram");
        invalidNgramOptions.put("ngram.min-gram", 3);
        invalidNgramOptions.put("ngram.max-gram", 2);
        assertThatThrownBy(() -> new TantivyFullTextIndexOptions(invalidNgramOptions))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("ngram min gram must not be greater than max gram");

        Map<String, Object> unsupportedLanguageOptions = new HashMap<>();
        unsupportedLanguageOptions.put("stem", true);
        unsupportedLanguageOptions.put("language", "klingon");
        assertThatThrownBy(() -> new TantivyFullTextIndexOptions(unsupportedLanguageOptions))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unsupported Tantivy language");
    }
}
