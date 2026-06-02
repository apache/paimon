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

import org.apache.paimon.options.Options;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.nio.charset.StandardCharsets;

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
    public void testSerializeDeserializeNgramOptions() throws Exception {
        Options options = new Options();
        options.set(TantivyFullTextIndexOptions.TOKENIZER, " NGRAM ");
        options.set(TantivyFullTextIndexOptions.NGRAM_MIN_GRAM, 2);
        options.set(TantivyFullTextIndexOptions.NGRAM_MAX_GRAM, 3);
        options.set(TantivyFullTextIndexOptions.NGRAM_PREFIX_ONLY, true);
        options.set(TantivyFullTextIndexOptions.LOWER_CASE, false);

        TantivyFullTextIndexOptions indexOptions =
                TantivyFullTextIndexOptions.deserialize(
                        TantivyFullTextIndexOptions.from(options).serialize());

        assertThat(indexOptions.tokenizer()).isEqualTo("ngram");
        assertThat(indexOptions.ngramMinGram()).isEqualTo(2);
        assertThat(indexOptions.ngramMaxGram()).isEqualTo(3);
        assertThat(indexOptions.ngramPrefixOnly()).isTrue();
        assertThat(indexOptions.lowerCase()).isFalse();
    }

    @Test
    public void testSerializeDeserializeAnalyzerOptions() throws Exception {
        Options options = new Options();
        options.set(TantivyFullTextIndexOptions.TOKENIZER, " WHITESPACE ");
        options.set(TantivyFullTextIndexOptions.LOWER_CASE, false);
        options.set(TantivyFullTextIndexOptions.MAX_TOKEN_LENGTH, 12);
        options.set(TantivyFullTextIndexOptions.ASCII_FOLDING, true);
        options.set(TantivyFullTextIndexOptions.STEM, true);
        options.set(TantivyFullTextIndexOptions.LANGUAGE, "English");
        options.set(TantivyFullTextIndexOptions.REMOVE_STOP_WORDS, true);
        options.set(TantivyFullTextIndexOptions.STOP_WORDS, "paimon;lake");
        options.set(TantivyFullTextIndexOptions.WITH_POSITION, false);

        TantivyFullTextIndexOptions indexOptions =
                TantivyFullTextIndexOptions.deserialize(
                        TantivyFullTextIndexOptions.from(options).serialize());

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
        assertThat(indexOptions.serialize())
                .isEqualTo(indexOptions.toNativeConfigJson().getBytes(StandardCharsets.UTF_8));
    }

    @Test
    public void testSerializeDeserializeJiebaOptions() throws Exception {
        Options options = new Options();
        options.set(TantivyFullTextIndexOptions.TOKENIZER, " JIEBA ");
        options.set(TantivyFullTextIndexOptions.LOWER_CASE, true);

        TantivyFullTextIndexOptions indexOptions =
                TantivyFullTextIndexOptions.deserialize(
                        TantivyFullTextIndexOptions.from(options).serialize());

        assertThat(indexOptions.tokenizer()).isEqualTo("jieba");
        assertThat(indexOptions.ngramMinGram()).isEqualTo(2);
        assertThat(indexOptions.ngramMaxGram()).isEqualTo(2);
        assertThat(indexOptions.ngramPrefixOnly()).isFalse();
        assertThat(indexOptions.lowerCase()).isTrue();
    }

    @Test
    public void testDeserializeLegacyBinaryNgramOptions() throws Exception {
        TantivyFullTextIndexOptions indexOptions =
                TantivyFullTextIndexOptions.deserialize(
                        legacyBinaryMeta(" NGRAM ", 2, 3, true, false));

        assertThat(indexOptions.tokenizer()).isEqualTo("ngram");
        assertThat(indexOptions.ngramMinGram()).isEqualTo(2);
        assertThat(indexOptions.ngramMaxGram()).isEqualTo(3);
        assertThat(indexOptions.ngramPrefixOnly()).isTrue();
        assertThat(indexOptions.lowerCase()).isFalse();
        assertThat(indexOptions.maxTokenLength()).isEqualTo(40);
        assertThat(indexOptions.asciiFolding()).isFalse();
        assertThat(indexOptions.stem()).isFalse();
        assertThat(indexOptions.language()).isEqualTo("english");
        assertThat(indexOptions.removeStopWords()).isFalse();
        assertThat(indexOptions.stopWords()).isEmpty();
        assertThat(indexOptions.withPosition()).isTrue();
    }

    @Test
    public void testDeserializeLegacyBinaryAnalyzerOptions() throws Exception {
        TantivyFullTextIndexOptions indexOptions =
                TantivyFullTextIndexOptions.deserialize(
                        legacyBinaryMeta(
                                " WHITESPACE ",
                                2,
                                2,
                                false,
                                false,
                                12,
                                true,
                                true,
                                "English",
                                true,
                                "paimon;lake",
                                false));

        assertThat(indexOptions.tokenizer()).isEqualTo("whitespace");
        assertThat(indexOptions.lowerCase()).isFalse();
        assertThat(indexOptions.maxTokenLength()).isEqualTo(12);
        assertThat(indexOptions.asciiFolding()).isTrue();
        assertThat(indexOptions.stem()).isTrue();
        assertThat(indexOptions.language()).isEqualTo("english");
        assertThat(indexOptions.removeStopWords()).isTrue();
        assertThat(indexOptions.stopWords()).isEqualTo("paimon;lake");
        assertThat(indexOptions.withPosition()).isFalse();
    }

    @Test
    public void testValidateTokenizerOptions() {
        assertThatThrownBy(() -> new TantivyFullTextIndexOptions("ik", 2, 2, false, true))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unsupported Tantivy tokenizer");

        assertThatThrownBy(() -> new TantivyFullTextIndexOptions("ngram", 3, 2, false, true))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("ngram min gram must not be greater than max gram");

        assertThatThrownBy(
                        () ->
                                new TantivyFullTextIndexOptions(
                                        "default", 2, 2, false, true, 40, false, true, "klingon",
                                        false, "", true))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unsupported Tantivy language");
    }

    private static byte[] legacyBinaryMeta(
            String tokenizer,
            int ngramMinGram,
            int ngramMaxGram,
            boolean ngramPrefixOnly,
            boolean lowerCase)
            throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(bytes);
        out.writeInt(1);
        out.writeUTF(tokenizer);
        out.writeInt(ngramMinGram);
        out.writeInt(ngramMaxGram);
        out.writeBoolean(ngramPrefixOnly);
        out.writeBoolean(lowerCase);
        out.flush();
        return bytes.toByteArray();
    }

    private static byte[] legacyBinaryMeta(
            String tokenizer,
            int ngramMinGram,
            int ngramMaxGram,
            boolean ngramPrefixOnly,
            boolean lowerCase,
            int maxTokenLength,
            boolean asciiFolding,
            boolean stem,
            String language,
            boolean removeStopWords,
            String stopWords,
            boolean withPosition)
            throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(bytes);
        out.writeInt(1);
        out.writeUTF(tokenizer);
        out.writeInt(ngramMinGram);
        out.writeInt(ngramMaxGram);
        out.writeBoolean(ngramPrefixOnly);
        out.writeBoolean(lowerCase);
        out.writeInt(maxTokenLength);
        out.writeBoolean(asciiFolding);
        out.writeBoolean(stem);
        out.writeUTF(language);
        out.writeBoolean(removeStopWords);
        out.writeUTF(stopWords);
        out.writeBoolean(withPosition);
        out.flush();
        return bytes.toByteArray();
    }
}
