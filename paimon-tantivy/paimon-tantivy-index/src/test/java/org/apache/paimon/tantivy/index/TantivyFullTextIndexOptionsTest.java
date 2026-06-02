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
    public void testValidateTokenizerOptions() {
        assertThatThrownBy(() -> new TantivyFullTextIndexOptions("ik", 2, 2, false, true))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unsupported Tantivy tokenizer");

        assertThatThrownBy(() -> new TantivyFullTextIndexOptions("ngram", 3, 2, false, true))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("ngram min gram must not be greater than max gram");
    }
}
