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

import org.apache.paimon.options.ConfigOption;
import org.apache.paimon.options.ConfigOptions;
import org.apache.paimon.options.Options;
import org.apache.paimon.utils.Preconditions;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/** Options for the Tantivy full-text index. */
public class TantivyFullTextIndexOptions {

    private static final int META_VERSION = 1;

    public static final ConfigOption<String> TOKENIZER =
            ConfigOptions.key("tantivy.tokenizer")
                    .stringType()
                    .defaultValue("default")
                    .withDescription(
                            "Tokenizer for Tantivy full-text index. Supported values are 'default', 'ngram', and 'jieba'.");

    public static final ConfigOption<Integer> NGRAM_MIN_GRAM =
            ConfigOptions.key("tantivy.ngram.min-gram")
                    .intType()
                    .defaultValue(2)
                    .withDescription("Minimum ngram length when 'tantivy.tokenizer' is 'ngram'.");

    public static final ConfigOption<Integer> NGRAM_MAX_GRAM =
            ConfigOptions.key("tantivy.ngram.max-gram")
                    .intType()
                    .defaultValue(2)
                    .withDescription("Maximum ngram length when 'tantivy.tokenizer' is 'ngram'.");

    public static final ConfigOption<Boolean> NGRAM_PREFIX_ONLY =
            ConfigOptions.key("tantivy.ngram.prefix-only")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Whether ngram tokenizer should only emit prefix ngrams when 'tantivy.tokenizer' is 'ngram'.");

    public static final ConfigOption<Boolean> LOWER_CASE =
            ConfigOptions.key("tantivy.lower-case")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription("Whether to lowercase tokens for configurable tokenizers.");

    public static final ConfigOption<Integer> SEARCHER_POOL_MAX_SIZE =
            ConfigOptions.key("tantivy.searcher-pool.max-size")
                    .intType()
                    .defaultValue(32)
                    .withDescription(
                            "Maximum number of idle TantivySearcher instances kept in the pool "
                                    + "across all index shards. Each entry holds the index open in "
                                    + "Rust memory (including the FST term dictionary), so memory "
                                    + "usage scales with this value times the index size per shard. "
                                    + "Set to 0 to disable pooling.");

    private final String tokenizer;
    private final int ngramMinGram;
    private final int ngramMaxGram;
    private final boolean ngramPrefixOnly;
    private final boolean lowerCase;

    public TantivyFullTextIndexOptions(
            String tokenizer,
            int ngramMinGram,
            int ngramMaxGram,
            boolean ngramPrefixOnly,
            boolean lowerCase) {
        this.tokenizer = normalizeTokenizer(tokenizer);
        this.ngramMinGram = ngramMinGram;
        this.ngramMaxGram = ngramMaxGram;
        this.ngramPrefixOnly = ngramPrefixOnly;
        this.lowerCase = lowerCase;
        validate();
    }

    public static TantivyFullTextIndexOptions defaults() {
        return new TantivyFullTextIndexOptions("default", 2, 2, false, true);
    }

    public static TantivyFullTextIndexOptions from(Options options) {
        return new TantivyFullTextIndexOptions(
                normalizeTokenizer(options.get(TOKENIZER)),
                options.get(NGRAM_MIN_GRAM),
                options.get(NGRAM_MAX_GRAM),
                options.get(NGRAM_PREFIX_ONLY),
                options.get(LOWER_CASE));
    }

    public String tokenizer() {
        return tokenizer;
    }

    public int ngramMinGram() {
        return ngramMinGram;
    }

    public int ngramMaxGram() {
        return ngramMaxGram;
    }

    public boolean ngramPrefixOnly() {
        return ngramPrefixOnly;
    }

    public boolean lowerCase() {
        return lowerCase;
    }

    public byte[] serialize() throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(bytes);
        out.writeInt(META_VERSION);
        out.writeUTF(tokenizer);
        out.writeInt(ngramMinGram);
        out.writeInt(ngramMaxGram);
        out.writeBoolean(ngramPrefixOnly);
        out.writeBoolean(lowerCase);
        out.flush();
        return bytes.toByteArray();
    }

    public static TantivyFullTextIndexOptions deserialize(byte[] data) throws IOException {
        if (data == null || data.length == 0) {
            return defaults();
        }

        DataInputStream in = new DataInputStream(new ByteArrayInputStream(data));
        int version = in.readInt();
        Preconditions.checkArgument(
                version == META_VERSION,
                "Unsupported Tantivy full-text index meta version: %s",
                version);
        return new TantivyFullTextIndexOptions(
                normalizeTokenizer(in.readUTF()),
                in.readInt(),
                in.readInt(),
                in.readBoolean(),
                in.readBoolean());
    }

    private static String normalizeTokenizer(String tokenizer) {
        return tokenizer == null ? "" : tokenizer.trim().toLowerCase();
    }

    private void validate() {
        Preconditions.checkArgument(
                "default".equals(tokenizer)
                        || "ngram".equals(tokenizer)
                        || "jieba".equals(tokenizer),
                "Unsupported Tantivy tokenizer: %s",
                tokenizer);
        Preconditions.checkArgument(ngramMinGram > 0, "ngram min gram must be positive.");
        Preconditions.checkArgument(ngramMaxGram > 0, "ngram max gram must be positive.");
        Preconditions.checkArgument(
                ngramMinGram <= ngramMaxGram, "ngram min gram must not be greater than max gram.");
    }
}
