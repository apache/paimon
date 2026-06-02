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
import org.apache.paimon.utils.JsonSerdeUtil;
import org.apache.paimon.utils.Preconditions;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;

/** Options for the Tantivy full-text index. */
public class TantivyFullTextIndexOptions {

    private static final int META_VERSION = 1;

    public static final ConfigOption<String> TOKENIZER =
            ConfigOptions.key("tantivy.tokenizer")
                    .stringType()
                    .defaultValue("default")
                    .withDescription(
                            "Tokenizer for Tantivy full-text index. Supported values are 'default', 'simple', 'whitespace', 'raw', 'ngram', and 'jieba'.");

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

    public static final ConfigOption<Integer> MAX_TOKEN_LENGTH =
            ConfigOptions.key("tantivy.max-token-length")
                    .intType()
                    .defaultValue(40)
                    .withDescription("Maximum token length kept by configurable tokenizers.");

    public static final ConfigOption<Boolean> ASCII_FOLDING =
            ConfigOptions.key("tantivy.ascii-folding")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Whether to normalize non-ASCII latin characters to ASCII.");

    public static final ConfigOption<Boolean> STEM =
            ConfigOptions.key("tantivy.stem")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Whether to apply stemming to emitted tokens.");

    public static final ConfigOption<String> LANGUAGE =
            ConfigOptions.key("tantivy.language")
                    .stringType()
                    .defaultValue("english")
                    .withDescription("Language used by stemming and built-in stop word filters.");

    public static final ConfigOption<Boolean> REMOVE_STOP_WORDS =
            ConfigOptions.key("tantivy.remove-stop-words")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Whether to remove built-in stop words for the configured language.");

    public static final ConfigOption<String> STOP_WORDS =
            ConfigOptions.key("tantivy.stop-words")
                    .stringType()
                    .defaultValue("")
                    .withDescription("Semicolon-separated custom stop words to remove.");

    public static final ConfigOption<Boolean> WITH_POSITION =
            ConfigOptions.key("tantivy.with-position")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription("Whether to store term positions for phrase queries.");

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
    private final int maxTokenLength;
    private final boolean asciiFolding;
    private final boolean stem;
    private final String language;
    private final boolean removeStopWords;
    private final String stopWords;
    private final boolean withPosition;

    public TantivyFullTextIndexOptions(
            String tokenizer,
            int ngramMinGram,
            int ngramMaxGram,
            boolean ngramPrefixOnly,
            boolean lowerCase) {
        this(
                tokenizer,
                ngramMinGram,
                ngramMaxGram,
                ngramPrefixOnly,
                lowerCase,
                40,
                false,
                false,
                "english",
                false,
                "",
                true);
    }

    public TantivyFullTextIndexOptions(
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
            boolean withPosition) {
        this.tokenizer = normalizeTokenizer(tokenizer);
        this.ngramMinGram = ngramMinGram;
        this.ngramMaxGram = ngramMaxGram;
        this.ngramPrefixOnly = ngramPrefixOnly;
        this.lowerCase = lowerCase;
        this.maxTokenLength = maxTokenLength;
        this.asciiFolding = asciiFolding;
        this.stem = stem;
        this.language = normalizeLanguage(language);
        this.removeStopWords = removeStopWords;
        this.stopWords = normalizeStopWords(stopWords);
        this.withPosition = withPosition;
        validate();
    }

    public static TantivyFullTextIndexOptions defaults() {
        return new TantivyFullTextIndexOptions(
                "default", 2, 2, false, true, 40, false, false, "english", false, "", true);
    }

    public static TantivyFullTextIndexOptions from(Options options) {
        return new TantivyFullTextIndexOptions(
                normalizeTokenizer(options.get(TOKENIZER)),
                options.get(NGRAM_MIN_GRAM),
                options.get(NGRAM_MAX_GRAM),
                options.get(NGRAM_PREFIX_ONLY),
                options.get(LOWER_CASE),
                options.get(MAX_TOKEN_LENGTH),
                options.get(ASCII_FOLDING),
                options.get(STEM),
                options.get(LANGUAGE),
                options.get(REMOVE_STOP_WORDS),
                options.get(STOP_WORDS),
                options.get(WITH_POSITION));
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

    public int maxTokenLength() {
        return maxTokenLength;
    }

    public boolean asciiFolding() {
        return asciiFolding;
    }

    public boolean stem() {
        return stem;
    }

    public String language() {
        return language;
    }

    public boolean removeStopWords() {
        return removeStopWords;
    }

    public String stopWords() {
        return stopWords;
    }

    public boolean withPosition() {
        return withPosition;
    }

    public List<String> stopWordList() {
        if (stopWords.isEmpty()) {
            return new ArrayList<>();
        }

        List<String> words = new ArrayList<>();
        for (String word : stopWords.split(";")) {
            String trimmed = word.trim();
            if (!trimmed.isEmpty()) {
                words.add(trimmed);
            }
        }
        return words;
    }

    public String toNativeConfigJson() {
        return JsonSerdeUtil.toFlatJson(toNativeConfig());
    }

    public byte[] serialize() throws IOException {
        return toNativeConfigJson().getBytes(StandardCharsets.UTF_8);
    }

    public static TantivyFullTextIndexOptions deserialize(byte[] data) throws IOException {
        if (data == null || data.length == 0) {
            return defaults();
        }

        if (isJsonConfig(data)) {
            return fromNativeConfigJson(new String(data, StandardCharsets.UTF_8));
        }

        return deserializeLegacyBinary(data);
    }

    private static TantivyFullTextIndexOptions deserializeLegacyBinary(byte[] data)
            throws IOException {
        DataInputStream in = new DataInputStream(new ByteArrayInputStream(data));
        int version = in.readInt();
        Preconditions.checkArgument(
                version == META_VERSION,
                "Unsupported Tantivy full-text index meta version: %s",
                version);
        String tokenizer = normalizeTokenizer(in.readUTF());
        int ngramMinGram = in.readInt();
        int ngramMaxGram = in.readInt();
        boolean ngramPrefixOnly = in.readBoolean();
        boolean lowerCase = in.readBoolean();
        try {
            return new TantivyFullTextIndexOptions(
                    tokenizer,
                    ngramMinGram,
                    ngramMaxGram,
                    ngramPrefixOnly,
                    lowerCase,
                    in.readInt(),
                    in.readBoolean(),
                    in.readBoolean(),
                    in.readUTF(),
                    in.readBoolean(),
                    in.readUTF(),
                    in.readBoolean());
        } catch (EOFException e) {
            return new TantivyFullTextIndexOptions(
                    tokenizer, ngramMinGram, ngramMaxGram, ngramPrefixOnly, lowerCase);
        }
    }

    private NativeConfig toNativeConfig() {
        NativeConfig config = new NativeConfig();
        config.tokenizer = tokenizer;
        config.ngramMinGram = ngramMinGram;
        config.ngramMaxGram = ngramMaxGram;
        config.ngramPrefixOnly = ngramPrefixOnly;
        config.lowerCase = lowerCase;
        config.maxTokenLength = maxTokenLength;
        config.asciiFolding = asciiFolding;
        config.stem = stem;
        config.language = language;
        config.removeStopWords = removeStopWords;
        config.stopWords = stopWordList();
        config.withPosition = withPosition;
        return config;
    }

    private static TantivyFullTextIndexOptions fromNativeConfigJson(String json) throws IOException {
        try {
            return fromNativeConfig(JsonSerdeUtil.fromJson(json, NativeConfig.class));
        } catch (UncheckedIOException e) {
            throw e.getCause();
        }
    }

    private static TantivyFullTextIndexOptions fromNativeConfig(NativeConfig config) {
        return new TantivyFullTextIndexOptions(
                config.tokenizer,
                config.ngramMinGram,
                config.ngramMaxGram,
                config.ngramPrefixOnly,
                config.lowerCase,
                config.maxTokenLength,
                config.asciiFolding,
                config.stem,
                config.language,
                config.removeStopWords,
                joinStopWords(config.stopWords),
                config.withPosition);
    }

    private static boolean isJsonConfig(byte[] data) {
        for (byte datum : data) {
            if (!Character.isWhitespace((char) datum)) {
                return datum == '{';
            }
        }
        return false;
    }

    private static String joinStopWords(List<String> stopWords) {
        if (stopWords == null || stopWords.isEmpty()) {
            return "";
        }

        List<String> words = new ArrayList<>();
        for (String stopWord : stopWords) {
            if (stopWord != null) {
                words.add(stopWord);
            }
        }
        return String.join(";", words);
    }

    private static String normalizeTokenizer(String tokenizer) {
        return tokenizer == null ? "" : tokenizer.trim().toLowerCase();
    }

    private static String normalizeLanguage(String language) {
        return language == null ? "" : language.trim().toLowerCase(Locale.ROOT);
    }

    private static String normalizeStopWords(String stopWords) {
        if (stopWords == null) {
            return "";
        }
        List<String> words = new ArrayList<>();
        for (String word : stopWords.split(";")) {
            String trimmed = word.trim();
            if (!trimmed.isEmpty()) {
                words.add(trimmed);
            }
        }
        return String.join(";", words);
    }

    private void validate() {
        Preconditions.checkArgument(
                "default".equals(tokenizer)
                        || "simple".equals(tokenizer)
                        || "whitespace".equals(tokenizer)
                        || "raw".equals(tokenizer)
                        || "ngram".equals(tokenizer)
                        || "jieba".equals(tokenizer),
                "Unsupported Tantivy tokenizer: %s",
                tokenizer);
        Preconditions.checkArgument(ngramMinGram > 0, "ngram min gram must be positive.");
        Preconditions.checkArgument(ngramMaxGram > 0, "ngram max gram must be positive.");
        Preconditions.checkArgument(
                ngramMinGram <= ngramMaxGram, "ngram min gram must not be greater than max gram.");
        Preconditions.checkArgument(maxTokenLength > 0, "max token length must be positive.");
        Preconditions.checkArgument(
                supportedLanguages().contains(language),
                "Unsupported Tantivy language: %s",
                language);
    }

    private static List<String> supportedLanguages() {
        return Arrays.asList(
                "arabic",
                "danish",
                "dutch",
                "english",
                "finnish",
                "french",
                "german",
                "greek",
                "hungarian",
                "italian",
                "norwegian",
                "portuguese",
                "romanian",
                "russian",
                "spanish",
                "swedish",
                "tamil",
                "turkish");
    }

    /** Native JSON config consumed by both Java metadata readers and the Rust Tantivy writer. */
    private static class NativeConfig {
        public String tokenizer = "default";
        public int ngramMinGram = 2;
        public int ngramMaxGram = 2;
        public boolean ngramPrefixOnly = false;
        public boolean lowerCase = true;
        public int maxTokenLength = 40;
        public boolean asciiFolding = false;
        public boolean stem = false;
        public String language = "english";
        public boolean removeStopWords = false;
        public List<String> stopWords = new ArrayList<>();
        public boolean withPosition = true;
    }
}
