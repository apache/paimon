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
import org.apache.paimon.utils.JsonSerdeUtil;
import org.apache.paimon.utils.Preconditions;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.core.type.TypeReference;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/** Options for the Tantivy full-text index. */
public class TantivyFullTextIndexOptions {

    static final String TANTIVY_PREFIX = "tantivy.";

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

    private final Map<String, Object> config;

    public TantivyFullTextIndexOptions(Map<String, Object> options) {
        this.config =
                Collections.unmodifiableMap(toNativeConfigMap(Preconditions.checkNotNull(options)));
    }

    public static TantivyFullTextIndexOptions defaults() {
        return new TantivyFullTextIndexOptions(Collections.emptyMap());
    }

    public String tokenizer() {
        return getString(optionKey(TOKENIZER), TOKENIZER.defaultValue());
    }

    public int ngramMinGram() {
        return getInt(optionKey(NGRAM_MIN_GRAM), NGRAM_MIN_GRAM.defaultValue());
    }

    public int ngramMaxGram() {
        return getInt(optionKey(NGRAM_MAX_GRAM), NGRAM_MAX_GRAM.defaultValue());
    }

    public boolean ngramPrefixOnly() {
        return getBoolean(optionKey(NGRAM_PREFIX_ONLY), NGRAM_PREFIX_ONLY.defaultValue());
    }

    public boolean lowerCase() {
        return getBoolean(optionKey(LOWER_CASE), LOWER_CASE.defaultValue());
    }

    public int maxTokenLength() {
        return getInt(optionKey(MAX_TOKEN_LENGTH), MAX_TOKEN_LENGTH.defaultValue());
    }

    public boolean asciiFolding() {
        return getBoolean(optionKey(ASCII_FOLDING), ASCII_FOLDING.defaultValue());
    }

    public boolean stem() {
        return getBoolean(optionKey(STEM), STEM.defaultValue());
    }

    public String language() {
        return getString(optionKey(LANGUAGE), LANGUAGE.defaultValue());
    }

    public boolean removeStopWords() {
        return getBoolean(optionKey(REMOVE_STOP_WORDS), REMOVE_STOP_WORDS.defaultValue());
    }

    public String stopWords() {
        return joinStopWords(stopWordList());
    }

    public boolean withPosition() {
        return getBoolean(optionKey(WITH_POSITION), WITH_POSITION.defaultValue());
    }

    public List<String> stopWordList() {
        return getStringList(optionKey(STOP_WORDS));
    }

    public String toNativeConfigJson() {
        return JsonSerdeUtil.toFlatJson(config);
    }

    public byte[] serialize() throws IOException {
        return toNativeConfigJson().getBytes(StandardCharsets.UTF_8);
    }

    public static TantivyFullTextIndexOptions deserialize(byte[] data) throws IOException {
        if (data == null || data.length == 0) {
            return defaults();
        }

        return fromNativeConfigJson(new String(data, StandardCharsets.UTF_8));
    }

    private static Map<String, Object> toNativeConfigMap(Map<String, Object> options) {
        Map<String, Object> config = new LinkedHashMap<>();
        String tokenizer =
                normalizeTokenizer(
                        getString(options, optionKey(TOKENIZER), TOKENIZER.defaultValue()));
        validateTokenizer(tokenizer);
        if (!tokenizer.equals(TOKENIZER.defaultValue())) {
            config.put(optionKey(TOKENIZER), tokenizer);
        }
        int ngramMinGram =
                getInt(options, optionKey(NGRAM_MIN_GRAM), NGRAM_MIN_GRAM.defaultValue());
        int ngramMaxGram =
                getInt(options, optionKey(NGRAM_MAX_GRAM), NGRAM_MAX_GRAM.defaultValue());
        validateNgramGrams(ngramMinGram, ngramMaxGram);
        if (ngramMinGram != NGRAM_MIN_GRAM.defaultValue()) {
            config.put(optionKey(NGRAM_MIN_GRAM), ngramMinGram);
        }
        if (ngramMaxGram != NGRAM_MAX_GRAM.defaultValue()) {
            config.put(optionKey(NGRAM_MAX_GRAM), ngramMaxGram);
        }
        boolean ngramPrefixOnly =
                getBoolean(options, optionKey(NGRAM_PREFIX_ONLY), NGRAM_PREFIX_ONLY.defaultValue());
        if (ngramPrefixOnly != NGRAM_PREFIX_ONLY.defaultValue()) {
            config.put(optionKey(NGRAM_PREFIX_ONLY), ngramPrefixOnly);
        }
        boolean lowerCase = getBoolean(options, optionKey(LOWER_CASE), LOWER_CASE.defaultValue());
        if (lowerCase != LOWER_CASE.defaultValue()) {
            config.put(optionKey(LOWER_CASE), lowerCase);
        }
        int maxTokenLength =
                getInt(options, optionKey(MAX_TOKEN_LENGTH), MAX_TOKEN_LENGTH.defaultValue());
        validateMaxTokenLength(maxTokenLength);
        if (maxTokenLength != MAX_TOKEN_LENGTH.defaultValue()) {
            config.put(optionKey(MAX_TOKEN_LENGTH), maxTokenLength);
        }
        boolean asciiFolding =
                getBoolean(options, optionKey(ASCII_FOLDING), ASCII_FOLDING.defaultValue());
        if (asciiFolding != ASCII_FOLDING.defaultValue()) {
            config.put(optionKey(ASCII_FOLDING), asciiFolding);
        }
        boolean stem = getBoolean(options, optionKey(STEM), STEM.defaultValue());
        if (stem != STEM.defaultValue()) {
            config.put(optionKey(STEM), stem);
        }
        String language =
                normalizeLanguage(getString(options, optionKey(LANGUAGE), LANGUAGE.defaultValue()));
        validateLanguage(language);
        if (!language.equals(LANGUAGE.defaultValue())) {
            config.put(optionKey(LANGUAGE), language);
        }
        boolean removeStopWords =
                getBoolean(options, optionKey(REMOVE_STOP_WORDS), REMOVE_STOP_WORDS.defaultValue());
        if (removeStopWords != REMOVE_STOP_WORDS.defaultValue()) {
            config.put(optionKey(REMOVE_STOP_WORDS), removeStopWords);
        }
        List<String> stopWordList = getStopWordList(options, optionKey(STOP_WORDS));
        if (!stopWordList.isEmpty()) {
            config.put(optionKey(STOP_WORDS), Collections.unmodifiableList(stopWordList));
        }
        boolean withPosition =
                getBoolean(options, optionKey(WITH_POSITION), WITH_POSITION.defaultValue());
        if (withPosition != WITH_POSITION.defaultValue()) {
            config.put(optionKey(WITH_POSITION), withPosition);
        }
        return config;
    }

    private static TantivyFullTextIndexOptions fromNativeConfigJson(String json)
            throws IOException {
        try {
            Map<String, Object> config =
                    JsonSerdeUtil.fromJson(json, new TypeReference<Map<String, Object>>() {});
            return new TantivyFullTextIndexOptions(config);
        } catch (UncheckedIOException e) {
            throw e.getCause();
        }
    }

    private String getString(String key, String defaultValue) {
        Object value = config.get(key);
        return value == null ? defaultValue : (String) value;
    }

    private int getInt(String key, int defaultValue) {
        Object value = config.get(key);
        return value == null ? defaultValue : (int) value;
    }

    private boolean getBoolean(String key, boolean defaultValue) {
        Object value = config.get(key);
        return value == null ? defaultValue : (boolean) value;
    }

    @SuppressWarnings("unchecked")
    private List<String> getStringList(String key) {
        Object value = config.get(key);
        if (value == null) {
            return new ArrayList<>();
        }
        return new ArrayList<>((List<String>) value);
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

    private static List<String> toStopWordList(Object value) {
        if (value == null) {
            return new ArrayList<>();
        }
        if (value instanceof List) {
            List<String> words = new ArrayList<>();
            for (Object word : (List<?>) value) {
                if (word != null) {
                    words.add(word.toString());
                }
            }
            return words;
        }
        return normalizeStopWordList(value.toString());
    }

    private static String getString(Map<String, Object> options, String key, String defaultValue) {
        Object value = options.get(key);
        return value == null ? defaultValue : value.toString();
    }

    private static int getInt(Map<String, Object> options, String key, int defaultValue) {
        Object value = options.get(key);
        if (value == null) {
            return defaultValue;
        }
        if (value instanceof Number) {
            return ((Number) value).intValue();
        }
        return Integer.parseInt(value.toString());
    }

    private static boolean getBoolean(
            Map<String, Object> options, String key, boolean defaultValue) {
        Object value = options.get(key);
        if (value == null) {
            return defaultValue;
        }
        if (value instanceof Boolean) {
            return (boolean) value;
        }
        return Boolean.parseBoolean(value.toString());
    }

    private static List<String> getStopWordList(Map<String, Object> options, String key) {
        Object value = options.get(key);
        if (value == null) {
            return new ArrayList<>();
        }
        if (value instanceof List) {
            return toStopWordList(value);
        }
        return normalizeStopWordList(value.toString());
    }

    private static String optionKey(ConfigOption<?> option) {
        return option.key().substring(TANTIVY_PREFIX.length());
    }

    private static String normalizeTokenizer(String tokenizer) {
        return tokenizer == null ? "" : tokenizer.trim().toLowerCase();
    }

    private static String normalizeLanguage(String language) {
        return language == null ? "" : language.trim().toLowerCase(Locale.ROOT);
    }

    private static List<String> normalizeStopWordList(String stopWords) {
        List<String> words = new ArrayList<>();
        if (stopWords == null) {
            return words;
        }
        for (String word : stopWords.split(";")) {
            String trimmed = word.trim();
            if (!trimmed.isEmpty()) {
                words.add(trimmed);
            }
        }
        return words;
    }

    private static void validateTokenizer(String tokenizer) {
        Preconditions.checkArgument(
                "default".equals(tokenizer)
                        || "simple".equals(tokenizer)
                        || "whitespace".equals(tokenizer)
                        || "raw".equals(tokenizer)
                        || "ngram".equals(tokenizer)
                        || "jieba".equals(tokenizer),
                "Unsupported Tantivy tokenizer: %s",
                tokenizer);
    }

    private static void validateNgramGrams(int ngramMinGram, int ngramMaxGram) {
        Preconditions.checkArgument(ngramMinGram > 0, "ngram min gram must be positive.");
        Preconditions.checkArgument(ngramMaxGram > 0, "ngram max gram must be positive.");
        Preconditions.checkArgument(
                ngramMinGram <= ngramMaxGram, "ngram min gram must not be greater than max gram.");
    }

    private static void validateMaxTokenLength(int maxTokenLength) {
        Preconditions.checkArgument(maxTokenLength > 0, "max token length must be positive.");
    }

    private static void validateLanguage(String language) {
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
}
