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
import java.io.EOFException;
import java.io.IOException;
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
        StringBuilder builder = new StringBuilder();
        builder.append('{');
        appendJsonField(builder, "tokenizer", tokenizer).append(',');
        appendJsonField(builder, "ngramMinGram", ngramMinGram).append(',');
        appendJsonField(builder, "ngramMaxGram", ngramMaxGram).append(',');
        appendJsonField(builder, "ngramPrefixOnly", ngramPrefixOnly).append(',');
        appendJsonField(builder, "lowerCase", lowerCase).append(',');
        appendJsonField(builder, "maxTokenLength", maxTokenLength).append(',');
        appendJsonField(builder, "asciiFolding", asciiFolding).append(',');
        appendJsonField(builder, "stem", stem).append(',');
        appendJsonField(builder, "language", language).append(',');
        appendJsonField(builder, "removeStopWords", removeStopWords).append(',');
        builder.append("\"stopWords\":[");
        List<String> stopWordList = stopWordList();
        for (int i = 0; i < stopWordList.size(); i++) {
            if (i > 0) {
                builder.append(',');
            }
            appendJsonString(builder, stopWordList.get(i));
        }
        builder.append("],");
        appendJsonField(builder, "withPosition", withPosition);
        builder.append('}');
        return builder.toString();
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

    private static StringBuilder appendJsonField(StringBuilder builder, String key, String value) {
        appendJsonString(builder, key).append(':');
        appendJsonString(builder, value);
        return builder;
    }

    private static StringBuilder appendJsonField(StringBuilder builder, String key, int value) {
        appendJsonString(builder, key).append(':').append(value);
        return builder;
    }

    private static StringBuilder appendJsonField(StringBuilder builder, String key, boolean value) {
        appendJsonString(builder, key).append(':').append(value);
        return builder;
    }

    private static StringBuilder appendJsonString(StringBuilder builder, String value) {
        builder.append('"');
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            switch (c) {
                case '"':
                    builder.append("\\\"");
                    break;
                case '\\':
                    builder.append("\\\\");
                    break;
                case '\b':
                    builder.append("\\b");
                    break;
                case '\f':
                    builder.append("\\f");
                    break;
                case '\n':
                    builder.append("\\n");
                    break;
                case '\r':
                    builder.append("\\r");
                    break;
                case '\t':
                    builder.append("\\t");
                    break;
                default:
                    if (c < 0x20) {
                        builder.append(String.format("\\u%04x", (int) c));
                    } else {
                        builder.append(c);
                    }
            }
        }
        builder.append('"');
        return builder;
    }
}
