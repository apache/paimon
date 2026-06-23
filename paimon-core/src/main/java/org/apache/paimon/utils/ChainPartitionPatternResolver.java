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

package org.apache.paimon.utils;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.Period;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAmount;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * Resolves timestamp pattern and formatter to extract time step and compute partition values for
 * chain table partitions.
 */
public class ChainPartitionPatternResolver {
    private static final Map<Character, ChronoField> FIELD_MAP = new HashMap<>();
    private final List<String> partitionColumns;
    private final String pattern;
    private final String formatter;
    private Map<PatternToken, List<FormatToken>> patternFormatMappings;
    private Map<PatternToken, Pair<Integer, Integer>> patternSpanMappings;
    private List<PatternToken> patternTokens;
    private List<FormatToken> formatTokens;

    public ChainPartitionPatternResolver(
            List<String> partitionColumns, String pattern, String formatter) {
        checkArgument(pattern != null, "pattern cannot be null");
        checkArgument(formatter != null, "formatter cannot be null");
        checkArgument(partitionColumns != null, "partitionColumns cannot be null");
        this.partitionColumns = partitionColumns;
        this.pattern = pattern;
        this.formatter = formatter;
        init();
    }

    static {
        FIELD_MAP.put('y', ChronoField.YEAR);
        FIELD_MAP.put('M', ChronoField.MONTH_OF_YEAR);
        FIELD_MAP.put('d', ChronoField.DAY_OF_MONTH);
        FIELD_MAP.put('H', ChronoField.HOUR_OF_DAY);
        FIELD_MAP.put('h', ChronoField.CLOCK_HOUR_OF_AMPM);
        FIELD_MAP.put('m', ChronoField.MINUTE_OF_HOUR);
        FIELD_MAP.put('s', ChronoField.SECOND_OF_MINUTE);
    }

    private void init() {
        this.patternFormatMappings = new HashMap<>();
        this.patternTokens = parsePattern();
        this.formatTokens = parseFormatter();
        boolean matched = matchRecursive(0, 0);
        checkArgument(
                matched, "Failed to match pattern '%s' to formatter '%s'", pattern, formatter);
        this.patternSpanMappings = calPatternSpanMappings();
    }

    /**
     * Extracts the minimum time step from the given pattern and formatter.
     *
     * @return the smallest {@link Duration} or {@link Period} step among variable-controlled time
     *     units
     */
    public TemporalAmount extractMinStep() {
        List<TimeFieldToken> fieldTokens =
                patternFormatMappings.values().stream()
                        .flatMap(Collection::stream)
                        .filter(token -> token instanceof TimeFieldToken)
                        .map(token -> (TimeFieldToken) token)
                        .collect(Collectors.toList());

        Optional<TimeFieldToken> min =
                fieldTokens.stream().min(Comparator.comparingInt(span -> span.field.ordinal()));
        checkArgument(min.isPresent(), "No time unit found in variable ranges");
        ChronoField field = min.get().field;
        return stepOf(field);
    }

    /**
     * Computes partition column values by formatting the given datetime and extracting each
     * variable's segment according to the pattern-to-format mapping.
     */
    public LinkedHashMap<String, String> calPartValues(LocalDateTime dateTime) {
        DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern(formatter);
        String formatted = dateTime.format(dateTimeFormatter);
        LinkedHashMap<String, String> result = new LinkedHashMap<>();
        for (Map.Entry<PatternToken, Pair<Integer, Integer>> entry :
                patternSpanMappings.entrySet()) {
            String variableName = entry.getKey().token.substring(1);
            int start = entry.getValue().getLeft();
            int end = entry.getValue().getRight();
            result.put(variableName, formatted.substring(start, end));
        }
        return result;
    }

    private Map<PatternToken, Pair<Integer, Integer>> calPatternSpanMappings() {
        int pos = 0;
        Map<FormatToken, Pair<Integer, Integer>> formattedSpanMapping = new HashMap<>();
        for (FormatToken token : formatTokens) {
            formattedSpanMapping.put(token, Pair.of(pos, pos + token.getLength()));
            pos += token.getLength();
        }

        Map<PatternToken, Pair<Integer, Integer>> patternSpanMapping = new HashMap<>();
        for (PatternToken patternToken : patternTokens) {
            if (!patternToken.isVariable) {
                continue;
            }
            List<FormatToken> tokens = patternFormatMappings.get(patternToken);
            List<Pair<Integer, Integer>> spans =
                    tokens.stream().map(formattedSpanMapping::get).collect(Collectors.toList());
            int start = spans.stream().map(Pair::getLeft).min(Integer::compareTo).get();
            int end = spans.stream().map(Pair::getRight).max(Integer::compareTo).get();
            patternSpanMapping.put(patternToken, Pair.of(start, end));
        }
        return patternSpanMapping;
    }

    /** Parses formatter into format tokens (time fields and literals). */
    private List<FormatToken> parseFormatter() {
        List<FormatToken> tokens = new ArrayList<>();
        for (int pos = 0; pos < formatter.length(); pos++) {
            char c = formatter.charAt(pos);
            if (isTimeChar(c)) {
                int start = pos;
                while (pos < formatter.length() && formatter.charAt(pos) == c) {
                    pos++;
                }
                ChronoField field = FIELD_MAP.get(c);
                tokens.add(new TimeFieldToken(field, start, pos));
                pos--;
            } else if (c == '\'') {
                // parse literals
                int start = pos++;
                for (; pos < formatter.length(); pos++) {
                    if (formatter.charAt(pos) == '\'') {
                        if (pos + 1 < formatter.length() && formatter.charAt(pos + 1) == '\'') {
                            pos++;
                        } else {
                            break; // end of literal
                        }
                    }
                }
                checkArgument(
                        pos < formatter.length(),
                        "Pattern ends with an incomplete string literal: " + formatter);
                String str = formatter.substring(start + 1, pos);
                if (str.isEmpty()) {
                    tokens.add(new LiteralToken("'", start, pos + 1));
                } else {
                    tokens.add(new LiteralToken(str.replace("''", "'"), start, pos + 1));
                }
            } else {
                String text = String.valueOf(c);
                tokens.add(new LiteralToken(text, pos, pos + 1));
            }
        }
        checkArgument(!tokens.isEmpty(), "No time unit found in formatter: %s", formatter);
        return tokens;
    }

    private static boolean isTimeChar(char c) {
        return FIELD_MAP.containsKey(c);
    }

    /** Parses pattern string into pattern tokens (variables and literals). */
    private List<PatternToken> parsePattern() {
        List<PatternToken> tokens = new ArrayList<>();
        int len = pattern.length();
        int cursor = 0;
        int partCursor = 0;
        StringBuilder literalBuf = new StringBuilder();
        while (cursor < len) {
            char curr = pattern.charAt(cursor);
            if (curr == '$') {
                if (literalBuf.length() > 0) {
                    tokens.add(new PatternToken(literalBuf.toString(), false));
                    literalBuf.setLength(0);
                }
                checkArgument(
                        partCursor < partitionColumns.size(),
                        "Extra variable in pattern, exceed partitionColumns count");
                String part = curr + partitionColumns.get(partCursor);
                checkArgument(pattern.substring(cursor).startsWith(part));
                tokens.add(new PatternToken(part, true));
                cursor += part.length();
                partCursor++;
            } else {
                literalBuf.append(curr);
                cursor++;
            }
        }
        if (literalBuf.length() > 0) {
            tokens.add(new PatternToken(literalBuf.toString(), false));
        }
        return tokens;
    }

    /**
     * Recursively matches pattern tokens to format tokens. For variable tokens, greedily consumes
     * consecutive format tokens. For literal tokens, verifies length and content match.
     */
    private boolean matchRecursive(int patternIdx, int formatIdx) {
        if (patternIdx == patternTokens.size()) {
            return formatIdx == formatTokens.size();
        }

        // Remaining format tokens must be at least as many as remaining pattern tokens
        if (formatTokens.size() - formatIdx < patternTokens.size() - patternIdx) {
            return false;
        }

        PatternToken patternToken = patternTokens.get(patternIdx);
        // Max format tokens this pattern token can consume, leaving at least 1 token per remaining
        // pattern token
        int maxLen = formatTokens.size() - formatIdx - (patternTokens.size() - patternIdx - 1);

        for (int len = 1; len <= maxLen; len++) {
            int endSpanIdx = formatIdx + len;
            if (patternToken.isVariable) {
                if (matchRecursive(patternIdx + 1, endSpanIdx)) {
                    patternFormatMappings.put(
                            patternToken, formatTokens.subList(formatIdx, endSpanIdx));
                    return true;
                }
            } else {
                // Literal pattern tokens match 1...len consecutive format tokens, split by token
                // length
                if (matchLiteral(patternToken.token, formatTokens, formatIdx, endSpanIdx)) {
                    if (matchRecursive(patternIdx + 1, endSpanIdx)) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    /**
     * Checks if a literal pattern token matches a sequence of format tokens. Verifies total length
     * and literal content match.
     */
    private static boolean matchLiteral(
            String patternToken, List<FormatToken> formatTokens, int startIdx, int endIdx) {
        int formatTokenTotalLen = 0;
        for (int i = startIdx; i < endIdx; i++) {
            formatTokenTotalLen += formatTokens.get(i).getLength();
        }
        if (patternToken.length() != formatTokenTotalLen) {
            return false;
        }

        int pos = 0;
        for (int i = startIdx; i < endIdx; i++) {
            FormatToken span = formatTokens.get(i);
            int spanLen = span.getLength();
            String sub = patternToken.substring(pos, pos + spanLen);

            if (span instanceof LiteralToken) {
                if (!((LiteralToken) span).token.equals(sub)) {
                    return false;
                }
            }
            // TimeFieldToken: length already verified, content is unrestricted
            pos += spanLen;
        }
        return true;
    }

    private static TemporalAmount stepOf(ChronoField field) {
        switch (field) {
            case SECOND_OF_MINUTE:
                return Duration.ofSeconds(1);
            case MINUTE_OF_HOUR:
                return Duration.ofMinutes(1);
            case HOUR_OF_DAY:
            case CLOCK_HOUR_OF_AMPM:
                return Duration.ofHours(1);
            case DAY_OF_MONTH:
                return Duration.ofDays(1);
            case MONTH_OF_YEAR:
                return Period.ofMonths(1);
            case YEAR:
                return Period.ofYears(1);
            default:
                throw new IllegalStateException("Unsupported field: " + field);
        }
    }

    private static class FormatToken {
        final int start;
        final int end;

        private FormatToken(int start, int end) {
            this.start = start;
            this.end = end;
        }

        public int getLength() {
            return end - start;
        }
    }

    private static class LiteralToken extends FormatToken {
        final String token;

        LiteralToken(String token, int start, int end) {
            super(start, end);
            this.token = token;
        }

        @Override
        public int getLength() {
            return token.length();
        }

        @Override
        public String toString() {
            return String.format("LiteralToken(token=%s, start=%d, end=%d)", token, start, end);
        }
    }

    private static class TimeFieldToken extends FormatToken {
        final ChronoField field;

        TimeFieldToken(ChronoField field, int start, int end) {
            super(start, end);
            this.field = field;
        }

        @Override
        public String toString() {
            return String.format("TimeFieldToken(field=%s, start=%d, end=%d)", field, start, end);
        }
    }

    private static class PatternToken {
        final String token;
        final boolean isVariable;

        PatternToken(String token, boolean isVariable) {
            this.token = token;
            this.isVariable = isVariable;
        }

        @Override
        public String toString() {
            return String.format("PatternToken(token='%s', isVariable=%s)", token, isVariable);
        }
    }
}
