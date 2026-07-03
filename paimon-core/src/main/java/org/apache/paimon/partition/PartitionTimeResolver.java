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

package org.apache.paimon.partition;

import org.apache.paimon.utils.Pair;

import java.time.DateTimeException;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Period;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAmount;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * Resolves timestamp pattern and formatter to extract time step and compute partition values for
 * chain table partitions.
 */
public class PartitionTimeResolver {
    private static final Map<Character, ChronoField> FIELD_MAP = new HashMap<>();
    private final List<String> partitionColumns;
    private final String pattern;
    private final String formatter;
    private Map<PatternToken, List<FormatToken>> patternFormatMappings;
    private Map<PatternToken, Pair<Integer, Integer>> patternSpanMappings;
    private List<PatternToken> patternTokens;
    private List<FormatToken> formatTokens;

    public PartitionTimeResolver(List<String> partitionColumns, String pattern, String formatter) {
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
    public LinkedHashMap<String, String> resolvePartitionValues(LocalDateTime dateTime) {
        DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern(formatter, Locale.ROOT);
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

    public LocalDateTime parsePartitionValues(List<?> partitionValues) {
        Map<String, Object> valueMap = new HashMap<>();
        for (int i = 0; i < partitionColumns.size(); i++) {
            valueMap.put(partitionColumns.get(i), partitionValues.get(i));
        }

        StringBuilder timestampString = new StringBuilder();
        for (PatternToken token : patternTokens) {
            if (token.isVariable) {
                timestampString.append(valueMap.get(token.token.substring(1)));
            } else {
                timestampString.append(token.token);
            }
        }
        DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern(formatter, Locale.ROOT);
        try {
            return LocalDateTime.parse(timestampString, dateTimeFormatter);
        } catch (DateTimeParseException e) {
            return LocalDateTime.of(
                    LocalDate.parse(timestampString, dateTimeFormatter), LocalTime.MIDNIGHT);
        }
    }

    private Map<PatternToken, Pair<Integer, Integer>> calPatternSpanMappings() {
        int pos = 0;
        Map<FormatToken, Integer> startPositions = new LinkedHashMap<>();
        for (FormatToken token : formatTokens) {
            startPositions.put(token, pos);
            pos += token.getLength();
        }

        Map<PatternToken, Pair<Integer, Integer>> patternSpanMapping = new LinkedHashMap<>();
        for (PatternToken patternToken : patternTokens) {
            if (!patternToken.isVariable) {
                continue;
            }
            List<FormatToken> tokens = patternFormatMappings.get(patternToken);
            int start = startPositions.get(tokens.get(0));
            int end = start;
            for (FormatToken token : tokens) {
                end += token.getLength();
            }
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
            } else if (Character.isLetter(c)) {
                throw new IllegalArgumentException(
                        String.format(
                                "Unsupported formatter pattern letter '%s' in formatter: %s.",
                                c, formatter));
            } else {
                tokens.add(new LiteralToken(String.valueOf(c), pos, pos + 1));
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
        List<String> sortedPartCols =
                partitionColumns.stream()
                        .sorted(Comparator.reverseOrder())
                        .collect(Collectors.toList());

        List<PatternToken> tokens = new ArrayList<>();
        StringBuilder literalBuf = new StringBuilder();
        for (int cursor = 0, len = pattern.length(); cursor < len; ) {
            char curr = pattern.charAt(cursor);
            if (curr == '$') {
                if (literalBuf.length() > 0) {
                    tokens.add(new PatternToken(literalBuf.toString(), false));
                    literalBuf.setLength(0);
                }
                boolean matched = false;
                // Match the longest column name first to resolve ambiguity when one column name
                // is a prefix of another (e.g., "dt" vs "dt1").
                for (String part : sortedPartCols) {
                    String varToken = curr + part;
                    if (pattern.startsWith(varToken, cursor)) {
                        tokens.add(new PatternToken(varToken, true));
                        cursor += varToken.length();
                        matched = true;
                        break;
                    }
                }
                checkArgument(
                        matched,
                        "Unknown variable in pattern '%s' at position %s",
                        pattern,
                        cursor);
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

        int matchedEndIdx = -1;
        for (int len = 1; len <= maxLen; len++) {
            int formatEndIdx = formatIdx + len;
            if (patternToken.isVariable) {
                if (matchRecursive(patternIdx + 1, formatEndIdx)) {
                    checkArgument(
                            matchedEndIdx == -1,
                            "Ambiguous mapping for pattern variable '%s' in pattern '%s' with formatter '%s'. "
                                    + "Please separate adjacent variables with literals.",
                            patternToken.token,
                            pattern,
                            formatter);
                    matchedEndIdx = formatEndIdx;
                }
            } else {
                // Literal pattern tokens match 1...len consecutive format tokens, split by token
                // length
                if (matchLiteral(patternToken.token, formatIdx, formatEndIdx)) {
                    if (matchRecursive(patternIdx + 1, formatEndIdx)) {
                        return true;
                    }
                }
            }
        }
        if (matchedEndIdx != -1) {
            patternFormatMappings.put(patternToken, formatTokens.subList(formatIdx, matchedEndIdx));
            return true;
        }
        return false;
    }

    /**
     * Checks if a literal pattern token matches a sequence of format tokens. Verifies total length
     * and literal content match.
     */
    private boolean matchLiteral(String patternToken, int startIdx, int endIdx) {
        int formatTokenTotalLen = 0;
        for (int i = startIdx; i < endIdx; i++) {
            formatTokenTotalLen += formatTokens.get(i).getLength();
        }
        if (patternToken.length() != formatTokenTotalLen) {
            return false;
        }

        int pos = 0;
        for (int i = startIdx; i < endIdx; i++) {
            FormatToken token = formatTokens.get(i);
            int tokenLen = token.getLength();
            String sub = patternToken.substring(pos, pos + tokenLen);

            if (token instanceof LiteralToken) {
                if (!((LiteralToken) token).token.equals(sub)) {
                    return false;
                }
            } else {
                ChronoField field = ((TimeFieldToken) token).field;
                try {
                    field.checkValidIntValue(Long.parseLong(sub));
                } catch (NumberFormatException | DateTimeException e) {
                    return false;
                }
            }
            pos += tokenLen;
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
            return String.format("LiteralToken{token=%s, start=%d, end=%d}", token, start, end);
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
            return String.format("TimeFieldToken{field=%s, start=%d, end=%d}", field, start, end);
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
            return String.format("PatternToken{token='%s', isVariable=%s}", token, isVariable);
        }
    }
}
