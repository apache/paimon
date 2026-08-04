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

import javax.annotation.Nullable;

import java.text.ParsePosition;
import java.time.DateTimeException;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Period;
import java.time.Year;
import java.time.YearMonth;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.format.ResolverStyle;
import java.time.format.SignStyle;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.TemporalAmount;
import java.time.temporal.TemporalField;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * Pattern-based implementation of {@link PartitionTimeResolvable}. It matches the user-provided
 * timestamp pattern against the formatter and supports bidirectional conversion between partition
 * values and {@link LocalDateTime}.
 */
public class PartitionTimeResolver implements PartitionTimeResolvable {
    private static final Map<Character, ChronoField> FIELD_MAP = new HashMap<>();
    private final List<String> partitionKeys;
    private final String pattern;
    private final String formatter;
    private Map<PatternToken, List<FormatToken>> patternFormatMappings;
    private List<PatternToken> patternTokens;
    private List<FormatToken> formatTokens;

    public PartitionTimeResolver(List<String> partitionKeys, String pattern, String formatter) {
        checkArgument(pattern != null, "pattern cannot be null");
        checkArgument(formatter != null, "formatter cannot be null");
        checkArgument(partitionKeys != null, "partitionKeys cannot be null");
        this.partitionKeys = partitionKeys;
        this.pattern = pattern;
        this.formatter = formatter;
        init();
    }

    /**
     * Creates a fallback resolver used when the user has not configured {@code
     * partition.timestamp-pattern} or {@code partition.timestamp-formatter}. The fallback supports
     * the common unconfigured case.
     */
    static PartitionTimeResolvable createFallback(
            List<String> partitionKeys, String pattern, String formatter) {
        return new FallbackPartitionTimeResolver(partitionKeys, pattern, formatter);
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
        this.patternTokens = parsePattern(partitionKeys, pattern);
        this.formatTokens = parseFormatter();
        boolean matched = matchRecursive(0, 0);
        checkArgument(
                matched, "Failed to match pattern '%s' to formatter '%s'", pattern, formatter);
    }

    @Override
    public List<String> partitionKeys() {
        return partitionKeys;
    }

    /**
     * Extracts the minimum time step from the given pattern and formatter.
     *
     * @return the smallest {@link Duration} or {@link Period} step among variable-controlled time
     *     units
     */
    @Override
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
    @Override
    public LinkedHashMap<String, String> resolvePartitionValues(LocalDateTime dateTime) {
        LinkedHashMap<String, String> result = new LinkedHashMap<>();
        for (PatternToken patternToken : patternTokens) {
            if (!patternToken.isVariable) {
                continue;
            }
            String variableName = patternToken.token.substring(1);
            List<FormatToken> tokens = patternFormatMappings.get(patternToken);
            int start = tokens.get(0).start;
            int end = tokens.get(tokens.size() - 1).end;
            DateTimeFormatter dateTimeFormatter =
                    DateTimeFormatter.ofPattern(formatter.substring(start, end), Locale.ROOT);
            result.put(variableName, dateTime.format(dateTimeFormatter));
        }
        return result;
    }

    @Override
    public LocalDateTime parsePartitionValues(List<?> partitionValues) {
        String timestampString =
                buildTimestampString(partitionKeys, partitionValues, patternTokens);
        DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern(formatter, Locale.ROOT);

        Set<ChronoField> fields =
                formatTokens.stream()
                        .filter(t -> t instanceof TimeFieldToken)
                        .map(t -> ((TimeFieldToken) t).field)
                        .collect(Collectors.toSet());

        if (fields.contains(ChronoField.HOUR_OF_DAY)
                || fields.contains(ChronoField.CLOCK_HOUR_OF_AMPM)
                || fields.contains(ChronoField.MINUTE_OF_HOUR)
                || fields.contains(ChronoField.SECOND_OF_MINUTE)) {
            return LocalDateTime.parse(timestampString, dateTimeFormatter);
        }
        if (fields.contains(ChronoField.DAY_OF_MONTH)) {
            return LocalDate.parse(timestampString, dateTimeFormatter).atStartOfDay();
        }
        if (fields.contains(ChronoField.MONTH_OF_YEAR)) {
            return YearMonth.parse(timestampString, dateTimeFormatter).atDay(1).atStartOfDay();
        }
        if (fields.contains(ChronoField.YEAR)) {
            return Year.parse(timestampString, dateTimeFormatter)
                    .atMonth(1)
                    .atDay(1)
                    .atStartOfDay();
        }
        throw new IllegalStateException("No time field found in formatter");
    }

    /**
     * Builds the timestamp string by substituting partition column values into the pattern tokens.
     */
    private static String buildTimestampString(
            List<String> partitionKeys, List<?> partitionValues, List<PatternToken> patternTokens) {
        checkArgument(partitionValues != null, "Values cannot be null");

        Map<String, Object> valueMap = new HashMap<>();
        for (int i = 0; i < partitionKeys.size(); i++) {
            valueMap.put(partitionKeys.get(i), partitionValues.get(i));
        }
        checkArgument(partitionValues.size() == valueMap.size(), "Values size mismatch");

        StringBuilder timestampString = new StringBuilder();
        for (PatternToken token : patternTokens) {
            if (token.isVariable) {
                timestampString.append(valueMap.get(token.token.substring(1)));
            } else {
                timestampString.append(token.token);
            }
        }
        return timestampString.toString();
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
    private static List<PatternToken> parsePattern(List<String> partitionKeys, String pattern) {
        List<String> sortedPartCols =
                partitionKeys.stream()
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
                if (minFormattedLength(formatIdx, formatEndIdx) > patternToken.token.length()) {
                    continue;
                }
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

    /** Checks if a literal pattern token matches a sequence of format tokens. */
    private boolean matchLiteral(String literalToken, int startIdx, int endIdx) {
        StringBuilder subFormatter = new StringBuilder();
        StringBuilder literalValue = new StringBuilder();
        boolean pureLiteral = true;
        for (int i = startIdx; i < endIdx; i++) {
            FormatToken token = formatTokens.get(i);
            subFormatter.append(formatter, token.start, token.end);
            pureLiteral = pureLiteral && token instanceof LiteralToken;
            if (pureLiteral) {
                literalValue.append(((LiteralToken) token).token);
            }
        }

        if (pureLiteral) {
            return literalToken.contentEquals(literalValue);
        }

        DateTimeFormatter fmt = DateTimeFormatter.ofPattern(subFormatter.toString(), Locale.ROOT);
        ParsePosition pp = new ParsePosition(0);
        try {
            TemporalAccessor ta = fmt.parse(literalToken, pp);
            if (pp.getErrorIndex() >= 0 || pp.getIndex() != literalToken.length()) {
                return false;
            }
            for (TemporalField field : FIELD_MAP.values()) {
                if (ta.isSupported(field)) {
                    try {
                        ta.get(field);
                    } catch (DateTimeException ignored) {
                        return false;
                    }
                }
            }
        } catch (Exception ignored) {
            return false;
        }
        return true;
    }

    /** Minimum formatted length of the given format-token range; used to prune literal matches. */
    private int minFormattedLength(int startIdx, int endIdx) {
        int length = 0;
        for (int i = startIdx; i < endIdx; i++) {
            FormatToken token = formatTokens.get(i);
            if (token instanceof TimeFieldToken) {
                TimeFieldToken t = (TimeFieldToken) token;
                // Text month forms (MMMM/MMMMM) output variable-length strings whose length
                // depends on the month value and locale. For example, MMMM outputs "May" (3) in
                // English but "五月" (2) in Chinese; MMMMM can output a single character.
                // Use a conservative lower bound of 1 to avoid pruning valid matches.
                if (t.field == ChronoField.MONTH_OF_YEAR && token.getLength() >= 3) {
                    length += 1;
                    continue;
                }
            }
            length += token.getLength();
        }
        return length;
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

    /**
     * Fallback resolver for the unconfigured case. When {@code partition.timestamp-pattern} or
     * {@code partition.timestamp-formatter} is missing, pattern defaults to the first partition
     * column and formatter defaults to {@code yyyy-MM-dd HH:mm:ss} / {@code yyyy-MM-dd}.
     */
    private static class FallbackPartitionTimeResolver implements PartitionTimeResolvable {
        private static final DateTimeFormatter TIMESTAMP_FORMATTER =
                new DateTimeFormatterBuilder()
                        .appendValue(ChronoField.YEAR, 1, 10, SignStyle.NORMAL)
                        .appendLiteral('-')
                        .appendValue(ChronoField.MONTH_OF_YEAR, 1, 2, SignStyle.NORMAL)
                        .appendLiteral('-')
                        .appendValue(ChronoField.DAY_OF_MONTH, 1, 2, SignStyle.NORMAL)
                        .optionalStart()
                        .appendLiteral(" ")
                        .appendValue(ChronoField.HOUR_OF_DAY, 1, 2, SignStyle.NORMAL)
                        .appendLiteral(':')
                        .appendValue(ChronoField.MINUTE_OF_HOUR, 1, 2, SignStyle.NORMAL)
                        .appendLiteral(':')
                        .appendValue(ChronoField.SECOND_OF_MINUTE, 1, 2, SignStyle.NORMAL)
                        .optionalStart()
                        .appendFraction(ChronoField.NANO_OF_SECOND, 1, 9, true)
                        .optionalEnd()
                        .optionalEnd()
                        .toFormatter()
                        .withResolverStyle(ResolverStyle.LENIENT);

        private static final DateTimeFormatter DATE_FORMATTER =
                new DateTimeFormatterBuilder()
                        .appendValue(ChronoField.YEAR, 1, 10, SignStyle.NORMAL)
                        .appendLiteral('-')
                        .appendValue(ChronoField.MONTH_OF_YEAR, 1, 2, SignStyle.NORMAL)
                        .appendLiteral('-')
                        .appendValue(ChronoField.DAY_OF_MONTH, 1, 2, SignStyle.NORMAL)
                        .toFormatter()
                        .withResolverStyle(ResolverStyle.LENIENT);

        private final List<String> partitionKeys;
        @Nullable private final String pattern;
        @Nullable private final String formatter;

        FallbackPartitionTimeResolver(
                List<String> partitionKeys, @Nullable String pattern, @Nullable String formatter) {
            checkArgument(partitionKeys != null, "partitionKeys cannot be null");
            this.partitionKeys = partitionKeys;
            this.pattern = pattern;
            this.formatter = formatter;
        }

        @Override
        public List<String> partitionKeys() {
            return partitionKeys;
        }

        @Override
        public LocalDateTime parsePartitionValues(List<?> partitionValues) {
            checkArgument(partitionValues != null, "Values cannot be null");
            String timestampString;
            if (pattern == null) {
                timestampString = partitionValues.get(0).toString();
            } else {
                timestampString =
                        buildTimestampString(
                                partitionKeys,
                                partitionValues,
                                parsePattern(partitionKeys, pattern));
            }
            return toLocalDateTime(timestampString, formatter);
        }

        private static LocalDateTime toLocalDateTime(
                String timestampString, @Nullable String formatterPattern) {
            if (formatterPattern == null) {
                try {
                    return LocalDateTime.parse(timestampString, TIMESTAMP_FORMATTER);
                } catch (DateTimeParseException e) {
                    return LocalDateTime.of(
                            LocalDate.parse(timestampString, DATE_FORMATTER), LocalTime.MIDNIGHT);
                }
            }
            DateTimeFormatter dateTimeFormatter =
                    DateTimeFormatter.ofPattern(formatterPattern, Locale.ROOT);
            try {
                return LocalDateTime.parse(timestampString, dateTimeFormatter);
            } catch (DateTimeParseException e) {
                return LocalDateTime.of(
                        LocalDate.parse(timestampString, dateTimeFormatter), LocalTime.MIDNIGHT);
            }
        }
    }
}
