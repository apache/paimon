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
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * Extracts the time step duration from a timestamp pattern and formatter for chain table
 * partitions.
 */
public class ChainPartitionStepExtractor {

    private static final LocalDateTime FINGERPRINT = LocalDateTime.of(2026, 6, 9, 11, 50, 58);
    private static final String TIME_UNIT_CHARS = "yMdHhmsS";
    private static final Pattern VARIABLE = Pattern.compile("\\$[a-zA-Z_]+");
    private final String pattern;
    private final String formatter;

    public ChainPartitionStepExtractor(String pattern, String formatter) {
        checkArgument(pattern != null, "pattern cannot be null");
        checkArgument(formatter != null, "formatter cannot be null");
        this.pattern = pattern;
        this.formatter = formatter;
    }

    /**
     * Extracts the minimum time step from the given pattern and formatter.
     *
     * @return the smallest {@link Duration} or {@link Period} step among variable-controlled time
     *     units
     */
    public TemporalAmount extractMinStep() {
        List<TimeSpan> spans = parseFormatter(formatter);
        List<Fragment> fragments = splitPattern(pattern);
        List<int[]> varRanges = matchFragments(fragments, formatter);
        ChronoField field = minFieldInRanges(spans, varRanges);
        return stepOf(field);
    }

    /** Parses formatter into time spans with their positions. */
    private static List<TimeSpan> parseFormatter(String formatter) {
        String fingerprint = DateTimeFormatter.ofPattern(formatter).format(FINGERPRINT);

        checkArgument(
                fingerprint.length() == formatter.length(),
                "Formatter with escapes or variable length not supported: %s",
                formatter);

        List<TimeSpan> spans = new ArrayList<>();
        int i = 0;
        while (i < formatter.length()) {
            char c = formatter.charAt(i);
            if (isTimeChar(c)) {
                int start = i;
                while (i < formatter.length() && formatter.charAt(i) == c) {
                    i++;
                }
                spans.add(new TimeSpan(resolveField(fingerprint.substring(start, i)), start, i));
            } else {
                i++;
            }
        }
        checkArgument(!spans.isEmpty(), "No time unit found in formatter: %s", formatter);
        return spans;
    }

    private static boolean isTimeChar(char c) {
        return TIME_UNIT_CHARS.indexOf(c) >= 0;
    }

    private static ChronoField resolveField(String value) {
        long v = Long.parseLong(value);
        if (v == FINGERPRINT.getSecond()) {
            return ChronoField.SECOND_OF_MINUTE;
        }
        if (v == FINGERPRINT.getMinute()) {
            return ChronoField.MINUTE_OF_HOUR;
        }
        if (v == FINGERPRINT.getHour()) {
            return ChronoField.HOUR_OF_DAY;
        }
        if (v == FINGERPRINT.getDayOfMonth()) {
            return ChronoField.DAY_OF_MONTH;
        }
        if (v == FINGERPRINT.getMonthValue()) {
            return ChronoField.MONTH_OF_YEAR;
        }
        if (v == FINGERPRINT.getYear() || v == FINGERPRINT.getYear() % 100) {
            return ChronoField.YEAR;
        }
        throw new IllegalStateException("Unknown time unit value: " + value);
    }

    private static List<Fragment> splitPattern(String pattern) {
        List<Fragment> fragments = new ArrayList<>();
        Matcher m = VARIABLE.matcher(pattern);
        int last = 0;

        while (m.find()) {
            if (m.start() > last) {
                fragments.add(new Fragment(pattern.substring(last, m.start()), false));
            }
            fragments.add(new Fragment(m.group(), true));
            last = m.end();
        }
        if (last < pattern.length()) {
            fragments.add(new Fragment(pattern.substring(last), false));
        }
        return fragments;
    }

    /** Matches constants to formatter positions, then derives variable ranges from gaps. */
    private static List<int[]> matchFragments(List<Fragment> fragments, String formatter) {
        // Step 1: Match all constants sequentially to ensure order and no overlap
        List<int[]> constantRanges = matchAllConstants(fragments, formatter);

        // Step 2: Generate variable ranges from gaps between constants
        int totalLength = formatter.length();
        List<int[]> variableRanges = new ArrayList<>();
        int previousEnd = 0;

        for (int[] constant : constantRanges) {
            int constantStart = constant[0];
            if (constantStart > previousEnd) {
                variableRanges.add(new int[] {previousEnd, constantStart});
            }
            previousEnd = constant[1];
        }

        // Add the range after the last constant
        if (previousEnd < totalLength) {
            variableRanges.add(new int[] {previousEnd, totalLength});
        }

        return variableRanges;
    }

    /**
     * Matches all constant fragments in order, ensuring each constant matches after the previous
     * one.
     */
    private static List<int[]> matchAllConstants(List<Fragment> fragments, String formatter) {
        List<int[]> constantRanges = new ArrayList<>();
        int lastMatchedEnd = 0;
        int formatterLen = formatter.length();

        for (int i = 0; i < fragments.size(); i++) {
            Fragment fragment = fragments.get(i);
            if (!fragment.isVariable) {
                String constant = fragment.text;
                int constLen = constant.length();

                checkArgument(
                        constLen <= formatterLen - lastMatchedEnd,
                        "Constant '%s' exceeds remaining formatter length",
                        constant);

                int pos = findConstantPos(constant, formatter, i, fragments.size(), lastMatchedEnd);
                checkArgument(
                        pos >= 0,
                        "Constant '%s' not found after position %d in formatter",
                        constant,
                        lastMatchedEnd);

                constantRanges.add(new int[] {pos, pos + constLen});
                lastMatchedEnd = pos + constLen;
            }
        }

        return constantRanges;
    }

    private static int findConstantPos(
            String constant, String formatter, int fragIndex, int fragCount, int startFrom) {
        int constLen = constant.length();
        int maxStart = formatter.length() - constLen;
        if (fragIndex == fragCount - 1) {
            // Last fragment: match from the end to ensure it occupies the trailing positions
            for (int s = maxStart; s >= startFrom; s--) {
                if (matchConstant(constant, formatter, s)) {
                    return s;
                }
            }
        } else {
            for (int s = startFrom; s <= maxStart; s++) {
                if (matchConstant(constant, formatter, s)) {
                    return s;
                }
            }
        }
        return -1;
    }

    /**
     * Checks if constant matches formatter at the given position. A digit in the constant must
     * correspond to a time unit character in the formatter, while non-digit characters must match
     * exactly.
     */
    private static boolean matchConstant(String constant, String formatter, int start) {
        for (int i = 0; i < constant.length(); i++) {
            char c = constant.charAt(i);
            char f = formatter.charAt(start + i);
            if (Character.isDigit(c)) {
                if (!isTimeChar(f)) {
                    return false;
                }
            } else if (c != f) {
                return false;
            }
        }
        return true;
    }

    private static ChronoField minFieldInRanges(List<TimeSpan> spans, List<int[]> ranges) {
        ChronoField min = null;
        int start = 0;
        for (int[] range : ranges) {
            for (int i = start; i < spans.size(); i++) {
                TimeSpan span = spans.get(i);
                if (span.start >= range[0] && span.end <= range[1]) {
                    if (min == null || span.field.ordinal() < min.ordinal()) {
                        min = span.field;
                        start = i + 1;
                    }
                }
            }
        }
        checkArgument(min != null, "No time unit found in variable ranges");
        return min;
    }

    private static TemporalAmount stepOf(ChronoField field) {
        switch (field) {
            case SECOND_OF_MINUTE:
                return Duration.ofSeconds(1);
            case MINUTE_OF_HOUR:
                return Duration.ofMinutes(1);
            case HOUR_OF_DAY:
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

    private static class TimeSpan {
        final ChronoField field;
        final int start;
        final int end;

        TimeSpan(ChronoField field, int start, int end) {
            this.field = field;
            this.start = start;
            this.end = end;
        }
    }

    private static class Fragment {
        final String text;
        final boolean isVariable;

        Fragment(String text, boolean isVariable) {
            this.text = text;
            this.isVariable = isVariable;
        }
    }
}
