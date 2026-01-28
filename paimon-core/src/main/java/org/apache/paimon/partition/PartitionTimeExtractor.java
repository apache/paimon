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

import org.apache.paimon.data.Timestamp;

import javax.annotation.Nullable;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.format.ResolverStyle;
import java.time.format.SignStyle;
import java.time.temporal.ChronoField;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Objects;

import static java.time.temporal.ChronoField.DAY_OF_MONTH;
import static java.time.temporal.ChronoField.HOUR_OF_DAY;
import static java.time.temporal.ChronoField.MINUTE_OF_HOUR;
import static java.time.temporal.ChronoField.MONTH_OF_YEAR;
import static java.time.temporal.ChronoField.SECOND_OF_MINUTE;
import static java.time.temporal.ChronoField.YEAR;

/** Time extractor to extract time from partition values. */
public class PartitionTimeExtractor {

    private static final DateTimeFormatter TIMESTAMP_FORMATTER =
            new DateTimeFormatterBuilder()
                    .appendValue(YEAR, 1, 10, SignStyle.NORMAL)
                    .appendLiteral('-')
                    .appendValue(MONTH_OF_YEAR, 1, 2, SignStyle.NORMAL)
                    .appendLiteral('-')
                    .appendValue(DAY_OF_MONTH, 1, 2, SignStyle.NORMAL)
                    .optionalStart()
                    .appendLiteral(" ")
                    .appendValue(HOUR_OF_DAY, 1, 2, SignStyle.NORMAL)
                    .appendLiteral(':')
                    .appendValue(MINUTE_OF_HOUR, 1, 2, SignStyle.NORMAL)
                    .appendLiteral(':')
                    .appendValue(SECOND_OF_MINUTE, 1, 2, SignStyle.NORMAL)
                    .optionalStart()
                    .appendFraction(ChronoField.NANO_OF_SECOND, 1, 9, true)
                    .optionalEnd()
                    .optionalEnd()
                    .toFormatter()
                    .withResolverStyle(ResolverStyle.LENIENT);

    private static final DateTimeFormatter DATE_FORMATTER =
            new DateTimeFormatterBuilder()
                    .appendValue(YEAR, 1, 10, SignStyle.NORMAL)
                    .appendLiteral('-')
                    .appendValue(MONTH_OF_YEAR, 1, 2, SignStyle.NORMAL)
                    .appendLiteral('-')
                    .appendValue(DAY_OF_MONTH, 1, 2, SignStyle.NORMAL)
                    .toFormatter()
                    .withResolverStyle(ResolverStyle.LENIENT);

    @Nullable private final String pattern;
    @Nullable private final String formatter;

    public PartitionTimeExtractor(@Nullable String pattern, @Nullable String formatter) {
        this.pattern = pattern;
        this.formatter = formatter;
    }

    public LocalDateTime extract(LinkedHashMap<String, String> spec) {
        return extract(new ArrayList<>(spec.keySet()), new ArrayList<>(spec.values()));
    }

    public LocalDateTime extract(List<String> partitionKeys, List<?> partitionValues) {
        // Try to extract directly from typed partition values (DATE/TIMESTAMP)
        if (pattern == null && partitionValues.size() == 1) {
            Object value = partitionValues.get(0);
            LocalDateTime directExtracted = extractFromTypedValue(value);
            if (directExtracted != null) {
                return directExtracted;
            }
        }

        // Handle pattern-based extraction with typed values
        if (pattern != null) {
            for (int i = 0; i < partitionKeys.size(); i++) {
                Object value = partitionValues.get(i);
                LocalDateTime directExtracted = extractFromTypedValue(value);
                if (directExtracted != null) {
                    // For typed values with pattern, replace with formatted string
                    String formattedValue = formatTypedValue(directExtracted);
                    if (formattedValue != null) {
                        String tempPattern =
                                pattern.replaceAll("\\$" + partitionKeys.get(i), formattedValue);
                        // If pattern only contains this one variable, we can use the extracted
                        // value
                        if (!tempPattern.contains("$")) {
                            return toLocalDateTime(tempPattern, this.formatter);
                        }
                    }
                }
            }
        }

        // Fall back to string-based extraction
        String timestampString;
        if (pattern == null) {
            timestampString = partitionValues.get(0).toString();
        } else {
            timestampString = pattern;
            for (int i = 0; i < partitionKeys.size(); i++) {
                timestampString =
                        timestampString.replaceAll(
                                "\\$" + partitionKeys.get(i), partitionValues.get(i).toString());
            }
        }
        return toLocalDateTime(timestampString, this.formatter);
    }

    private static LocalDateTime toLocalDateTime(
            String timestampString, @Nullable String formatterPattern) {

        if (formatterPattern == null) {
            return PartitionTimeExtractor.toLocalDateTimeDefault(timestampString);
        }
        DateTimeFormatter dateTimeFormatter =
                DateTimeFormatter.ofPattern(Objects.requireNonNull(formatterPattern), Locale.ROOT);
        try {
            return LocalDateTime.parse(timestampString, Objects.requireNonNull(dateTimeFormatter));
        } catch (DateTimeParseException e) {
            return LocalDateTime.of(
                    LocalDate.parse(timestampString, Objects.requireNonNull(dateTimeFormatter)),
                    LocalTime.MIDNIGHT);
        }
    }

    public static LocalDateTime toLocalDateTimeDefault(String timestampString) {
        try {
            return LocalDateTime.parse(timestampString, TIMESTAMP_FORMATTER);
        } catch (DateTimeParseException e) {
            return LocalDateTime.of(
                    LocalDate.parse(timestampString, DATE_FORMATTER), LocalTime.MIDNIGHT);
        }
    }

    /**
     * Extract LocalDateTime from typed partition values (Integer for DATE, Timestamp for
     * TIMESTAMP).
     *
     * @param value the partition value object
     * @return LocalDateTime if the value is a supported type, null otherwise
     */
    @Nullable
    private static LocalDateTime extractFromTypedValue(Object value) {
        if (value == null) {
            return null;
        }

        // Handle INTEGER type - represents days since epoch (DATE type)
        if (value instanceof Integer) {
            int daysSinceEpoch = (Integer) value;
            return LocalDate.ofEpochDay(daysSinceEpoch).atStartOfDay();
        }

        // Handle Paimon Timestamp type
        if (value instanceof Timestamp) {
            Timestamp timestamp = (Timestamp) value;
            return timestamp.toLocalDateTime();
        }

        // Handle java.sql.Date
        if (value instanceof java.sql.Date) {
            return ((java.sql.Date) value).toLocalDate().atStartOfDay();
        }

        // Handle java.sql.Timestamp
        if (value instanceof java.sql.Timestamp) {
            return ((java.sql.Timestamp) value).toLocalDateTime();
        }

        // Handle java.time.LocalDate
        if (value instanceof LocalDate) {
            return ((LocalDate) value).atStartOfDay();
        }

        // Handle java.time.LocalDateTime
        if (value instanceof LocalDateTime) {
            return (LocalDateTime) value;
        }

        return null;
    }

    /**
     * Format a LocalDateTime to string for pattern replacement.
     *
     * @param dateTime the LocalDateTime to format
     * @return formatted string, or null if input is null
     */
    @Nullable
    private static String formatTypedValue(LocalDateTime dateTime) {
        if (dateTime == null) {
            return null;
        }
        // Use ISO format: yyyy-MM-dd HH:mm:ss or yyyy-MM-dd if time is midnight
        if (dateTime.toLocalTime().equals(LocalTime.MIDNIGHT)) {
            return dateTime.toLocalDate().toString();
        }
        return dateTime.toString().replace("T", " ");
    }
}
