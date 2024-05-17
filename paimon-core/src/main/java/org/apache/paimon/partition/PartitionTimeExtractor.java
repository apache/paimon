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

import org.apache.paimon.operation.FileStoreCommitImpl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.time.temporal.ChronoField.DAY_OF_MONTH;
import static java.time.temporal.ChronoField.HOUR_OF_DAY;
import static java.time.temporal.ChronoField.MINUTE_OF_HOUR;
import static java.time.temporal.ChronoField.MONTH_OF_YEAR;
import static java.time.temporal.ChronoField.SECOND_OF_MINUTE;
import static java.time.temporal.ChronoField.YEAR;

/** Time extractor to extract time from partition values. */
public class PartitionTimeExtractor {

    private static final Logger LOG = LoggerFactory.getLogger(FileStoreCommitImpl.class);
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
        LocalDateTime dateTime = null;
        try {
            String timestampString;
            if (pattern == null) {
                timestampString = partitionValues.get(0).toString();
            } else {
                timestampString = pattern;
                for (int i = 0; i < partitionKeys.size(); i++) {
                    timestampString =
                            timestampString.replaceAll(
                                    "\\$" + partitionKeys.get(i),
                                    partitionValues.get(i).toString());
                }
            }
            dateTime = toLocalDateTime(timestampString, this.formatter);
        } catch (Exception e) {
            String paritionInfos =
                    IntStream.range(0, partitionKeys.size())
                            .mapToObj(i -> partitionKeys.get(i) + ":" + partitionValues.get(i))
                            .collect(Collectors.joining(","));
            LOG.warn(
                    "Parition {} can't be extract datetime to expire,Please check the partition expiration configuration or manually delete the partition using the drop-partition command. ",
                    paritionInfos);
        }
        return dateTime;
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
}
