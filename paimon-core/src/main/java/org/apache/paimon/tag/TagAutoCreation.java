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

package org.apache.paimon.tag;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.operation.TagDeletion;
import org.apache.paimon.utils.SnapshotManager;
import org.apache.paimon.utils.TagManager;

import javax.annotation.Nullable;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.ResolverStyle;
import java.time.format.SignStyle;
import java.util.Optional;
import java.util.SortedMap;

import static java.time.temporal.ChronoField.DAY_OF_MONTH;
import static java.time.temporal.ChronoField.HOUR_OF_DAY;
import static java.time.temporal.ChronoField.MONTH_OF_YEAR;
import static java.time.temporal.ChronoField.YEAR;
import static org.apache.paimon.Snapshot.FIRST_SNAPSHOT_ID;
import static org.apache.paimon.shade.guava30.com.google.common.base.MoreObjects.firstNonNull;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/** A manager to create tags automatically. */
public class TagAutoCreation {

    private static final DateTimeFormatter HOUR_FORMATTER =
            new DateTimeFormatterBuilder()
                    .appendValue(YEAR, 1, 10, SignStyle.NORMAL)
                    .appendLiteral('-')
                    .appendValue(MONTH_OF_YEAR, 2, 2, SignStyle.NORMAL)
                    .appendLiteral('-')
                    .appendValue(DAY_OF_MONTH, 2, 2, SignStyle.NORMAL)
                    .appendLiteral(" ")
                    .appendValue(HOUR_OF_DAY, 2, 2, SignStyle.NORMAL)
                    .toFormatter()
                    .withResolverStyle(ResolverStyle.LENIENT);

    private static final DateTimeFormatter DAY_FORMATTER =
            new DateTimeFormatterBuilder()
                    .appendValue(YEAR, 1, 10, SignStyle.NORMAL)
                    .appendLiteral('-')
                    .appendValue(MONTH_OF_YEAR, 2, 2, SignStyle.NORMAL)
                    .appendLiteral('-')
                    .appendValue(DAY_OF_MONTH, 2, 2, SignStyle.NORMAL)
                    .toFormatter()
                    .withResolverStyle(ResolverStyle.LENIENT);

    private final SnapshotManager snapshotManager;
    private final TagManager tagManager;
    private final TagDeletion tagDeletion;
    private final TimeExtractor timeExtractor;
    private final TagPeriodHandler periodHandler;
    private final Duration delay;
    private final Integer numRetainedMax;
    private LocalDateTime nextTag;
    private long nextSnapshot;

    private TagAutoCreation(
            SnapshotManager snapshotManager,
            TagManager tagManager,
            TagDeletion tagDeletion,
            TimeExtractor timeExtractor,
            TagPeriodHandler periodHandler,
            Duration delay,
            Integer numRetainedMax) {
        this.snapshotManager = snapshotManager;
        this.tagManager = tagManager;
        this.tagDeletion = tagDeletion;
        this.timeExtractor = timeExtractor;
        this.periodHandler = periodHandler;
        this.delay = delay;
        this.numRetainedMax = numRetainedMax;

        this.periodHandler.validateDelay(delay);

        SortedMap<Snapshot, String> tags = tagManager.tags();

        if (tags.isEmpty()) {
            this.nextSnapshot =
                    firstNonNull(snapshotManager.earliestSnapshotId(), FIRST_SNAPSHOT_ID);
        } else {
            Snapshot lastTag = tags.lastKey();
            this.nextSnapshot = lastTag.id() + 1;

            LocalDateTime time = periodHandler.tagToTime(tags.get(lastTag));
            this.nextTag = periodHandler.nextTagTime(time);
        }
    }

    public void refreshTags() {
        Long snapshotId = Optional.ofNullable(snapshotManager.latestSnapshotId()).orElse(1L);
        LocalDateTime currentTime =
                Instant.ofEpochMilli(System.currentTimeMillis())
                        .atZone(ZoneId.systemDefault())
                        .toLocalDateTime();
        processTags(snapshotManager.snapshot(snapshotId), Optional.of(currentTime));
    }

    public void run() {
        while (true) {
            if (snapshotManager.snapshotExists(nextSnapshot)) {
                Snapshot snapshot = snapshotManager.snapshot(nextSnapshot);
                processTags(snapshot, timeExtractor.extract(snapshot));
                nextSnapshot++;
            } else {
                // avoid snapshot has been expired
                Long earliest = snapshotManager.earliestSnapshotId();
                if (earliest != null && earliest > nextSnapshot) {
                    nextSnapshot = earliest;
                } else {
                    break;
                }
            }
        }
    }

    private void processTags(Snapshot snapshot, Optional<LocalDateTime> timeOptional) {
        timeOptional.ifPresent(time -> createTags(snapshot, time));
        pruneOldTags();
    }

    private void createTags(Snapshot snapshot, LocalDateTime time) {
        if (nextTag == null
                || isAfterOrEqual(time.minus(delay), periodHandler.nextTagTime(nextTag))) {
            LocalDateTime thisTag = periodHandler.normalizeToTagTime(time);
            String tagName = periodHandler.timeToTag(thisTag);
            tagManager.createTag(snapshot, tagName);
            nextTag = periodHandler.nextTagTime(thisTag);
        }
    }

    private void pruneOldTags() {
        if (numRetainedMax == null) {
            return;
        }
        SortedMap<Snapshot, String> tags = tagManager.tags();
        int tagsToDelete = tags.size() - numRetainedMax;
        if (tagsToDelete > 0) {
            tags.values().stream()
                    .limit(tagsToDelete)
                    .forEach(tag -> tagManager.deleteTag(tag, tagDeletion, snapshotManager));
        }
    }

    private boolean isAfterOrEqual(LocalDateTime t1, LocalDateTime t2) {
        return t1.isAfter(t2) || t1.isEqual(t2);
    }

    private interface TimeExtractor {

        Optional<LocalDateTime> extract(Snapshot snapshot);
    }

    private static class ProcessTimeExtractor implements TimeExtractor {

        @Override
        public Optional<LocalDateTime> extract(Snapshot snapshot) {
            return Optional.of(
                    Instant.ofEpochMilli(snapshot.timeMillis())
                            .atZone(ZoneId.systemDefault())
                            .toLocalDateTime());
        }
    }

    private static class WatermarkExtractor implements TimeExtractor {

        private final ZoneId watermarkZoneId;

        private WatermarkExtractor(ZoneId watermarkZoneId) {
            this.watermarkZoneId = watermarkZoneId;
        }

        @Override
        public Optional<LocalDateTime> extract(Snapshot snapshot) {
            Long watermark = snapshot.watermark();
            if (watermark == null) {
                return Optional.empty();
            }

            return Optional.of(
                    Instant.ofEpochMilli(watermark).atZone(watermarkZoneId).toLocalDateTime());
        }
    }

    private interface TagPeriodHandler {

        void validateDelay(Duration delay);

        LocalDateTime tagToTime(String tag);

        LocalDateTime normalizeToTagTime(LocalDateTime time);

        String timeToTag(LocalDateTime time);

        LocalDateTime nextTagTime(LocalDateTime time);
    }

    private abstract static class BaseTagPeriodHandler implements TagPeriodHandler {

        protected abstract Duration onePeriod();

        protected abstract DateTimeFormatter formatter();

        @Override
        public void validateDelay(Duration delay) {
            checkArgument(onePeriod().compareTo(delay) > 0);
        }

        @Override
        public LocalDateTime tagToTime(String tag) {
            return LocalDateTime.parse(tag, formatter());
        }

        @Override
        public LocalDateTime normalizeToTagTime(LocalDateTime time) {
            long mills = Timestamp.fromLocalDateTime(time).getMillisecond();
            long periodMills = onePeriod().toMillis();
            LocalDateTime normalized =
                    Timestamp.fromEpochMillis((mills / periodMills) * periodMills)
                            .toLocalDateTime();
            return normalized.minus(onePeriod());
        }

        @Override
        public String timeToTag(LocalDateTime time) {
            return time.format(formatter());
        }

        @Override
        public LocalDateTime nextTagTime(LocalDateTime time) {
            return time.plus(onePeriod());
        }
    }

    private static class HourlyTagPeriodHandler extends BaseTagPeriodHandler {

        private static final Duration ONE_PERIOD = Duration.ofHours(1);

        @Override
        protected Duration onePeriod() {
            return ONE_PERIOD;
        }

        @Override
        protected DateTimeFormatter formatter() {
            return HOUR_FORMATTER;
        }
    }

    private static class DailyTagPeriodHandler extends BaseTagPeriodHandler {

        private static final Duration ONE_PERIOD = Duration.ofDays(1);

        @Override
        protected Duration onePeriod() {
            return ONE_PERIOD;
        }

        @Override
        protected DateTimeFormatter formatter() {
            return DAY_FORMATTER;
        }

        @Override
        public LocalDateTime tagToTime(String tag) {
            return LocalDate.parse(tag, formatter()).atStartOfDay();
        }
    }

    private static class TwoHoursTagPeriodHandler extends BaseTagPeriodHandler {

        private static final Duration ONE_PERIOD = Duration.ofHours(2);

        @Override
        protected Duration onePeriod() {
            return ONE_PERIOD;
        }

        @Override
        protected DateTimeFormatter formatter() {
            return HOUR_FORMATTER;
        }
    }

    @Nullable
    public static TagAutoCreation create(
            CoreOptions options,
            SnapshotManager snapshotManager,
            TagManager tagManager,
            TagDeletion tagDeletion) {
        TimeExtractor timeExtractor;
        switch (options.tagCreationMode()) {
            case NONE:
                return null;
            case PROCESS_TIME:
                timeExtractor = new ProcessTimeExtractor();
                break;
            case WATERMARK:
                timeExtractor = new WatermarkExtractor(ZoneId.of(options.sinkWatermarkTimeZone()));
                break;
            default:
                throw new UnsupportedOperationException("Unsupported " + options.tagCreationMode());
        }

        TagPeriodHandler periodHandler;
        switch (options.tagCreationPeriod()) {
            case DAILY:
                periodHandler = new DailyTagPeriodHandler();
                break;
            case HOURLY:
                periodHandler = new HourlyTagPeriodHandler();
                break;
            case TWO_HOURS:
                periodHandler = new TwoHoursTagPeriodHandler();
                break;
            default:
                throw new UnsupportedOperationException(
                        "Unsupported " + options.tagCreationPeriod());
        }

        return new TagAutoCreation(
                snapshotManager,
                tagManager,
                tagDeletion,
                timeExtractor,
                periodHandler,
                options.tagCreationDelay(),
                options.tagNumRetainedMax());
    }
}
