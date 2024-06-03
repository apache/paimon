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

import javax.annotation.Nullable;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Optional;

/** Extractor to extract tag time from {@link Snapshot}. */
public interface TagTimeExtractor {

    Optional<LocalDateTime> extract(long timeMilli, @Nullable Long watermark);

    /** Extract time from snapshot time millis. */
    class ProcessTimeExtractor implements TagTimeExtractor {

        @Override
        public Optional<LocalDateTime> extract(long timeMilli, @Nullable Long watermark) {
            return Optional.of(
                    Instant.ofEpochMilli(timeMilli)
                            .atZone(ZoneId.systemDefault())
                            .toLocalDateTime());
        }
    }

    /** Extract time from snapshot watermark. */
    class WatermarkExtractor implements TagTimeExtractor {

        private final ZoneId watermarkZoneId;

        private WatermarkExtractor(ZoneId watermarkZoneId) {
            this.watermarkZoneId = watermarkZoneId;
        }

        @Override
        public Optional<LocalDateTime> extract(long timeMilli, @Nullable Long watermark) {
            if (watermark == null || watermark < 0) {
                return Optional.empty();
            }

            return Optional.of(
                    Instant.ofEpochMilli(watermark).atZone(watermarkZoneId).toLocalDateTime());
        }
    }

    @Nullable
    static TagTimeExtractor createForAutoTag(CoreOptions options) {
        return create(options.tagCreationMode(), options);
    }

    @Nullable
    static TagTimeExtractor createForTagPreview(CoreOptions options) {
        return create(options.tagToPartitionPreview(), options);
    }

    @Nullable
    static TagTimeExtractor create(CoreOptions.TagCreationMode mode, CoreOptions options) {
        switch (mode) {
            case NONE:
            case BATCH:
                return null;
            case PROCESS_TIME:
                return new ProcessTimeExtractor();
            case WATERMARK:
                return new WatermarkExtractor(ZoneId.of(options.sinkWatermarkTimeZone()));
            default:
                throw new UnsupportedOperationException("Unsupported " + options.tagCreationMode());
        }
    }
}
