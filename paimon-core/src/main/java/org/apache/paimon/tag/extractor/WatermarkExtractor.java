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

package org.apache.paimon.tag.extractor;

import org.apache.paimon.Snapshot;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Optional;

/**
 * The WatermarkExtractor class is an implementation of the TimeExtractor interface. It extracts a
 * LocalDateTime from a given Snapshot using its watermark field.
 *
 * <p>The time zone for the watermark can be customized through the constructor.
 */
public class WatermarkExtractor implements TimeExtractor {

    private final ZoneId watermarkZoneId;

    public WatermarkExtractor(ZoneId watermarkZoneId) {
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
