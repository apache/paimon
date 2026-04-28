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

package dev.vortex;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;

/**
 * Utility class for date and time conversions in Vortex.
 *
 * <p>This class provides helper functions for converting Java date/time objects
 * to nanoseconds since the Unix epoch, which is the internal representation
 * used by Vortex for temporal data.</p>
 *
 * <p>Helpful functions borrowed from Iceberg class with same name.</p>
 */
public final class DateTimeUtil {
    /**
     * The Unix epoch as an OffsetDateTime (1970-01-01T00:00:00Z).
     */
    public static final OffsetDateTime EPOCH = Instant.ofEpochSecond(0).atOffset(ZoneOffset.UTC);

    /**
     * Converts a LocalDateTime to nanoseconds since the Unix epoch.
     *
     * <p>The LocalDateTime is assumed to be in UTC timezone for conversion purposes.
     * When decoded back to LocalDateTime, the timezone information will not be preserved.</p>
     *
     * @param dateTime the LocalDateTime to convert
     * @return nanoseconds since the Unix epoch
     */
    public static long nanosFromTimestamp(LocalDateTime dateTime) {
        return ChronoUnit.NANOS.between(EPOCH, dateTime.atOffset(ZoneOffset.UTC));
    }

    /**
     * Converts an OffsetDateTime to nanoseconds since the Unix epoch.
     *
     * <p>The timezone information in the OffsetDateTime is preserved during conversion.</p>
     *
     * @param dateTime the OffsetDateTime to convert
     * @return nanoseconds since the Unix epoch
     */
    public static long nanosFromTimestamptz(OffsetDateTime dateTime) {
        return ChronoUnit.NANOS.between(EPOCH, dateTime);
    }
}
