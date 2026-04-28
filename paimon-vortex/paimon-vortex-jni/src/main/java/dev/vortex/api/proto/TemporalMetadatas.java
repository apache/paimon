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

package dev.vortex.api.proto;

import org.apache.paimon.shade.guava30.com.google.common.base.Preconditions;
import org.apache.paimon.shade.guava30.com.google.common.base.Supplier;
import org.apache.paimon.shade.guava30.com.google.common.base.Suppliers;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Optional;

/**
 * Utility class for creating and parsing temporal metadata in Vortex protocol buffers.
 * Provides constants for time units and methods for serializing/deserializing timestamp
 * metadata including time zone information. This class handles the binary format used
 * to encode temporal type information in the Vortex data format.
 */
public final class TemporalMetadatas {
    private TemporalMetadatas() {}

    /** Time unit constant representing nanoseconds precision. */
    public static byte TIME_UNIT_NANOS = 0;
    /** Time unit constant representing microseconds precision. */
    public static byte TIME_UNIT_MICROS = 1;
    /** Time unit constant representing milliseconds precision. */
    public static byte TIME_UNIT_MILLIS = 2;
    /** Time unit constant representing seconds precision. */
    public static byte TIME_UNIT_SECONDS = 3;
    /** Time unit constant representing days precision. */
    public static byte TIME_UNIT_DAYS = 4;

    /** Supplier for date metadata with days precision. */
    public static final Supplier<byte[]> DATE_DAYS = Suppliers.memoize(() -> new byte[] {TIME_UNIT_DAYS});
    /** Supplier for date metadata with milliseconds precision. */
    public static final Supplier<byte[]> DATE_MILLIS = Suppliers.memoize(() -> new byte[] {TIME_UNIT_MILLIS});
    /** Supplier for time metadata with seconds precision. */
    public static final Supplier<byte[]> TIME_SECONDS = Suppliers.memoize(() -> new byte[] {TIME_UNIT_SECONDS});
    /** Supplier for time metadata with milliseconds precision. */
    public static final Supplier<byte[]> TIME_MILLIS = Suppliers.memoize(() -> new byte[] {TIME_UNIT_MILLIS});
    /** Supplier for time metadata with microseconds precision. */
    public static final Supplier<byte[]> TIME_MICROS = Suppliers.memoize(() -> new byte[] {TIME_UNIT_MICROS});
    /** Supplier for time metadata with nanoseconds precision. */
    public static final Supplier<byte[]> TIME_NANOS = Suppliers.memoize(() -> new byte[] {TIME_UNIT_NANOS});

    /**
     * Creates serialized timestamp metadata with the specified time unit and optional time zone.
     * The resulting byte array contains the time unit as the first byte, followed by a little-endian
     * uint16 length field, and then the UTF-8 encoded time zone string (if present).
     *
     * @param timeUnit the time unit for the timestamp, must be between TIME_UNIT_NANOS and TIME_UNIT_SECONDS (exclusive of TIME_UNIT_DAYS)
     * @param timeZone optional time zone identifier string
     * @return serialized timestamp metadata as a byte array
     * @throws RuntimeException if timeUnit is invalid for timestamps
     */
    public static byte[] timestamp(byte timeUnit, Optional<String> timeZone) {
        Preconditions.checkArgument(
                timeUnit >= TIME_UNIT_NANOS && timeUnit < TIME_UNIT_DAYS, "invalid timeUnit for Timestamp:" + timeUnit);

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        baos.write(timeUnit);
        if (timeZone.isPresent()) {
            byte[] timeZoneBytes = timeZone.get().getBytes(StandardCharsets.UTF_8);
            // Write length as little-endian uint16.
            int lenLow = timeZoneBytes.length & 0xFF;
            int lenHigh = (timeZoneBytes.length >> 8) & 0xFF;
            baos.write(lenLow);
            baos.write(lenHigh);
            baos.writeBytes(timeZoneBytes);
        } else {
            // write uint16 zero value
            baos.write(0);
            baos.write(0);
        }
        return baos.toByteArray();
    }

    /**
     * Extract the time unit byte from the serialized metadata.
     *
     * @param serializedMetadata the serialized temporal metadata byte array
     * @return the time unit byte from the first position of the metadata
     * @throws RuntimeException if the time unit byte is invalid
     */
    public static byte getTimeUnit(byte[] serializedMetadata) {
        byte timeUnit = serializedMetadata[0];
        Preconditions.checkArgument(
                timeUnit >= TIME_UNIT_NANOS && timeUnit <= TIME_UNIT_DAYS, "invalid timeUnit byte: " + timeUnit);

        return timeUnit;
    }

    /**
     * Read from a serialized metadata representation into a time zone string.
     * Parses the time zone information from the serialized metadata, reading the length
     * as a little-endian uint16 and then decoding the UTF-8 time zone string.
     *
     * @param serializedMetadata the serialized temporal metadata byte array
     * @return Optional containing the time zone string, or empty if no time zone was specified
     */
    public static Optional<String> getTimeZone(byte[] serializedMetadata) {
        byte _timeUnit = serializedMetadata[0];
        byte lenLow = serializedMetadata[1];
        byte lenHigh = serializedMetadata[2];
        int len = ((lenHigh & 0xFF) << 8) | (lenLow & 0xFF);
        if (len == 0) {
            return Optional.empty();
        } else {
            byte[] timeZoneBytes = new byte[len];
            System.arraycopy(serializedMetadata, 3, timeZoneBytes, 0, len);
            return Optional.of(new String(timeZoneBytes, StandardCharsets.UTF_8));
        }
    }
}
