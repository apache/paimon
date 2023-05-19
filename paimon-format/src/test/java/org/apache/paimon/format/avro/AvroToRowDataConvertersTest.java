/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.format.avro;

import org.apache.paimon.data.Timestamp;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Instant;

public class AvroToRowDataConvertersTest {
    private static Timestamp now = Timestamp.now();
    private static long ts_millis = now.getMillisecond();
    private static long ts_micros = now.toMicros() + 123L;
    private static Timestamp now_micros = Timestamp.fromMicros(ts_micros);
    private static Instant instant = Instant.ofEpochMilli(ts_millis);

    @Test
    public void testConvertToTimestamp() {
        Assertions.assertEquals(now, AvroToRowDataConverters.convertToTimestamp(ts_millis, 3));

        Assertions.assertEquals(
                now_micros, AvroToRowDataConverters.convertToTimestamp(ts_micros, 6));

        Assertions.assertEquals(now, AvroToRowDataConverters.convertToTimestamp(instant, 3));

        Assertions.assertEquals(now, AvroToRowDataConverters.convertToTimestamp(instant, 6));
    }

    @Test
    public void testConvertToOffsetTimestampFromMillis() {
        Assertions.assertEquals(
                now, AvroToRowDataConverters.convertToOffsetTimestampFromMillis(ts_millis));

        Assertions.assertEquals(
                now, AvroToRowDataConverters.convertToOffsetTimestampFromMillis(instant));

        Assertions.assertThrowsExactly(
                IllegalArgumentException.class,
                () -> AvroToRowDataConverters.convertToOffsetTimestampFromMillis(null));
    }

    @Test
    public void testConvertToOffsetTimestampFromMicros() {
        Assertions.assertEquals(
                now_micros, AvroToRowDataConverters.convertToOffsetTimestampFromMicros(ts_micros));

        Assertions.assertEquals(
                now, AvroToRowDataConverters.convertToOffsetTimestampFromMicros(instant));

        Assertions.assertThrowsExactly(
                IllegalArgumentException.class,
                () -> AvroToRowDataConverters.convertToOffsetTimestampFromMicros(null));
    }
}
