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

/** Test for avro to row data converters. */
public class AvroToRowDataConvertersTest {
    private static final Timestamp NOW = Timestamp.now();
    private static final long TS_MILLIS = NOW.getMillisecond();
    private static final long TS_MICROS = NOW.toMicros() + 123L;
    private static final Timestamp NOW_MICROS = Timestamp.fromMicros(TS_MICROS);
    private static final Instant INSTANT = Instant.ofEpochMilli(TS_MILLIS);

    @Test
    public void testConvertToTimestamp() {
        Assertions.assertEquals(NOW, AvroToRowDataConverters.convertToTimestamp(TS_MILLIS, 3));

        Assertions.assertEquals(
                NOW_MICROS, AvroToRowDataConverters.convertToTimestamp(TS_MICROS, 6));

        Assertions.assertEquals(NOW, AvroToRowDataConverters.convertToTimestamp(INSTANT, 3));

        Assertions.assertEquals(NOW, AvroToRowDataConverters.convertToTimestamp(INSTANT, 6));
    }
}
