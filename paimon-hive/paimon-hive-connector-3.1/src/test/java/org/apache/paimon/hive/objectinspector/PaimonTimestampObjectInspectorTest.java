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

package org.apache.paimon.hive.objectinspector;

import org.apache.hadoop.hive.common.type.Timestamp;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link PaimonTimestampObjectInspector}. */
public class PaimonTimestampObjectInspectorTest {

    /**
     * Builds the expected Hive {@link Timestamp} from a {@link LocalDateTime} through an
     * independent path (epoch-second + nano-of-second, both interpreted at UTC just like Hive's
     * {@code ofEpochSecond}), so the assertion is derived rather than a copy of the production
     * arithmetic under test.
     */
    private static Timestamp hiveTimestampOf(LocalDateTime localDateTime) {
        Instant instant = localDateTime.toInstant(ZoneOffset.UTC);
        return Timestamp.ofEpochSecond(instant.getEpochSecond(), instant.getNano());
    }

    @Test
    public void testGetPrimitiveJavaObjectPreEpochWithFraction() {
        PaimonTimestampObjectInspector oi = new PaimonTimestampObjectInspector();

        // Pre-1970 value with a fractional second: Paimon stores a negative millisecond, so the
        // old `millis % 1000` produced a negative nano-of-second and Hive's ofEpochMilli threw
        // DateTimeException. This must now round-trip without throwing.
        LocalDateTime localDateTime = LocalDateTime.of(1969, 12, 31, 23, 59, 59, 500_000_000);
        org.apache.paimon.data.Timestamp input =
                org.apache.paimon.data.Timestamp.fromLocalDateTime(localDateTime);

        assertThat(oi.getPrimitiveJavaObject(input)).isEqualTo(hiveTimestampOf(localDateTime));
    }

    @Test
    public void testGetPrimitiveJavaObjectPostEpochWithFraction() {
        PaimonTimestampObjectInspector oi = new PaimonTimestampObjectInspector();

        // Post-1970 fractional value: positive path is unchanged by the fix.
        LocalDateTime localDateTime = LocalDateTime.of(2023, 1, 2, 3, 4, 5, 123_456_789);
        org.apache.paimon.data.Timestamp input =
                org.apache.paimon.data.Timestamp.fromLocalDateTime(localDateTime);

        assertThat(oi.getPrimitiveJavaObject(input)).isEqualTo(hiveTimestampOf(localDateTime));
    }

    @Test
    public void testGetPrimitiveJavaObjectNull() {
        PaimonTimestampObjectInspector oi = new PaimonTimestampObjectInspector();

        assertThat(oi.getPrimitiveJavaObject(null)).isNull();
    }
}
