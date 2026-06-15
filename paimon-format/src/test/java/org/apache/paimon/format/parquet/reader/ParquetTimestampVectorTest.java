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

package org.apache.paimon.format.parquet.reader;

import org.apache.paimon.data.Timestamp;
import org.apache.paimon.data.columnar.heap.HeapLongVector;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

/** Test for {@link ParquetTimestampVector}. */
public class ParquetTimestampVectorTest {

    @Test
    public void testLowPrecisionTimestampDecodesAsMicros() {
        // TIMESTAMP(n<=3) is stored as epoch-microseconds after PR #8230 (MICROS annotation).
        // getTimestamp() must decode via fromMicros(), not fromEpochMillis().
        long epochMicros = 1_718_396_248_123_000L;

        HeapLongVector vec = new HeapLongVector(1);
        vec.setLong(0, epochMicros);
        ParquetTimestampVector tv = new ParquetTimestampVector(vec);

        Assertions.assertThat(tv.getTimestamp(0, 3))
                .isEqualTo(Timestamp.fromMicros(epochMicros));
    }

    @Test
    public void testMidPrecisionTimestampDecodesAsMicros() {
        // Regression: precision 4-6 path must continue to use fromMicros.
        long epochMicros = 1_718_396_248_123_456L;

        HeapLongVector vec = new HeapLongVector(1);
        vec.setLong(0, epochMicros);
        ParquetTimestampVector tv = new ParquetTimestampVector(vec);

        Assertions.assertThat(tv.getTimestamp(0, 6))
                .isEqualTo(Timestamp.fromMicros(epochMicros));
    }

    @Test
    public void testLowPrecisionFromEpochMillisYieldsWrongTimestamp() {
        // Documents the bug: fromEpochMillis(epoch_µs) inflates the timestamp 1000x,
        // placing it in year ~58xxx. fromMicros(epoch_µs) returns the correct wall-clock time.
        long epochMicros = 1_718_396_248_123_000L;

        HeapLongVector vec = new HeapLongVector(1);
        vec.setLong(0, epochMicros);
        ParquetTimestampVector tv = new ParquetTimestampVector(vec);

        Timestamp result = tv.getTimestamp(0, 3);

        Assertions.assertThat(result).isEqualTo(Timestamp.fromMicros(epochMicros));
        Assertions.assertThat(result).isNotEqualTo(Timestamp.fromEpochMillis(epochMicros));
    }
}
