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

package org.apache.paimon.fileindex.bloomfilter;

import org.apache.paimon.data.Timestamp;
import org.apache.paimon.types.DataTypes;

import org.junit.jupiter.api.Test;

import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link FastHash.FastHashVisitor}. */
public class FastHashVisitorTest {

    private static final Random RANDOM = new Random();

    @Test
    public void testTinyIntType() {
        FastHash function = FastHash.getHashFunction(DataTypes.TINYINT());
        byte c = (byte) RANDOM.nextInt();
        assertThat(function.hash(c)).isEqualTo(FastHash.FastHashVisitor.getLongHash(c));
    }

    @Test
    public void testSmallIntType() {
        FastHash function = FastHash.getHashFunction(DataTypes.SMALLINT());
        short c = (short) RANDOM.nextInt();
        assertThat(function.hash(c)).isEqualTo(FastHash.FastHashVisitor.getLongHash(c));
    }

    @Test
    public void testIntType() {
        FastHash function = FastHash.getHashFunction(DataTypes.INT());
        int c = RANDOM.nextInt();
        assertThat(function.hash(c))
                .isEqualTo((FastHash.FastHashVisitor.getLongHash(c)));
    }

    @Test
    public void testBigIntType() {
        FastHash function = FastHash.getHashFunction(DataTypes.BIGINT());
        long c = RANDOM.nextLong();
        assertThat(function.hash(c))
                .isEqualTo((FastHash.FastHashVisitor.getLongHash(c)));
    }

    @Test
    public void testFloatType() {
        FastHash function = FastHash.getHashFunction(DataTypes.FLOAT());
        float c = RANDOM.nextFloat();
        assertThat(function.hash(c))
                .isEqualTo((FastHash.FastHashVisitor.getLongHash(Float.floatToIntBits(c))));
    }

    @Test
    public void testDoubleType() {
        FastHash function = FastHash.getHashFunction(DataTypes.DOUBLE());
        double c = RANDOM.nextDouble();
        assertThat(function.hash(c))
                .isEqualTo((FastHash.FastHashVisitor.getLongHash(Double.doubleToLongBits(c))));
    }

    @Test
    public void testDateType() {
        FastHash function = FastHash.getHashFunction(DataTypes.DATE());
        int c = RANDOM.nextInt();
        assertThat(function.hash(c))
                .isEqualTo((FastHash.FastHashVisitor.getLongHash(c)));
    }

    @Test
    public void testTimestampType() {
        FastHash function = FastHash.getHashFunction(DataTypes.TIMESTAMP_MILLIS());
        Timestamp c = Timestamp.fromEpochMillis(System.currentTimeMillis());
        assertThat(function.hash(c))
                .isEqualTo((FastHash.FastHashVisitor.getLongHash(c.getMillisecond())));
    }

    @Test
    public void testLocalZonedTimestampType() {
        FastHash function = FastHash.getHashFunction(DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3));
        Timestamp c = Timestamp.fromEpochMillis(System.currentTimeMillis());
        assertThat(function.hash(c))
                .isEqualTo((FastHash.FastHashVisitor.getLongHash(c.getMillisecond())));
    }
}
