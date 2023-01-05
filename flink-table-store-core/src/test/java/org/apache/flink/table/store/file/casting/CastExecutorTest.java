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

package org.apache.flink.table.store.file.casting;

import org.apache.flink.table.data.DecimalDataUtils;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BinaryType;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.TimeType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.utils.DateTimeUtils;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link CastExecutor}. */
public class CastExecutorTest {

    @Test
    public void testNumericToNumeric() {
        // byte to other numeric
        compareCastResult(
                CastExecutors.resolve(new TinyIntType(false), new SmallIntType(false)),
                (byte) 1,
                (short) 1);
        compareCastResult(
                CastExecutors.resolve(new TinyIntType(false), new IntType(false)), (byte) 1, 1);
        compareCastResult(
                CastExecutors.resolve(new TinyIntType(false), new BigIntType(false)), (byte) 1, 1L);
        compareCastResult(
                CastExecutors.resolve(new TinyIntType(false), new FloatType(false)), (byte) 1, 1F);
        compareCastResult(
                CastExecutors.resolve(new TinyIntType(false), new DoubleType(false)), (byte) 1, 1D);

        // short to other numeric
        compareCastResult(
                CastExecutors.resolve(new SmallIntType(false), new IntType(false)), (short) 1, 1);
        compareCastResult(
                CastExecutors.resolve(new SmallIntType(false), new BigIntType(false)),
                (short) 1,
                1L);
        compareCastResult(
                CastExecutors.resolve(new SmallIntType(false), new FloatType(false)),
                (short) 1,
                1F);
        compareCastResult(
                CastExecutors.resolve(new SmallIntType(false), new DoubleType(false)),
                (short) 1,
                1D);

        // int to other numeric
        compareCastResult(CastExecutors.resolve(new IntType(false), new BigIntType(false)), 1, 1L);
        compareCastResult(CastExecutors.resolve(new IntType(false), new FloatType(false)), 1, 1F);
        compareCastResult(CastExecutors.resolve(new IntType(false), new DoubleType(false)), 1, 1D);

        // bigint to other numeric
        compareCastResult(
                CastExecutors.resolve(new BigIntType(false), new FloatType(false)), 1L, 1F);
        compareCastResult(
                CastExecutors.resolve(new BigIntType(false), new DoubleType(false)), 1L, 1D);

        // float to double
        compareCastResult(
                CastExecutors.resolve(new FloatType(false), new DoubleType(false)), 1F, 1D);
    }

    @Test
    public void testNumericToDecimal() {
        compareCastResult(
                CastExecutors.resolve(new TinyIntType(false), new DecimalType(10, 2)),
                (byte) 1,
                DecimalDataUtils.castFrom(1, 10, 2));
        compareCastResult(
                CastExecutors.resolve(new SmallIntType(false), new DecimalType(10, 2)),
                (short) 1,
                DecimalDataUtils.castFrom(1, 10, 2));
        compareCastResult(
                CastExecutors.resolve(new IntType(false), new DecimalType(10, 2)),
                1,
                DecimalDataUtils.castFrom(1, 10, 2));
        compareCastResult(
                CastExecutors.resolve(new BigIntType(false), new DecimalType(10, 2)),
                1L,
                DecimalDataUtils.castFrom(1, 10, 2));
        compareCastResult(
                CastExecutors.resolve(new FloatType(false), new DecimalType(10, 2)),
                1.23456F,
                DecimalDataUtils.castFrom(1.23456D, 10, 2));
        compareCastResult(
                CastExecutors.resolve(new DoubleType(false), new DecimalType(10, 2)),
                1.23456D,
                DecimalDataUtils.castFrom(1.23456D, 10, 2));
    }

    @Test
    public void testDecimalToDecimal() {
        compareCastResult(
                CastExecutors.resolve(new DecimalType(10, 4), new DecimalType(10, 2)),
                DecimalDataUtils.castFrom(1.23456D, 10, 4),
                DecimalDataUtils.castFrom(1.23456D, 10, 2));
        compareCastResult(
                CastExecutors.resolve(new DecimalType(10, 2), new DecimalType(10, 4)),
                DecimalDataUtils.castFrom(1.23456D, 10, 2),
                DecimalDataUtils.castFrom(1.2300D, 10, 4));
    }

    @Test
    public void testDecimalToNumeric() {
        compareCastResult(
                CastExecutors.resolve(new DecimalType(10, 4), new FloatType(false)),
                DecimalDataUtils.castFrom(1.23456D, 10, 4),
                1.2346F);
        compareCastResult(
                CastExecutors.resolve(new DecimalType(10, 2), new DoubleType(false)),
                DecimalDataUtils.castFrom(1.23456D, 10, 2),
                1.23D);
    }

    @Test
    public void testStringToString() {
        // varchar(10) to varchar(5)
        compareCastResult(
                CastExecutors.resolve(new VarCharType(10), new VarCharType(5)),
                StringData.fromString("1234567890"),
                StringData.fromString("12345"));

        // varchar(10) to varchar(20)
        compareCastResult(
                CastExecutors.resolve(new VarCharType(10), new VarCharType(20)),
                StringData.fromString("1234567890"),
                StringData.fromString("1234567890"));

        // varchar(10) to char(5)
        compareCastResult(
                CastExecutors.resolve(new VarCharType(10), new CharType(5)),
                StringData.fromString("1234567890"),
                StringData.fromString("12345"));

        // varchar(10) to char(20)
        compareCastResult(
                CastExecutors.resolve(new VarCharType(10), new CharType(20)),
                StringData.fromString("1234567890"),
                StringData.fromString("1234567890          "));

        // char(10) to varchar(5)
        compareCastResult(
                CastExecutors.resolve(new CharType(10), new VarCharType(5)),
                StringData.fromString("1234567890"),
                StringData.fromString("12345"));

        // char(10) to varchar(20)
        compareCastResult(
                CastExecutors.resolve(new CharType(10), new VarCharType(20)),
                StringData.fromString("12345678  "),
                StringData.fromString("12345678  "));

        // char(10) to char(5)
        compareCastResult(
                CastExecutors.resolve(new CharType(10), new CharType(5)),
                StringData.fromString("12345678  "),
                StringData.fromString("12345"));

        // char(10) to char(20)
        compareCastResult(
                CastExecutors.resolve(new CharType(10), new CharType(20)),
                StringData.fromString("12345678  "),
                StringData.fromString("12345678            "));
    }

    @Test
    public void testStringToBinary() {
        // string(10) to binary(5)
        compareCastResult(
                CastExecutors.resolve(new VarCharType(10), new VarBinaryType(5)),
                StringData.fromString("12345678"),
                "12345".getBytes());

        // string(10) to binary(20)
        compareCastResult(
                CastExecutors.resolve(new VarCharType(10), new VarBinaryType(20)),
                StringData.fromString("12345678"),
                "12345678".getBytes());
    }

    @Test
    public void testBinaryToBinary() {
        // binary(10) to binary(5)
        compareCastResult(
                CastExecutors.resolve(new BinaryType(10), new BinaryType(5)),
                "1234567890".getBytes(),
                "12345".getBytes());

        // binary(10) to binary(20)
        compareCastResult(
                CastExecutors.resolve(new BinaryType(10), new BinaryType(20)),
                "12345678".getBytes(),
                new byte[] {49, 50, 51, 52, 53, 54, 55, 56, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0});

        // binary(10) to varbinary(5)
        compareCastResult(
                CastExecutors.resolve(new BinaryType(10), new VarBinaryType(5)),
                "1234567890".getBytes(),
                "12345".getBytes());

        // binary(10) to varbinary(20)
        compareCastResult(
                CastExecutors.resolve(new BinaryType(10), new VarBinaryType(20)),
                "12345678".getBytes(),
                "12345678".getBytes());
    }

    @Test
    public void testTimestampData() {
        long mills = System.currentTimeMillis();
        TimestampData timestampData = TimestampData.fromEpochMillis(mills);

        // timestamp(5) to timestamp(2)
        compareCastResult(
                CastExecutors.resolve(new TimestampType(5), new TimestampType(2)),
                timestampData,
                DateTimeUtils.truncate(TimestampData.fromEpochMillis(mills), 2));

        // timestamp to date
        compareCastResult(
                CastExecutors.resolve(new TimestampType(5), new DateType()),
                TimestampData.fromEpochMillis(mills),
                (int) (mills / DateTimeUtils.MILLIS_PER_DAY));

        // timestamp to time
        compareCastResult(
                CastExecutors.resolve(new TimestampType(5), new TimeType(2)),
                TimestampData.fromEpochMillis(mills),
                (int) (mills % DateTimeUtils.MILLIS_PER_DAY));
    }

    @SuppressWarnings("rawtypes")
    private void compareCastResult(CastExecutor<?, ?> cast, Object input, Object output) {
        assertThat(((CastExecutor) cast).cast(input)).isEqualTo(output);
    }
}
