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

package org.apache.paimon.casting;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.BinaryType;
import org.apache.paimon.types.BooleanType;
import org.apache.paimon.types.CharType;
import org.apache.paimon.types.DateType;
import org.apache.paimon.types.DecimalType;
import org.apache.paimon.types.DoubleType;
import org.apache.paimon.types.FloatType;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.LocalZonedTimestampType;
import org.apache.paimon.types.SmallIntType;
import org.apache.paimon.types.TimeType;
import org.apache.paimon.types.TimestampType;
import org.apache.paimon.types.TinyIntType;
import org.apache.paimon.types.VarBinaryType;
import org.apache.paimon.types.VarCharType;
import org.apache.paimon.utils.DateTimeUtils;
import org.apache.paimon.utils.DecimalUtils;

import org.junit.jupiter.api.Test;

import java.util.TimeZone;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
                CastExecutors.resolve(new SmallIntType(false), new TinyIntType(false)),
                (short) 123,
                (byte) 123);
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
    public void testNumericToTimestamp() {
        compareCastResult(
                CastExecutors.resolve(new BigIntType(false), new LocalZonedTimestampType(3)),
                1721898748000L,
                DateTimeUtils.parseTimestampData(
                        "2024-07-25 17:12:28.000", 3, TimeZone.getTimeZone("Asia/Shanghai")));

        compareCastResult(
                CastExecutors.resolve(new BigIntType(false), new TimestampType(3)),
                1721898748000L,
                DateTimeUtils.parseTimestampData("2024-07-25 09:12:28.000", 3));
    }

    @Test
    public void testNumericToDecimal() {
        compareCastResult(
                CastExecutors.resolve(new TinyIntType(false), new DecimalType(10, 2)),
                (byte) 1,
                DecimalUtils.castFrom(1, 10, 2));
        compareCastResult(
                CastExecutors.resolve(new SmallIntType(false), new DecimalType(10, 2)),
                (short) 1,
                DecimalUtils.castFrom(1, 10, 2));
        compareCastResult(
                CastExecutors.resolve(new IntType(false), new DecimalType(10, 2)),
                1,
                DecimalUtils.castFrom(1, 10, 2));
        compareCastResult(
                CastExecutors.resolve(new BigIntType(false), new DecimalType(10, 2)),
                1L,
                DecimalUtils.castFrom(1, 10, 2));
        compareCastResult(
                CastExecutors.resolve(new FloatType(false), new DecimalType(10, 2)),
                1.23456F,
                DecimalUtils.castFrom(1.23456D, 10, 2));
        compareCastResult(
                CastExecutors.resolve(new DoubleType(false), new DecimalType(10, 2)),
                1.23456D,
                DecimalUtils.castFrom(1.23456D, 10, 2));
    }

    @Test
    public void testDecimalToDecimal() {
        compareCastResult(
                CastExecutors.resolve(new DecimalType(10, 4), new DecimalType(10, 2)),
                DecimalUtils.castFrom(1.23456D, 10, 4),
                DecimalUtils.castFrom(1.23456D, 10, 2));
        compareCastResult(
                CastExecutors.resolve(new DecimalType(10, 2), new DecimalType(10, 4)),
                DecimalUtils.castFrom(1.23456D, 10, 2),
                DecimalUtils.castFrom(1.2300D, 10, 4));
    }

    @Test
    public void testDecimalToNumeric() {
        compareCastResult(
                CastExecutors.resolve(new DecimalType(10, 4), new FloatType(false)),
                DecimalUtils.castFrom(1.23456D, 10, 4),
                1.2346F);
        compareCastResult(
                CastExecutors.resolve(new DecimalType(10, 2), new DoubleType(false)),
                DecimalUtils.castFrom(1.23456D, 10, 2),
                1.23D);
    }

    @Test
    public void testBooleanToNumeric() {
        compareCastResult(
                CastExecutors.resolve(new BooleanType(false), new TinyIntType(false)),
                true,
                (byte) 1);
        compareCastResult(
                CastExecutors.resolve(new BooleanType(false), new SmallIntType(false)),
                true,
                (short) 1);
        compareCastResult(
                CastExecutors.resolve(new BooleanType(false), new IntType(false)), true, 1);
        compareCastResult(
                CastExecutors.resolve(new BooleanType(false), new BigIntType(false)), true, 1L);
        compareCastResult(
                CastExecutors.resolve(new BooleanType(false), new FloatType(false)), true, 1F);
        compareCastResult(
                CastExecutors.resolve(new BooleanType(false), new DoubleType(false)), true, 1D);
        compareCastResult(
                CastExecutors.resolve(new BooleanType(false), new DecimalType(1, 0)),
                true,
                DecimalUtils.castFrom(1, 1, 0));
    }

    @Test
    public void testNumericToBoolean() {
        compareCastResult(
                CastExecutors.resolve(new TinyIntType(false), new BooleanType(false)),
                (byte) 1,
                true);
        compareCastResult(
                CastExecutors.resolve(new SmallIntType(false), new BooleanType(false)),
                (short) 1,
                true);
        compareCastResult(
                CastExecutors.resolve(new IntType(false), new BooleanType(false)), 0, false);
        compareCastResult(
                CastExecutors.resolve(new BigIntType(false), new BooleanType(false)), 12L, true);
    }

    // To string rules

    @Test
    public void testNumericToString() {
        // byte to string
        compareCastResult(
                CastExecutors.resolve(new TinyIntType(false), new VarCharType(5)),
                (byte) 1,
                BinaryString.fromString("1"));

        // short to string
        compareCastResult(
                CastExecutors.resolve(new SmallIntType(false), new VarCharType(5)),
                (short) 1,
                BinaryString.fromString("1"));

        // int to string
        compareCastResult(
                CastExecutors.resolve(new IntType(false), new VarCharType(5)),
                1,
                BinaryString.fromString("1"));

        // bigint to string
        compareCastResult(
                CastExecutors.resolve(new BigIntType(false), new VarCharType(5)),
                1L,
                BinaryString.fromString("1"));

        // float to string
        compareCastResult(
                CastExecutors.resolve(new FloatType(false), new VarCharType(10)),
                1.23456F,
                BinaryString.fromString("1.23456"));

        // double to string
        compareCastResult(
                CastExecutors.resolve(new DoubleType(false), new VarCharType(10)),
                1.23456D,
                BinaryString.fromString("1.23456"));

        // decimal to string
        compareCastResult(
                CastExecutors.resolve(new DecimalType(10, 5), new VarCharType(20)),
                DecimalUtils.castFrom(1.23456D, 10, 5),
                BinaryString.fromString("1.23456"));
    }

    @Test
    public void testBooleanToString() {
        compareCastResult(
                CastExecutors.resolve(new BooleanType(false), new CharType(5)),
                true,
                BinaryString.fromString("true "));
        compareCastResult(
                CastExecutors.resolve(new BooleanType(false), new VarCharType(5)),
                true,
                BinaryString.fromString("true"));
    }

    @Test
    public void testTimestampToString() {
        long mills = System.currentTimeMillis();
        Timestamp timestamp = Timestamp.fromEpochMillis(mills);
        compareCastResult(
                CastExecutors.resolve(new TimestampType(5), VarCharType.STRING_TYPE),
                timestamp,
                BinaryString.fromString(
                        DateTimeUtils.formatTimestamp(timestamp, DateTimeUtils.UTC_ZONE, 5)));

        compareCastResult(
                CastExecutors.resolve(new LocalZonedTimestampType(5), VarCharType.STRING_TYPE),
                timestamp,
                BinaryString.fromString(
                        DateTimeUtils.formatTimestamp(timestamp, TimeZone.getDefault(), 5)));
    }

    @Test
    public void testTimeToString() {
        compareCastResult(
                CastExecutors.resolve(new TimeType(2), VarCharType.STRING_TYPE),
                36115615,
                BinaryString.fromString("10:01:55.61"));
    }

    @Test
    public void testDateToString() {
        compareCastResult(
                CastExecutors.resolve(new DateType(), VarCharType.STRING_TYPE),
                19516,
                BinaryString.fromString("2023-06-08"));
    }

    @Test
    public void testStringToString() {
        // varchar(10) to varchar(5)
        compareCastResult(
                CastExecutors.resolve(new VarCharType(10), new VarCharType(5)),
                BinaryString.fromString("1234567890"),
                BinaryString.fromString("12345"));

        // varchar(10) to varchar(20)
        compareCastResult(
                CastExecutors.resolve(new VarCharType(10), new VarCharType(20)),
                BinaryString.fromString("1234567890"),
                BinaryString.fromString("1234567890"));

        // varchar(10) to char(5)
        compareCastResult(
                CastExecutors.resolve(new VarCharType(10), new CharType(5)),
                BinaryString.fromString("1234567890"),
                BinaryString.fromString("12345"));

        // varchar(10) to char(20)
        compareCastResult(
                CastExecutors.resolve(new VarCharType(10), new CharType(20)),
                BinaryString.fromString("1234567890"),
                BinaryString.fromString("1234567890          "));

        // char(10) to varchar(5)
        compareCastResult(
                CastExecutors.resolve(new CharType(10), new VarCharType(5)),
                BinaryString.fromString("1234567890"),
                BinaryString.fromString("12345"));

        // char(10) to varchar(20)
        compareCastResult(
                CastExecutors.resolve(new CharType(10), new VarCharType(20)),
                BinaryString.fromString("12345678  "),
                BinaryString.fromString("12345678  "));

        // char(10) to char(5)
        compareCastResult(
                CastExecutors.resolve(new CharType(10), new CharType(5)),
                BinaryString.fromString("12345678  "),
                BinaryString.fromString("12345"));

        // char(10) to char(20)
        compareCastResult(
                CastExecutors.resolve(new CharType(10), new CharType(20)),
                BinaryString.fromString("12345678  "),
                BinaryString.fromString("12345678            "));
    }

    // From string rules

    @Test
    public void testStringToBoolean() {
        compareCastResult(
                CastExecutors.resolve(new VarCharType(5), new BooleanType(false)),
                BinaryString.fromString("t"),
                true);
        compareCastResult(
                CastExecutors.resolve(new VarCharType(5), new BooleanType(false)),
                BinaryString.fromString("true"),
                true);
        compareCastResult(
                CastExecutors.resolve(new VarCharType(5), new BooleanType(false)),
                BinaryString.fromString("y"),
                true);
        compareCastResult(
                CastExecutors.resolve(new VarCharType(5), new BooleanType(false)),
                BinaryString.fromString("yes"),
                true);
        compareCastResult(
                CastExecutors.resolve(new VarCharType(5), new BooleanType(false)),
                BinaryString.fromString("1"),
                true);
        compareCastResult(
                CastExecutors.resolve(new VarCharType(5), new BooleanType(false)),
                BinaryString.fromString("TRUE"),
                true);

        compareCastResult(
                CastExecutors.resolve(new VarCharType(5), new BooleanType(false)),
                BinaryString.fromString("f"),
                false);
        compareCastResult(
                CastExecutors.resolve(new VarCharType(5), new BooleanType(false)),
                BinaryString.fromString("false"),
                false);
        compareCastResult(
                CastExecutors.resolve(new VarCharType(5), new BooleanType(false)),
                BinaryString.fromString("n"),
                false);
        compareCastResult(
                CastExecutors.resolve(new VarCharType(5), new BooleanType(false)),
                BinaryString.fromString("no"),
                false);
        compareCastResult(
                CastExecutors.resolve(new VarCharType(5), new BooleanType(false)),
                BinaryString.fromString("0"),
                false);

        assertThatThrownBy(
                        () ->
                                compareCastResult(
                                        CastExecutors.resolve(
                                                new VarCharType(5), new BooleanType(false)),
                                        BinaryString.fromString("11"),
                                        false))
                .hasMessage("Cannot parse '11' as BOOLEAN.");
    }

    @Test
    public void testStringToDecimal() {
        compareCastResult(
                CastExecutors.resolve(new VarCharType(5), new DecimalType(5, 2)),
                BinaryString.fromString("1.233"),
                DecimalUtils.castFrom(1.233D, 5, 2));
    }

    @Test
    public void testStringToNumeric() {
        // string to byte
        compareCastResult(
                CastExecutors.resolve(new VarCharType(5), new TinyIntType(false)),
                BinaryString.fromString("1"),
                (byte) 1);

        // string to short
        compareCastResult(
                CastExecutors.resolve(new VarCharType(5), new SmallIntType(false)),
                BinaryString.fromString("1"),
                (short) 1);

        // string to int
        compareCastResult(
                CastExecutors.resolve(new VarCharType(5), new IntType(false)),
                BinaryString.fromString("1"),
                1);

        // string to bigint
        compareCastResult(
                CastExecutors.resolve(new VarCharType(5), new BigIntType(false)),
                BinaryString.fromString("1"),
                1L);

        // string to float
        compareCastResult(
                CastExecutors.resolve(new VarCharType(10), new FloatType(false)),
                BinaryString.fromString("1.23456"),
                1.23456F);

        // string to double
        compareCastResult(
                CastExecutors.resolve(new VarCharType(10), new DoubleType(false)),
                BinaryString.fromString("1.23456"),
                1.23456D);
    }

    @Test
    public void testStringToDate() {
        String date = "2023-06-06";
        compareCastResult(
                CastExecutors.resolve(new VarCharType(25), new DateType()),
                BinaryString.fromString(date),
                DateTimeUtils.parseDate(date));
    }

    @Test
    public void testStringToTime() {
        String date = "09:30:00.0";
        compareCastResult(
                CastExecutors.resolve(new VarCharType(25), new TimeType(2)),
                BinaryString.fromString(date),
                DateTimeUtils.parseTime(date));
    }

    @Test
    public void testStringToTimestamp() {
        String date = "2017-12-12 09:30:00.0";
        compareCastResult(
                CastExecutors.resolve(new VarCharType(25), new TimestampType(3)),
                BinaryString.fromString(date),
                DateTimeUtils.parseTimestampData(date, 3));

        compareCastResult(
                CastExecutors.resolve(new VarCharType(25), new LocalZonedTimestampType(3)),
                BinaryString.fromString(date),
                DateTimeUtils.parseTimestampData(date, 3, TimeZone.getDefault()));
    }

    @Test
    public void testStringToBinary() {
        // string(10) to binary(5)
        compareCastResult(
                CastExecutors.resolve(new VarCharType(10), new VarBinaryType(5)),
                BinaryString.fromString("12345678"),
                "12345".getBytes());

        // string(10) to binary(20)
        compareCastResult(
                CastExecutors.resolve(new VarCharType(10), new VarBinaryType(20)),
                BinaryString.fromString("12345678"),
                "12345678".getBytes());
    }

    // To binary rules

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

    // Date/Time/Timestamp rules

    @Test
    public void testTimestampData() {
        long mills = System.currentTimeMillis();
        Timestamp timestamp = Timestamp.fromEpochMillis(mills);

        // timestamp(5) to timestamp(2)
        compareCastResult(
                CastExecutors.resolve(new TimestampType(5), new TimestampType(2)),
                timestamp,
                DateTimeUtils.truncate(Timestamp.fromEpochMillis(mills), 2));

        // timestamp to date
        compareCastResult(
                CastExecutors.resolve(new TimestampType(5), new DateType()),
                Timestamp.fromEpochMillis(mills),
                (int) (mills / DateTimeUtils.MILLIS_PER_DAY));

        // timestamp to time
        compareCastResult(
                CastExecutors.resolve(new TimestampType(5), new TimeType(2)),
                Timestamp.fromEpochMillis(mills),
                (int) (mills % DateTimeUtils.MILLIS_PER_DAY));

        // timestamp(3) to timestamp_ltz(3)
        compareCastResult(
                CastExecutors.resolve(new TimestampType(3), new LocalZonedTimestampType(3)),
                timestamp,
                DateTimeUtils.timestampToTimestampWithLocalZone(
                        Timestamp.fromEpochMillis(mills), TimeZone.getDefault()));

        // timestamp_ltz(5) to timestamp(2)
        compareCastResult(
                CastExecutors.resolve(new LocalZonedTimestampType(5), new TimestampType(2)),
                timestamp,
                DateTimeUtils.truncate(
                        DateTimeUtils.timestampWithLocalZoneToTimestamp(
                                Timestamp.fromEpochMillis(mills), TimeZone.getDefault()),
                        2));

        // timestamp_ltz to date
        compareCastResult(
                CastExecutors.resolve(new LocalZonedTimestampType(5), new DateType()),
                Timestamp.fromEpochMillis(mills),
                DateTimeUtils.timestampWithLocalZoneToDate(timestamp, TimeZone.getDefault()));

        // timestamp_ltz to time
        compareCastResult(
                CastExecutors.resolve(new LocalZonedTimestampType(5), new TimeType(2)),
                Timestamp.fromEpochMillis(mills),
                DateTimeUtils.timestampWithLocalZoneToTime(timestamp, TimeZone.getDefault()));
    }

    @Test
    public void testDateToTimestamp() {
        String date = "2023-06-06";
        compareCastResult(
                CastExecutors.resolve(new DateType(), new TimestampType(5)),
                DateTimeUtils.parseDate(date),
                DateTimeUtils.parseTimestampData(date, 3));

        compareCastResult(
                CastExecutors.resolve(new DateType(), new LocalZonedTimestampType(5)),
                DateTimeUtils.parseDate(date),
                DateTimeUtils.parseTimestampData(date, 3, TimeZone.getDefault()));
    }

    @Test
    public void testTimeToTimestamp() {
        String time = "12:00:00.123";
        compareCastResult(
                CastExecutors.resolve(new TimeType(), new TimestampType(3)),
                DateTimeUtils.parseTime(time),
                DateTimeUtils.parseTimestampData("1970-01-01 " + time, 3));
    }

    @SuppressWarnings("rawtypes")
    private void compareCastResult(CastExecutor<?, ?> cast, Object input, Object output) {
        assertThat(((CastExecutor) cast).cast(input)).isEqualTo(output);
    }
}
