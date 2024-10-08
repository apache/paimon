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

package org.apache.paimon.utils;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.memory.MemorySegmentUtils;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeChecks;

import java.time.DateTimeException;
import java.util.Arrays;
import java.util.List;
import java.util.TimeZone;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.paimon.data.BinaryString.fromString;
import static org.apache.paimon.types.DataTypeRoot.BINARY;
import static org.apache.paimon.types.DataTypeRoot.CHAR;

/** Util for {@link BinaryString}. */
public class BinaryStringUtils {

    public static final BinaryString NULL_STRING = fromString("NULL");
    public static final BinaryString TRUE_STRING = fromString("TRUE");
    public static final BinaryString FALSE_STRING = fromString("FALSE");

    public static final BinaryString[] EMPTY_STRING_ARRAY = new BinaryString[0];
    private static final List<BinaryString> TRUE_STRINGS =
            Stream.of("t", "true", "y", "yes", "1")
                    .map(BinaryString::fromString)
                    .collect(Collectors.toList());

    private static final List<BinaryString> FALSE_STRINGS =
            Stream.of("f", "false", "n", "no", "0")
                    .map(BinaryString::fromString)
                    .collect(Collectors.toList());

    private static byte[] getTmpBytes(BinaryString str, int sizeInBytes) {
        byte[] bytes = MemorySegmentUtils.allocateReuseBytes(sizeInBytes);
        MemorySegmentUtils.copyToBytes(str.getSegments(), str.getOffset(), bytes, 0, sizeInBytes);
        return bytes;
    }

    /** Parse a {@link BinaryString} to boolean. */
    public static boolean toBoolean(BinaryString str) {
        BinaryString lowerCase = str.toLowerCase();
        if (TRUE_STRINGS.contains(lowerCase)) {
            return true;
        }
        if (FALSE_STRINGS.contains(lowerCase)) {
            return false;
        }
        throw new RuntimeException("Cannot parse '" + str + "' as BOOLEAN.");
    }

    /**
     * Parses this BinaryString to Long.
     *
     * <p>Note that, in this method we accumulate the result in negative format, and convert it to
     * positive format at the end, if this string is not started with '-'. This is because min value
     * is bigger than max value in digits, e.g. Long.MAX_VALUE is '9223372036854775807' and
     * Long.MIN_VALUE is '-9223372036854775808'.
     *
     * <p>This code is mostly copied from LazyLong.parseLong in Hive.
     */
    public static long toLong(BinaryString str) throws NumberFormatException {
        int sizeInBytes = str.getSizeInBytes();
        byte[] tmpBytes = getTmpBytes(str, sizeInBytes);
        if (sizeInBytes == 0) {
            throw numberFormatExceptionFor(str, "Input is empty.");
        }
        int i = 0;

        byte b = tmpBytes[i];
        final boolean negative = b == '-';
        if (negative || b == '+') {
            i++;
            if (sizeInBytes == 1) {
                throw numberFormatExceptionFor(str, "Input has only positive or negative symbol.");
            }
        }

        long result = 0;
        final byte separator = '.';
        final int radix = 10;
        final long stopValue = Long.MIN_VALUE / radix;
        while (i < sizeInBytes) {
            b = tmpBytes[i];
            i++;
            if (b == separator) {
                // We allow decimals and will return a truncated integral in that case.
                // Therefore we won't throw an exception here (checking the fractional
                // part happens below.)
                break;
            }

            int digit;
            if (b >= '0' && b <= '9') {
                digit = b - '0';
            } else {
                throw numberFormatExceptionFor(str, "Invalid character found.");
            }

            // We are going to process the new digit and accumulate the result. However, before
            // doing this, if the result is already smaller than the
            // stopValue(Long.MIN_VALUE / radix), then result * 10 will definitely be smaller
            // than minValue, and we can stop.
            if (result < stopValue) {
                throw numberFormatExceptionFor(str, "Overflow.");
            }

            result = result * radix - digit;
            // Since the previous result is less than or equal to
            // stopValue(Long.MIN_VALUE / radix), we can just use `result > 0` to check overflow.
            // If result overflows, we should stop.
            if (result > 0) {
                throw numberFormatExceptionFor(str, "Overflow.");
            }
        }

        // This is the case when we've encountered a decimal separator. The fractional
        // part will not change the number, but we will verify that the fractional part
        // is well formed.
        while (i < sizeInBytes) {
            byte currentByte = tmpBytes[i];
            if (currentByte < '0' || currentByte > '9') {
                throw numberFormatExceptionFor(str, "Invalid character found.");
            }
            i++;
        }

        if (!negative) {
            result = -result;
            if (result < 0) {
                throw numberFormatExceptionFor(str, "Overflow.");
            }
        }
        return result;
    }

    /**
     * Parses this BinaryString to Int.
     *
     * <p>Note that, in this method we accumulate the result in negative format, and convert it to
     * positive format at the end, if this string is not started with '-'. This is because min value
     * is bigger than max value in digits, e.g. Integer.MAX_VALUE is '2147483647' and
     * Integer.MIN_VALUE is '-2147483648'.
     *
     * <p>This code is mostly copied from LazyInt.parseInt in Hive.
     *
     * <p>Note that, this method is almost same as `toLong`, but we leave it duplicated for
     * performance reasons, like Hive does.
     */
    public static int toInt(BinaryString str) throws NumberFormatException {
        int sizeInBytes = str.getSizeInBytes();
        byte[] tmpBytes = getTmpBytes(str, sizeInBytes);
        if (sizeInBytes == 0) {
            throw numberFormatExceptionFor(str, "Input is empty.");
        }
        int i = 0;

        byte b = tmpBytes[i];
        final boolean negative = b == '-';
        if (negative || b == '+') {
            i++;
            if (sizeInBytes == 1) {
                throw numberFormatExceptionFor(str, "Input has only positive or negative symbol.");
            }
        }

        int result = 0;
        final byte separator = '.';
        final int radix = 10;
        final long stopValue = Integer.MIN_VALUE / radix;
        while (i < sizeInBytes) {
            b = tmpBytes[i];
            i++;
            if (b == separator) {
                // We allow decimals and will return a truncated integral in that case.
                // Therefore we won't throw an exception here (checking the fractional
                // part happens below.)
                break;
            }

            int digit;
            if (b >= '0' && b <= '9') {
                digit = b - '0';
            } else {
                throw numberFormatExceptionFor(str, "Invalid character found.");
            }

            // We are going to process the new digit and accumulate the result. However, before
            // doing this, if the result is already smaller than the
            // stopValue(Long.MIN_VALUE / radix), then result * 10 will definitely be smaller
            // than minValue, and we can stop.
            if (result < stopValue) {
                throw numberFormatExceptionFor(str, "Overflow.");
            }

            result = result * radix - digit;
            // Since the previous result is less than or equal to
            // stopValue(Long.MIN_VALUE / radix), we can just use `result > 0` to check overflow.
            // If result overflows, we should stop.
            if (result > 0) {
                throw numberFormatExceptionFor(str, "Overflow.");
            }
        }

        // This is the case when we've encountered a decimal separator. The fractional
        // part will not change the number, but we will verify that the fractional part
        // is well formed.
        while (i < sizeInBytes) {
            byte currentByte = tmpBytes[i];
            if (currentByte < '0' || currentByte > '9') {
                throw numberFormatExceptionFor(str, "Invalid character found.");
            }
            i++;
        }

        if (!negative) {
            result = -result;
            if (result < 0) {
                throw numberFormatExceptionFor(str, "Overflow.");
            }
        }
        return result;
    }

    public static short toShort(BinaryString str) throws NumberFormatException {
        int intValue = toInt(str);
        short result = (short) intValue;
        if (result == intValue) {
            return result;
        }
        throw numberFormatExceptionFor(str, "Overflow.");
    }

    public static byte toByte(BinaryString str) throws NumberFormatException {
        int intValue = toInt(str);
        byte result = (byte) intValue;
        if (result == intValue) {
            return result;
        }
        throw numberFormatExceptionFor(str, "Overflow.");
    }

    public static double toDouble(BinaryString str) throws NumberFormatException {
        return Double.parseDouble(str.toString());
    }

    public static float toFloat(BinaryString str) throws NumberFormatException {
        return Float.parseFloat(str.toString());
    }

    private static NumberFormatException numberFormatExceptionFor(
            BinaryString input, String reason) {
        return new NumberFormatException("For input string: '" + input + "'. " + reason);
    }

    public static int toDate(BinaryString input) throws DateTimeException {
        String str = input.toString();
        if (StringUtils.isNumeric(str)) {
            // for Integer.toString conversion
            return toInt(input);
        }
        Integer date = DateTimeUtils.parseDate(str);
        if (date == null) {
            throw new DateTimeException("For input string: '" + input + "'.");
        }

        return date;
    }

    public static int toTime(BinaryString input) throws DateTimeException {
        String str = input.toString();
        if (StringUtils.isNumeric(str)) {
            // for Integer.toString conversion
            return toInt(input);
        }
        Integer date = DateTimeUtils.parseTime(str);
        if (date == null) {
            throw new DateTimeException("For input string: '" + input + "'.");
        }

        return date;
    }

    /** Used by {@code CAST(x as TIMESTAMP)}. */
    public static Timestamp toTimestamp(BinaryString input, int precision)
            throws DateTimeException {
        if (StringUtils.isNumeric(input.toString())) {
            long epoch = toLong(input);
            return fromMillisToTimestamp(epoch, precision);
        }
        return DateTimeUtils.parseTimestampData(input.toString(), precision);
    }

    /** Used by {@code CAST(x as TIMESTAMP_LTZ)}. */
    public static Timestamp toTimestamp(BinaryString input, int precision, TimeZone timeZone)
            throws DateTimeException {
        return DateTimeUtils.parseTimestampData(input.toString(), precision, timeZone);
    }

    // Helper method to convert milliseconds to Timestamp with the provided precision.
    private static Timestamp fromMillisToTimestamp(long epoch, int precision) {
        // Calculate milliseconds and nanoseconds from epoch based on precision
        long millis;
        int nanosOfMillis;

        switch (precision) {
            case 0: // seconds
                millis = epoch * 1000;
                nanosOfMillis = 0;
                break;
            case 3: // milliseconds
                millis = epoch;
                nanosOfMillis = 0;
                break;
            case 6: // microseconds
                millis = epoch / 1000;
                nanosOfMillis = (int)((epoch % 1000) * 1000);
                break;
            case 9: // nanoseconds
                millis = epoch / 1_000_000;
                nanosOfMillis = (int)(epoch % 1_000_000);
                break;
            default:
                throw new RuntimeException("Unsupported precision: " + precision);
        }
        return Timestamp.fromEpochMillis(millis, nanosOfMillis);
    }

    public static BinaryString toCharacterString(BinaryString strData, DataType type) {
        final boolean targetCharType = type.getTypeRoot() == CHAR;
        final int targetLength = DataTypeChecks.getLength(type);
        if (strData.numChars() > targetLength) {
            return strData.substring(0, targetLength);
        } else if (strData.numChars() < targetLength && targetCharType) {
            int padLength = targetLength - strData.numChars();
            BinaryString padString = BinaryString.blankString(padLength);
            return StringUtils.concat(strData, padString);
        }
        return strData;
    }

    public static byte[] toBinaryString(byte[] byteArrayTerm, DataType type) {
        final boolean targetBinaryType = type.getTypeRoot() == BINARY;
        final int targetLength = DataTypeChecks.getLength(type);
        if (byteArrayTerm.length == targetLength) {
            return byteArrayTerm;
        }
        if (targetBinaryType) {
            return Arrays.copyOf(byteArrayTerm, targetLength);
        } else {
            if (byteArrayTerm.length <= targetLength) {
                return byteArrayTerm;
            } else {
                return Arrays.copyOf(byteArrayTerm, targetLength);
            }
        }
    }
}
