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

package org.apache.flink.table.store.utils;

import org.apache.flink.table.store.data.BinaryString;
import org.apache.flink.table.store.memory.MemorySegmentUtils;

import java.util.Arrays;
import java.util.Random;

import static org.apache.flink.table.store.data.BinaryString.fromBytes;

/**
 * Utils for {@link BinaryString} and utility class to convert objects into strings in vice-versa.
 */
public class StringUtils {

    /**
     * An empty string array. There are just too many places where one needs an empty string array
     * and wants to save some object allocation.
     */
    public static final String[] EMPTY_STRING_ARRAY = new String[0];

    private static final char[] HEX_CHARS = {
        '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'
    };

    /**
     * Concatenates input strings together into a single string. Returns NULL if any argument is
     * NULL.
     */
    public static BinaryString concat(BinaryString... inputs) {
        return concat(Arrays.asList(inputs));
    }

    public static BinaryString concat(Iterable<BinaryString> inputs) {
        // Compute the total length of the result.
        int totalLength = 0;
        for (BinaryString input : inputs) {
            if (input == null) {
                return null;
            }

            totalLength += input.getSizeInBytes();
        }

        // Allocate a new byte array, and copy the inputs one by one into it.
        final byte[] result = new byte[totalLength];
        int offset = 0;
        for (BinaryString input : inputs) {
            if (input != null) {
                int len = input.getSizeInBytes();
                MemorySegmentUtils.copyToBytes(
                        input.getSegments(), input.getOffset(), result, offset, len);
                offset += len;
            }
        }
        return fromBytes(result);
    }

    /**
     * Checks if the string is null, empty, or contains only whitespace characters. A whitespace
     * character is defined via {@link Character#isWhitespace(char)}.
     *
     * @param str The string to check
     * @return True, if the string is null or blank, false otherwise.
     */
    public static boolean isNullOrWhitespaceOnly(String str) {
        if (str == null || str.length() == 0) {
            return true;
        }

        final int len = str.length();
        for (int i = 0; i < len; i++) {
            if (!Character.isWhitespace(str.charAt(i))) {
                return false;
            }
        }
        return true;
    }

    /**
     * Given an array of bytes it will convert the bytes to a hex string representation of the
     * bytes.
     *
     * @param bytes the bytes to convert in a hex string
     * @param start start index, inclusively
     * @param end end index, exclusively
     * @return hex string representation of the byte array
     */
    public static String byteToHexString(final byte[] bytes, final int start, final int end) {
        if (bytes == null) {
            throw new IllegalArgumentException("bytes == null");
        }

        int length = end - start;
        char[] out = new char[length * 2];

        for (int i = start, j = 0; i < end; i++) {
            out[j++] = HEX_CHARS[(0xF0 & bytes[i]) >>> 4];
            out[j++] = HEX_CHARS[0x0F & bytes[i]];
        }

        return new String(out);
    }

    /**
     * Given an array of bytes it will convert the bytes to a hex string representation of the
     * bytes.
     *
     * @param bytes the bytes to convert in a hex string
     * @return hex string representation of the byte array
     */
    public static String byteToHexString(final byte[] bytes) {
        return byteToHexString(bytes, 0, bytes.length);
    }

    /**
     * Creates a random string with a length within the given interval. The string contains only
     * characters that can be represented as a single code point.
     *
     * @param rnd The random used to create the strings.
     * @param minLength The minimum string length.
     * @param maxLength The maximum string length (inclusive).
     * @return A random String.
     */
    public static String getRandomString(Random rnd, int minLength, int maxLength) {
        int len = rnd.nextInt(maxLength - minLength + 1) + minLength;

        char[] data = new char[len];
        for (int i = 0; i < data.length; i++) {
            data[i] = (char) (rnd.nextInt(0x7fff) + 1);
        }
        return new String(data);
    }

    /**
     * Creates a random string with a length within the given interval. The string contains only
     * characters that can be represented as a single code point.
     *
     * @param rnd The random used to create the strings.
     * @param minLength The minimum string length.
     * @param maxLength The maximum string length (inclusive).
     * @param minValue The minimum character value to occur.
     * @param maxValue The maximum character value to occur.
     * @return A random String.
     */
    public static String getRandomString(
            Random rnd, int minLength, int maxLength, char minValue, char maxValue) {
        int len = rnd.nextInt(maxLength - minLength + 1) + minLength;

        char[] data = new char[len];
        int diff = maxValue - minValue + 1;

        for (int i = 0; i < data.length; i++) {
            data[i] = (char) (rnd.nextInt(diff) + minValue);
        }
        return new String(data);
    }
}
