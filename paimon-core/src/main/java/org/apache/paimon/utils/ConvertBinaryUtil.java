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

import java.nio.charset.StandardCharsets;

/** BinaryUtil util to convert the value to binary. */
public class ConvertBinaryUtil {

    private ConvertBinaryUtil() {}

    public static byte[] paddingTo8Byte(byte[] data) {
        return paddingToNByte(data, 8);
    }

    public static byte[] paddingToNByte(byte[] data, int paddingNum) {
        if (data.length == paddingNum) {
            return data;
        }
        if (data.length > paddingNum) {
            byte[] result = new byte[paddingNum];
            System.arraycopy(data, 0, result, 0, paddingNum);
            return result;
        }
        int paddingSize = paddingNum - data.length;
        byte[] result = new byte[paddingNum];
        for (int i = 0; i < paddingSize; i++) {
            result[i] = 0;
        }
        System.arraycopy(data, 0, result, paddingSize, data.length);

        return result;
    }

    public static byte[] utf8To8Byte(String data) {
        return paddingTo8Byte(data.getBytes(StandardCharsets.UTF_8));
    }

    public static Long convertStringToLong(String data) {
        byte[] bytes = utf8To8Byte(data);
        return convertBytesToLong(bytes);
    }

    public static long convertBytesToLong(byte[] bytes) {
        byte[] paddedBytes = paddingTo8Byte(bytes);
        long temp = 0L;
        for (int i = 7; i >= 0; i--) {
            temp = temp | (((long) paddedBytes[i] & 0xff) << (7 - i) * 8);
        }
        return temp;
    }
}
