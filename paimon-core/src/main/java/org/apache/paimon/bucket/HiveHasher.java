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

package org.apache.paimon.bucket;

import org.apache.paimon.memory.MemorySegment;

import java.math.BigDecimal;
import java.math.RoundingMode;

/** Hive hash util. */
public class HiveHasher {

    private static final int HIVE_DECIMAL_MAX_PRECISION = 38;
    private static final int HIVE_DECIMAL_MAX_SCALE = 38;

    @Override
    public String toString() {
        return HiveHasher.class.getSimpleName();
    }

    public static int hashInt(int input) {
        return input;
    }

    public static int hashLong(long input) {
        return Long.hashCode(input);
    }

    public static int hashBytes(byte[] bytes) {
        int result = 0;
        for (byte value : bytes) {
            result = (result * 31) + value;
        }
        return result;
    }

    public static int hashUnsafeBytes(MemorySegment[] segments, int offset, int length) {
        int result = 0;
        for (MemorySegment segment : segments) {
            int remaining = segment.size() - offset;
            if (remaining > 0) {
                int bytesToRead = Math.min(remaining, length);
                for (int i = 0; i < bytesToRead; i++) {
                    result = (result * 31) + segment.get(offset + i);
                }
                length -= bytesToRead;
                offset = 0;
            } else {
                offset -= segment.size();
            }

            if (length == 0) {
                break;
            }
        }
        return result;
    }

    public static BigDecimal normalizeDecimal(BigDecimal input) {
        if (input == null) {
            return null;
        }

        BigDecimal result = trimDecimal(input);
        int intDigits = result.precision() - result.scale();
        if (intDigits > HIVE_DECIMAL_MAX_PRECISION) {
            return null;
        }

        int maxScale =
                Math.min(
                        HIVE_DECIMAL_MAX_SCALE,
                        Math.min(HIVE_DECIMAL_MAX_PRECISION - intDigits, result.scale()));
        if (result.scale() > maxScale) {
            result = result.setScale(maxScale, RoundingMode.HALF_UP);
            result = trimDecimal(result);
        }

        return result;
    }

    private static BigDecimal trimDecimal(BigDecimal input) {
        if (input.compareTo(BigDecimal.ZERO) == 0) {
            return BigDecimal.ZERO;
        }

        BigDecimal result = input.stripTrailingZeros();
        if (result.compareTo(BigDecimal.ZERO) == 0) {
            return BigDecimal.ZERO;
        }

        if (result.scale() < 0) {
            result = result.setScale(0);
        }

        return result;
    }
}
