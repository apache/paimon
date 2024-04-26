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

/** Collection of simple mathematical routines. */
public class MathUtils {

    /**
     * Decrements the given number down to the closest power of two. If the argument is a power of
     * two, it remains unchanged.
     *
     * @param value The value to round down.
     * @return The closest value that is a power of two and less or equal than the given value.
     */
    public static int roundDownToPowerOf2(int value) {
        return Integer.highestOneBit(value);
    }

    /**
     * Checks whether the given value is a power of two.
     *
     * @param value The value to check.
     * @return True, if the value is a power of two, false otherwise.
     */
    public static boolean isPowerOf2(long value) {
        return (value & (value - 1)) == 0;
    }

    /**
     * Computes the logarithm of the given value to the base of 2. This method throws an error, if
     * the given argument is not a power of 2.
     *
     * @param value The value to compute the logarithm for.
     * @return The logarithm to the base of 2.
     * @throws ArithmeticException Thrown, if the given value is zero.
     * @throws IllegalArgumentException Thrown, if the given value is not a power of two.
     */
    public static int log2strict(int value) throws ArithmeticException, IllegalArgumentException {
        if (value == 0) {
            throw new ArithmeticException("Logarithm of zero is undefined.");
        }
        if ((value & (value - 1)) != 0) {
            throw new IllegalArgumentException(
                    "The given value " + value + " is not a power of two.");
        }
        return 31 - Integer.numberOfLeadingZeros(value);
    }

    public static Integer max(Integer v1, Integer v2) {
        if (v1 == null && v2 == null) {
            return null;
        }

        if (v1 != null && v2 == null) {
            return v1;
        }

        if (v1 == null) {
            return v2;
        }

        return Math.max(v1, v2);
    }

    public static Integer min(Integer v1, Integer v2) {
        if (v1 == null && v2 == null) {
            return null;
        }

        if (v1 != null && v2 == null) {
            return v1;
        }

        if (v1 == null) {
            return v2;
        }

        return Math.min(v1, v2);
    }

    /** Safely increments the given int value by one, ensuring that no overflow occurs. */
    public static int incrementSafely(int a) {
        if (a == Integer.MAX_VALUE) {
            return a;
        }
        return a + 1;
    }

    /** Safely add the given int value by another int value, ensuring that no overflow occurs. */
    public static int addSafely(int a, int b) {
        try {
            return Math.addExact(a, b);
        } catch (ArithmeticException e) {
            return Integer.MAX_VALUE;
        }
    }
}
