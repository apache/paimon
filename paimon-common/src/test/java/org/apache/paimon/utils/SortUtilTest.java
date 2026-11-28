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

import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link SortUtil}. */
public class SortUtilTest {

    @Test
    public void testCompareBinaryByFirstDifferentUnsignedByte() {
        // lengths cover both the 8-byte word comparison and the byte-by-byte tail
        for (int length = 1; length <= 17; length++) {
            for (int pos = 0; pos < length; pos++) {
                byte[] smaller = new byte[length];
                byte[] larger = new byte[length];
                // 0x80 is negative as a signed byte, but greater than 0x7F unsigned
                smaller[pos] = 0x7F;
                larger[pos] = (byte) 0x80;
                // bytes after the first difference must not decide the order
                Arrays.fill(smaller, pos + 1, length, (byte) 0xFF);

                assertThat(SortUtil.compareBinary(smaller, larger))
                        .as("length %s, position %s", length, pos)
                        .isNegative();
                assertThat(SortUtil.compareBinary(larger, smaller))
                        .as("length %s, position %s", length, pos)
                        .isPositive();
            }
        }
    }

    @Test
    public void testCompareBinaryOrdersPrefixFirst() {
        byte[] bytes = new byte[17];
        Arrays.fill(bytes, (byte) 0xFF);
        for (int length = 0; length < bytes.length; length++) {
            byte[] prefix = Arrays.copyOf(bytes, length);

            assertThat(SortUtil.compareBinary(prefix, bytes))
                    .as("prefix length %s", length)
                    .isNegative();
            assertThat(SortUtil.compareBinary(bytes, prefix))
                    .as("prefix length %s", length)
                    .isPositive();
        }

        assertThat(SortUtil.compareBinary(bytes, bytes.clone())).isZero();
    }
}
