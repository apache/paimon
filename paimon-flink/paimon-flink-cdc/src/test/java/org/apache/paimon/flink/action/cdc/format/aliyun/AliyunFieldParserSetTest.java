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

package org.apache.paimon.flink.action.cdc.format.aliyun;

import org.junit.jupiter.api.Test;

import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the SET decoding in {@link AliyunFieldParser}. */
public class AliyunFieldParserSetTest {

    // set('c0','c1',...,'c39') — 40 members, more than the 31 bits of a signed int.
    private static final String SET_40 =
            "set("
                    + IntStream.range(0, 40)
                            .mapToObj(i -> "'c" + i + "'")
                            .collect(Collectors.joining(","))
                    + ")";

    // set('c0',...,'c63') — the full 64 members MySQL allows; the 64th sets bit 63.
    private static final String SET_64 =
            "set("
                    + IntStream.range(0, 64)
                            .mapToObj(i -> "'c" + i + "'")
                            .collect(Collectors.joining(","))
                    + ")";

    @Test
    public void testSetValueBeyondIntRange() {
        // MySQL stores the chosen members as a bitmap; selecting the 33rd member sets bit 32,
        // whose decimal value (2^32) overflows a signed int.
        String value = Long.toString(1L << 32);
        assertThat(AliyunFieldParser.convertSet(value, SET_40)).isEqualTo("[c32]");
    }

    @Test
    public void testSetValueCombinesLowAndHighBits() {
        // bits 0 and 39 selected together
        long bitmap = 1L | (1L << 39);
        assertThat(AliyunFieldParser.convertSet(Long.toString(bitmap), SET_40))
                .isEqualTo("[c0,c39]");
    }

    @Test
    public void testSetValue64thMemberNeedsUnsignedParse() {
        // The 64th member sets bit 63; its decimal bitmap (2^63) exceeds Long.MAX_VALUE, so a
        // signed parse throws and the whole sync fails. It must be parsed unsigned.
        String value = Long.toUnsignedString(1L << 63);
        assertThat(AliyunFieldParser.convertSet(value, SET_64)).isEqualTo("[c63]");
    }

    @Test
    public void testSmallSetValueUnchanged() {
        // regression: values within the int range decode exactly as before
        assertThat(AliyunFieldParser.convertSet("3", "set('a','b','c')")).isEqualTo("[a,b]");
    }
}
