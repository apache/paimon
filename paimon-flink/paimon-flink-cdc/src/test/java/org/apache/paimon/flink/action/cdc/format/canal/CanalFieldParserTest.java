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

package org.apache.paimon.flink.action.cdc.format.canal;

import org.junit.jupiter.api.Test;

import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for {@link CanalFieldParser}. */
public class CanalFieldParserTest {

    @Test
    public void testEnumIndexZeroMapsToEmptyString() {
        // MySQL stores enum index 0 for an empty or invalid value, so it must map to the empty
        // string rather than indexing options[-1] and throwing ArrayIndexOutOfBoundsException.
        assertThat(CanalFieldParser.getEnumValueByIndex("enum('a','b','c')", 0)).isEmpty();
        // Valid 1-based indices still resolve to their members.
        assertThat(CanalFieldParser.getEnumValueByIndex("enum('a','b','c')", 1)).isEqualTo("a");
        assertThat(CanalFieldParser.getEnumValueByIndex("enum('a','b','c')", 3)).isEqualTo("c");
    }

    @Test
    public void testSetValuesBeyondIntRange() {
        String mysqlType =
                IntStream.range(0, 64)
                        .mapToObj(i -> "'m" + i + "'")
                        .collect(Collectors.joining(",", "set(", ")"));

        assertThat(CanalFieldParser.convertSet("5", "set('a','b','c')")).isEqualTo("[a,c]");
        // the 32nd member is bit 31, which no longer fits into an int
        assertThat(CanalFieldParser.convertSet("2147483648", mysqlType)).isEqualTo("[m31]");
        // the bitmap of a set with 64 members is an unsigned 64-bit value
        assertThat(CanalFieldParser.convertSet("9223372036854775809", mysqlType))
                .isEqualTo("[m0,m63]");
        assertThat(CanalFieldParser.convertSet("18446744073709551615", mysqlType))
                .isEqualTo(
                        IntStream.range(0, 64)
                                .mapToObj(i -> "m" + i)
                                .collect(Collectors.joining(",", "[", "]")));
    }
}
