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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link ParameterUtils}. */
class ParameterUtilsTest {

    @Test
    void testParseIntegerRanges() {
        assertThat(ParameterUtils.parseIntegerRanges("0-2, 4, 2, 6 - 7", 8))
                .isEqualTo(Arrays.asList(0, 1, 2, 4, 6, 7));
    }

    @Test
    void testInvalidIntegerRanges() {
        assertThatThrownBy(() -> ParameterUtils.parseIntegerRanges("", 8))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must not be empty");
        assertThatThrownBy(() -> ParameterUtils.parseIntegerRanges("0,,2", 8))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("empty item");
        assertThatThrownBy(() -> ParameterUtils.parseIntegerRanges("3-1", 8))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must not be greater");
        assertThatThrownBy(() -> ParameterUtils.parseIntegerRanges("-1", 8))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid integer or range");
        assertThatThrownBy(() -> ParameterUtils.parseIntegerRanges("0-8", 8))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("out of range");
    }
}
