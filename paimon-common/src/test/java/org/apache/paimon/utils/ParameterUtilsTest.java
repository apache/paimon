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

import org.apache.paimon.types.DataField;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link ParameterUtils}. */
class ParameterUtilsTest {

    @Test
    void testParseDataFieldArrayWithoutIds() {
        // create_function passes a user-written parameter list, which may omit the ids; each
        // field still has to get its own instead of every one landing on 0
        List<DataField> fields =
                ParameterUtils.parseDataFieldArray(
                        "[{\"name\":\"a\",\"type\":\"INT\"},"
                                + "{\"name\":\"b\",\"type\":\"STRING\"},"
                                + "{\"name\":\"c\",\"type\":\"BIGINT\"}]");

        assertThat(fields).extracting(DataField::id).containsExactly(0, 1, 2);
        assertThat(fields).extracting(DataField::name).containsExactly("a", "b", "c");
    }

    @Test
    void testParseDataFieldArrayKeepsExplicitIds() {
        List<DataField> fields =
                ParameterUtils.parseDataFieldArray(
                        "[{\"id\":3,\"name\":\"a\",\"type\":\"INT\"},"
                                + "{\"id\":9,\"name\":\"b\",\"type\":\"STRING\"}]");

        assertThat(fields).extracting(DataField::id).containsExactly(3, 9);
    }

    @Test
    void testParseDataFieldArrayRejectsPartialIds() {
        // both orders must be rejected: supplying a counter to a list that already carries an id
        // would let the id-less fields silently draw a colliding one
        assertThatThrownBy(
                        () ->
                                ParameterUtils.parseDataFieldArray(
                                        "[{\"name\":\"a\",\"type\":\"INT\"},"
                                                + "{\"id\":7,\"name\":\"b\",\"type\":\"STRING\"}]"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Field id is required");

        assertThatThrownBy(
                        () ->
                                ParameterUtils.parseDataFieldArray(
                                        "[{\"id\":0,\"name\":\"a\",\"type\":\"INT\"},"
                                                + "{\"name\":\"b\",\"type\":\"STRING\"}]"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Field id is required");
    }

    @Test
    void testParseDataFieldArrayRejectsIdLessNestedField() {
        // a nested row inside an explicitly numbered list would otherwise draw id 0 and collide
        // with the first top-level field
        assertThatThrownBy(
                        () ->
                                ParameterUtils.parseDataFieldArray(
                                        "[{\"id\":0,\"name\":\"a\",\"type\":\"INT\"},"
                                                + "{\"id\":1,\"name\":\"b\",\"type\":"
                                                + "{\"type\":\"ROW\",\"fields\":"
                                                + "[{\"name\":\"x\",\"type\":\"INT\"}]}}]"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Field id is required");
    }

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
