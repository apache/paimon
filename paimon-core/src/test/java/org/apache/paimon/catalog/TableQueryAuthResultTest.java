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

package org.apache.paimon.catalog;

import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.JsonSerdeUtil;

import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests that malformed query-authorization definitions cannot be silently ignored. */
public class TableQueryAuthResultTest {

    @Test
    void testInvalidRowFilterFailsClosed() {
        assertThatThrownBy(
                        () ->
                                new TableQueryAuthResult(Collections.singletonList(""), null)
                                        .extractPredicate())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("cannot be empty");
        assertThatThrownBy(
                        () ->
                                new TableQueryAuthResult(Collections.singletonList("null"), null)
                                        .extractPredicate())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("JSON null");

        Predicate emptyCompound =
                JsonSerdeUtil.fromJson(
                        "{\"kind\":\"COMPOUND\",\"function\":\"AND\",\"children\":[]}",
                        Predicate.class);
        assertThatThrownBy(() -> TableQueryAuthResult.remapPredicate(emptyCompound, RowType.of()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must contain a predicate");

        Predicate missingFunction =
                JsonSerdeUtil.fromJson(
                        "{\"kind\":\"COMPOUND\",\"function\":null,\"children\":["
                                + "{\"kind\":\"LEAF\",\"transform\":{\"name\":\"NULL\"},"
                                + "\"function\":\"TRUE\",\"literals\":[]},"
                                + "{\"kind\":\"LEAF\",\"transform\":{\"name\":\"NULL\"},"
                                + "\"function\":\"TRUE\",\"literals\":[]}]}",
                        Predicate.class);
        assertThatThrownBy(() -> TableQueryAuthResult.remapPredicate(missingFunction, RowType.of()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("function cannot be null");
    }

    @Test
    void testInvalidColumnMaskFailsClosed() {
        assertThatThrownBy(
                        () ->
                                new TableQueryAuthResult(
                                                null, Collections.singletonMap("email", ""))
                                        .extractColumnMasking())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("cannot be empty");
        assertThatThrownBy(
                        () ->
                                new TableQueryAuthResult(
                                                null, Collections.singletonMap("email", "null"))
                                        .extractColumnMasking())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("JSON null");
    }
}
