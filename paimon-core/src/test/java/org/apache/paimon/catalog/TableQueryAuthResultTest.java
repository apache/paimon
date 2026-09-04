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

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.predicate.ConcatWsTransform;
import org.apache.paimon.predicate.Equal;
import org.apache.paimon.predicate.FieldRef;
import org.apache.paimon.predicate.FieldTransform;
import org.apache.paimon.predicate.LeafPredicate;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.JsonSerdeUtil;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
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

    private static final RowType TABLE_TYPE =
            RowType.of(
                    new org.apache.paimon.types.DataField(0, "display", DataTypes.STRING()),
                    new org.apache.paimon.types.DataField(1, "extra", DataTypes.STRING()));

    private static String filterJson() {
        return JsonSerdeUtil.toFlatJson(
                LeafPredicate.of(
                        new FieldTransform(new FieldRef(1, "extra", DataTypes.STRING())),
                        Equal.INSTANCE,
                        Collections.singletonList(BinaryString.fromString("x"))));
    }

    private static String maskJson() {
        return JsonSerdeUtil.toFlatJson(
                new ConcatWsTransform(
                        Arrays.asList(
                                BinaryString.fromString("-"),
                                new FieldRef(1, "extra", DataTypes.STRING()))));
    }

    @Test
    public void testValidateRejectsReAddedColumnOfSameName() {
        Map<String, String> masking = Collections.singletonMap("display", maskJson());
        TableQueryAuthResult result = new TableQueryAuthResult(null, masking);

        assertThatCode(() -> result.validateReadableWithoutRename(TABLE_TYPE, TABLE_TYPE))
                .doesNotThrowAnyException();

        RowType latest =
                RowType.of(
                        new org.apache.paimon.types.DataField(0, "display", DataTypes.STRING()),
                        new org.apache.paimon.types.DataField(7, "extra", DataTypes.STRING()));
        assertThatThrownBy(() -> result.validateReadableWithoutRename(latest, TABLE_TYPE))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("dropped and re-added");
    }

    @Test
    public void testValidateRejectsReAddedColumnForRowFilter() {
        TableQueryAuthResult result =
                new TableQueryAuthResult(Collections.singletonList(filterJson()), null);
        assertThatCode(() -> result.validateReadableWithoutRename(TABLE_TYPE, TABLE_TYPE))
                .doesNotThrowAnyException();

        RowType latest =
                RowType.of(
                        new org.apache.paimon.types.DataField(0, "display", DataTypes.STRING()),
                        new org.apache.paimon.types.DataField(7, "extra", DataTypes.STRING()));
        assertThatThrownBy(() -> result.validateReadableWithoutRename(latest, TABLE_TYPE))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Row filter")
                .hasMessageContaining("dropped and re-added");
    }

    @Test
    public void testHasRules() {
        assertThat(new TableQueryAuthResult(null, null).hasRules()).isFalse();
        assertThat(
                        new TableQueryAuthResult(Collections.emptyList(), Collections.emptyMap())
                                .hasRules())
                .isFalse();
        // a blank entry is now rejected rather than ignored, see testInvalidRowFilterFailsClosed
        Map<String, String> masking = Collections.singletonMap("display", maskJson());
        assertThat(new TableQueryAuthResult(null, masking).hasRules()).isTrue();
    }
}
