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

/** Test for {@link TableQueryAuthResult}. */
public class TableQueryAuthResultTest {

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

        // same names and ids: the rule binds to the same physical columns
        assertThatCode(() -> result.validateReadableWithoutRename(TABLE_TYPE, TABLE_TYPE))
                .doesNotThrowAnyException();

        // the mask input 'extra' was dropped and re-added, so the latest schema gives it a fresh
        // id. A time-travel read of the pre-drop snapshot still has an 'extra', but it is an
        // unrelated column; resolving the rule by name would mask with its values.
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
        // a row filter is remapped by name too, so it needs the same binding check as a mask
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
        // blank filter entries parse to no predicate
        assertThat(new TableQueryAuthResult(Collections.singletonList(""), null).hasRules())
                .isFalse();
        Map<String, String> masking = Collections.singletonMap("display", maskJson());
        assertThat(new TableQueryAuthResult(null, masking).hasRules()).isTrue();
    }

    @Test
    public void testWidenReadType() {
        Map<String, String> masking = Collections.singletonMap("display", maskJson());
        TableQueryAuthResult result = new TableQueryAuthResult(null, masking);
        // the mask input is unprojected: widen
        RowType widened = result.widenReadType(TABLE_TYPE, TABLE_TYPE.project("display"));
        assertThat(widened).isNotNull();
        assertThat(widened.getFieldNames()).containsExactly("display", "extra");
        // already covered: no widening
        assertThat(result.widenReadType(TABLE_TYPE, TABLE_TYPE)).isNull();
    }
}
