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
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.predicate.Equal;
import org.apache.paimon.predicate.FieldRef;
import org.apache.paimon.predicate.FieldTransform;
import org.apache.paimon.predicate.LeafPredicate;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.Transform;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.IteratorRecordReader;
import org.apache.paimon.utils.JsonSerdeUtil;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link TableQueryAuthResult}. */
class TableQueryAuthResultTest {

    private static final RowType NESTED_TYPE =
            RowType.of(
                    new DataType[] {DataTypes.INT(), DataTypes.STRING()}, new String[] {"b", "c"});

    /** Output row type with top-level fields reordered relative to the table schema (id, s). */
    private static final RowType OUTPUT_TYPE =
            RowType.of(new DataType[] {NESTED_TYPE, DataTypes.INT()}, new String[] {"s", "id"});

    private static Transform nestedFieldTransform() {
        return new FieldTransform(
                new FieldRef(1, "s.b", DataTypes.INT(), new int[] {0}, new int[] {2}));
    }

    @Test
    void testNestedRowFilterRemappedByTopLevelField() throws Exception {
        Predicate predicate =
                LeafPredicate.of(
                        nestedFieldTransform(), Equal.INSTANCE, Collections.singletonList(10));
        TableQueryAuthResult authResult =
                new TableQueryAuthResult(
                        Collections.singletonList(JsonSerdeUtil.toJson(predicate)), null);

        List<InternalRow> rows =
                Arrays.asList(
                        GenericRow.of(GenericRow.of(10, BinaryString.fromString("x")), 1),
                        GenericRow.of(GenericRow.of(20, BinaryString.fromString("y")), 2));

        List<Integer> ids = new ArrayList<>();
        authResult
                .doAuth(new IteratorRecordReader<>(rows.iterator()), OUTPUT_TYPE)
                .forEachRemaining(row -> ids.add(row.getInt(1)));

        assertThat(ids).containsExactly(1);
    }

    @Test
    void testNestedColumnMaskingRemappedByTopLevelField() throws Exception {
        Map<String, String> masking =
                Collections.singletonMap("id", JsonSerdeUtil.toJson(nestedFieldTransform()));
        TableQueryAuthResult authResult = new TableQueryAuthResult(null, masking);

        List<InternalRow> rows =
                Collections.singletonList(
                        GenericRow.of(GenericRow.of(42, BinaryString.fromString("x")), 1));

        List<Integer> ids = new ArrayList<>();
        authResult
                .doAuth(new IteratorRecordReader<>(rows.iterator()), OUTPUT_TYPE)
                .forEachRemaining(row -> ids.add(row.getInt(1)));

        assertThat(ids).containsExactly(42);
    }
}
