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

package org.apache.paimon.predicate;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.JsonSerdeUtil;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link NestedFieldTransform}. */
class NestedFieldTransformTest {

    // user STRUCT<id BIGINT, addr STRUCT<city STRING, zip STRING>>
    private static final RowType ADDR_TYPE =
            RowType.of(
                    new org.apache.paimon.types.DataType[] {DataTypes.STRING(), DataTypes.STRING()},
                    new String[] {"city", "zip"});
    private static final RowType USER_TYPE =
            RowType.of(
                    new org.apache.paimon.types.DataType[] {DataTypes.BIGINT(), ADDR_TYPE},
                    new String[] {"id", "addr"});
    private static final RowType ROW_TYPE =
            RowType.of(
                    new org.apache.paimon.types.DataType[] {DataTypes.INT(), USER_TYPE},
                    new String[] {"pk", "user"});

    private static final FieldRef USER_REF = new FieldRef(1, "user", USER_TYPE);

    private static GenericRow row(Object user) {
        return GenericRow.of(1, user);
    }

    @Test
    public void testReadOneLevel() {
        NestedFieldTransform transform =
                new NestedFieldTransform(USER_REF, Collections.singletonList("id"));

        assertThat(transform.fieldName()).isEqualTo("user.id");
        assertThat(transform.outputType()).isEqualTo(DataTypes.BIGINT());
        assertThat(transform.transform(row(GenericRow.of(42L, null)))).isEqualTo(42L);
    }

    @Test
    public void testReadTwoLevels() {
        NestedFieldTransform transform =
                new NestedFieldTransform(USER_REF, Arrays.asList("addr", "city"));

        assertThat(transform.fieldName()).isEqualTo("user.addr.city");
        assertThat(transform.outputType()).isEqualTo(DataTypes.STRING());

        GenericRow addr =
                GenericRow.of(
                        BinaryString.fromString("Beijing"), BinaryString.fromString("100080"));
        assertThat(transform.transform(row(GenericRow.of(42L, addr))))
                .isEqualTo(BinaryString.fromString("Beijing"));
    }

    /** The descent loop is recursive; three levels must read as well as two. */
    @Test
    public void testReadThreeLevels() {
        RowType level3 =
                RowType.of(
                        new org.apache.paimon.types.DataType[] {DataTypes.BIGINT()},
                        new String[] {"d"});
        RowType level2 =
                RowType.of(new org.apache.paimon.types.DataType[] {level3}, new String[] {"c"});
        RowType level1 =
                RowType.of(new org.apache.paimon.types.DataType[] {level2}, new String[] {"b"});
        FieldRef ref = new FieldRef(0, "a", level1);

        NestedFieldTransform transform =
                new NestedFieldTransform(ref, Arrays.asList("b", "c", "d"));
        assertThat(transform.fieldName()).isEqualTo("a.b.c.d");
        assertThat(transform.outputType()).isEqualTo(DataTypes.BIGINT());

        GenericRow row = GenericRow.of(GenericRow.of(GenericRow.of(GenericRow.of(42L))));
        assertThat(transform.transform(row)).isEqualTo(42L);

        // a null two levels down still yields null
        GenericRow withNull = GenericRow.of(GenericRow.of(GenericRow.of((Object) null)));
        assertThat(transform.transform(withNull)).isNull();
    }

    @Test
    public void testNullAnywhereOnThePathYieldsNull() {
        NestedFieldTransform transform =
                new NestedFieldTransform(USER_REF, Arrays.asList("addr", "city"));

        // the top-level column is null
        assertThat(transform.transform(row(null))).isNull();
        // an intermediate struct is null
        assertThat(transform.transform(row(GenericRow.of(42L, null)))).isNull();
        // the leaf itself is null
        assertThat(transform.transform(row(GenericRow.of(42L, GenericRow.of(null, null)))))
                .isNull();
    }

    @Test
    public void testPredicateOnNullEvaluatesFalse() {
        PredicateBuilder builder = new PredicateBuilder(ROW_TYPE);
        Predicate predicate =
                builder.equal(
                        new NestedFieldTransform(USER_REF, Arrays.asList("addr", "city")),
                        BinaryString.fromString("Beijing"));

        assertThat(predicate.test(row(null))).isFalse();
        assertThat(predicate.test(row(GenericRow.of(42L, null)))).isFalse();
    }

    /**
     * The whole safety story rests on this: nothing that equates a leaf with a top-level column can
     * mistake a nested field for one, because it never gets a {@link FieldRef} back.
     */
    @Test
    public void testNoFieldRefIsExposed() {
        LeafPredicate predicate =
                (LeafPredicate)
                        new PredicateBuilder(ROW_TYPE)
                                .equal(
                                        new NestedFieldTransform(
                                                USER_REF, Collections.singletonList("id")),
                                        42L);

        assertThat(predicate.fieldRefOptional()).isEmpty();
        // the enclosing column is what schema-level rewrites see
        assertThat(predicate.fieldNames()).containsExactly("user");
    }

    /** Min/max of the enclosing column say nothing about the nested field, so nothing is pruned. */
    @Test
    public void testStatsNeverPrune() {
        Predicate predicate =
                new PredicateBuilder(ROW_TYPE)
                        .equal(
                                new NestedFieldTransform(USER_REF, Collections.singletonList("id")),
                                42L);

        assertThat(
                        predicate.test(
                                100L,
                                GenericRow.of(1, null),
                                GenericRow.of(10, null),
                                new GenericArray(new Object[] {0L, 0L})))
                .isTrue();
    }

    @Test
    public void testProjectionKeepsThePath() {
        Predicate predicate =
                new PredicateBuilder(ROW_TYPE)
                        .equal(
                                new NestedFieldTransform(USER_REF, Arrays.asList("addr", "city")),
                                42L);

        // "user" moves from index 1 to index 0
        Optional<Predicate> projected =
                predicate.visit(PredicateProjectionConverter.fromProjection(new int[] {1}));

        assertThat(projected).isPresent();
        NestedFieldTransform transform =
                (NestedFieldTransform) ((LeafPredicate) projected.get()).transform();
        assertThat(transform.fieldRef().index()).isEqualTo(0);
        assertThat(transform.path()).containsExactly("addr", "city");
        assertThat(transform.fieldName()).isEqualTo("user.addr.city");
    }

    @Test
    public void testJsonRoundTrip() {
        Predicate predicate =
                new PredicateBuilder(ROW_TYPE)
                        .equal(
                                new NestedFieldTransform(USER_REF, Arrays.asList("addr", "city")),
                                BinaryString.fromString("Beijing"));

        String json = JsonSerdeUtil.toJson(predicate);
        assertThat(JsonSerdeUtil.fromJson(json, Predicate.class)).isEqualTo(predicate);
    }

    @Test
    public void testRejectsPathThroughNonRowType() {
        FieldRef arrayRef = new FieldRef(0, "tags", DataTypes.ARRAY(DataTypes.STRING()));
        assertThatThrownBy(() -> new NestedFieldTransform(arrayRef, Collections.singletonList("x")))
                .isInstanceOf(IllegalArgumentException.class);

        assertThatThrownBy(() -> new NestedFieldTransform(USER_REF, Collections.emptyList()))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(
                        () -> new NestedFieldTransform(USER_REF, Collections.singletonList("nope")))
                .isInstanceOf(IllegalArgumentException.class);
    }

    /**
     * Remapping must not let a nested reference drift onto a different field. Column pruning can
     * hand {@code copyWithNewInputs} a structurally different row type — a bare position stays in
     * range and silently addresses whatever now sits there. Row filters and column masks are
     * remapped this way, so drifting has to fail closed rather than resolve elsewhere.
     */
    @Test
    public void testRemapOntoAPrunedRowTypeDoesNotDrift() {
        RowType full =
                RowType.of(
                        new org.apache.paimon.types.DataType[] {
                            DataTypes.STRING(), DataTypes.STRING()
                        },
                        new String[] {"secret", "region"});
        FieldRef infoRef = new FieldRef(0, "info", full);
        NestedFieldTransform onSecret =
                new NestedFieldTransform(infoRef, Collections.singletonList("secret"));
        assertThat(onSecret.fieldName()).isEqualTo("info.secret");

        // "secret" was pruned away; position 0 is now "region"
        RowType pruned =
                RowType.of(
                        new org.apache.paimon.types.DataType[] {DataTypes.STRING()},
                        new String[] {"region"});
        FieldRef prunedRef = new FieldRef(0, "info", pruned);

        assertThatThrownBy(() -> onSecret.copyWithNewInputs(Collections.singletonList(prunedRef)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    /** Remapping onto a reordered row type must keep addressing the same field. */
    @Test
    public void testRemapFollowsTheFieldWhenPositionsShift() {
        RowType full =
                RowType.of(
                        new org.apache.paimon.types.DataType[] {
                            DataTypes.STRING(), DataTypes.STRING()
                        },
                        new String[] {"secret", "region"});
        NestedFieldTransform onSecret =
                new NestedFieldTransform(
                        new FieldRef(0, "info", full), Collections.singletonList("secret"));

        RowType reordered =
                RowType.of(
                        new org.apache.paimon.types.DataType[] {
                            DataTypes.STRING(), DataTypes.STRING()
                        },
                        new String[] {"region", "secret"});
        Transform remapped =
                onSecret.copyWithNewInputs(
                        Collections.singletonList(new FieldRef(0, "info", reordered)));

        assertThat(((NestedFieldTransform) remapped).fieldName()).isEqualTo("info.secret");
    }
}
