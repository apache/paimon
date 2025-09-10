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

package org.apache.paimon.mergetree.compact.aggregate;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.GenericMap;
import org.apache.paimon.mergetree.compact.aggregate.factory.FieldBoolAndAggFactory;
import org.apache.paimon.mergetree.compact.aggregate.factory.FieldBoolOrAggFactory;
import org.apache.paimon.mergetree.compact.aggregate.factory.FieldCollectAggFactory;
import org.apache.paimon.mergetree.compact.aggregate.factory.FieldFirstNonNullValueAggFactory;
import org.apache.paimon.mergetree.compact.aggregate.factory.FieldFirstValueAggFactory;
import org.apache.paimon.mergetree.compact.aggregate.factory.FieldHllSketchAggFactory;
import org.apache.paimon.mergetree.compact.aggregate.factory.FieldLastNonNullValueAggFactory;
import org.apache.paimon.mergetree.compact.aggregate.factory.FieldLastValueAggFactory;
import org.apache.paimon.mergetree.compact.aggregate.factory.FieldListaggAggFactory;
import org.apache.paimon.mergetree.compact.aggregate.factory.FieldMaxAggFactory;
import org.apache.paimon.mergetree.compact.aggregate.factory.FieldMergeMapAggFactory;
import org.apache.paimon.mergetree.compact.aggregate.factory.FieldMinAggFactory;
import org.apache.paimon.mergetree.compact.aggregate.factory.FieldNestedUpdateAggFactory;
import org.apache.paimon.mergetree.compact.aggregate.factory.FieldPrimaryKeyAggFactory;
import org.apache.paimon.mergetree.compact.aggregate.factory.FieldProductAggFactory;
import org.apache.paimon.mergetree.compact.aggregate.factory.FieldRoaringBitmap32AggFactory;
import org.apache.paimon.mergetree.compact.aggregate.factory.FieldRoaringBitmap64AggFactory;
import org.apache.paimon.mergetree.compact.aggregate.factory.FieldSumAggFactory;
import org.apache.paimon.mergetree.compact.aggregate.factory.FieldThetaSketchAggFactory;
import org.apache.paimon.types.DataTypes;

import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Test that for a NOT NULL field (that is, both accumulator and input are non-null), whether {@link
 * FieldAggregator} will produce null value when retracting.
 */
public class FieldAggregatorRetractNullTest {

    @Test
    public void testFieldFirstValueAgg() {
        FieldFirstValueAgg fieldFirstValueAgg =
                new FieldFirstValueAggFactory().create(DataTypes.INT(), null, null);
        assertThatThrownBy(() -> fieldFirstValueAgg.retract(1, 1))
                .hasMessageContaining(
                        "Aggregate function 'first_value' does not support retraction");
    }

    @Test
    public void testFieldFirstNonNullValueAgg() {
        FieldFirstNonNullValueAgg fieldFirstNonNullValueAgg =
                new FieldFirstNonNullValueAggFactory().create(DataTypes.INT(), null, null);
        assertThatThrownBy(() -> fieldFirstNonNullValueAgg.retract(1, 1))
                .hasMessageContaining(
                        "Aggregate function 'first_non_null_value' does not support retraction");
    }

    @Test
    public void testFieldSumAgg() {
        FieldSumAgg fieldSumAgg = new FieldSumAggFactory().create(DataTypes.INT(), null, null);
        assertThat(fieldSumAgg.retract(1, 1)).isNotNull();
    }

    @Test
    public void testFieldNestedUpdateAgg() {
        FieldNestedUpdateAgg fieldNestedUpdateAgg =
                new FieldNestedUpdateAggFactory()
                        .create(
                                DataTypes.ARRAY(DataTypes.ROW(DataTypes.INT())),
                                CoreOptions.fromMap(Collections.emptyMap()),
                                "f0");
        assertThat(
                        fieldNestedUpdateAgg.retract(
                                new GenericArray(new Object[0]), new GenericArray(new Object[0])))
                .isNotNull();
    }

    @Test
    public void testFieldMinAgg() {
        FieldMinAgg fieldMinAgg = new FieldMinAggFactory().create(DataTypes.INT(), null, null);
        assertThatThrownBy(() -> fieldMinAgg.retract(1, 1))
                .hasMessageContaining("Aggregate function 'min' does not support retraction");
    }

    @Test
    void testFieldMaxAgg() {
        FieldMaxAgg fieldMaxAgg = new FieldMaxAggFactory().create(DataTypes.INT(), null, null);
        assertThatThrownBy(() -> fieldMaxAgg.retract(1, 1))
                .hasMessageContaining("Aggregate function 'max' does not support retraction");
    }

    @Test
    public void testFieldRoaringBitmap32Agg() {
        FieldRoaringBitmap32Agg roaringBitmap32Agg =
                new FieldRoaringBitmap32AggFactory().create(DataTypes.VARBINARY(20), null, null);
        assertThatThrownBy(() -> roaringBitmap32Agg.retract(1, 1))
                .hasMessageContaining("Aggregate function 'rbm32' does not support retraction");
    }

    @Test
    public void testFieldRoaringBitmap64Agg() {
        FieldRoaringBitmap64Agg roaringBitmap64Agg =
                new FieldRoaringBitmap64AggFactory().create(DataTypes.VARBINARY(20), null, null);
        assertThatThrownBy(() -> roaringBitmap64Agg.retract(1, 1))
                .hasMessageContaining("Aggregate function 'rbm64' does not support retraction");
    }

    @Test
    public void testFieldThetaSketchAgg() {
        FieldThetaSketchAgg thetaSketchAgg =
                new FieldThetaSketchAggFactory().create(DataTypes.VARBINARY(20), null, null);
        assertThatThrownBy(() -> thetaSketchAgg.retract(1, 1))
                .hasMessageContaining(
                        "Aggregate function 'theta_sketch' does not support retraction");
    }

    @Test
    public void testFieldHllSketchAgg() {
        FieldHllSketchAgg hllSketchAgg =
                new FieldHllSketchAggFactory().create(DataTypes.VARBINARY(20), null, null);
        assertThatThrownBy(() -> hllSketchAgg.retract(1, 1))
                .hasMessageContaining(
                        "Aggregate function 'hll_sketch' does not support retraction");
    }

    @Test
    public void testFieldListaggAgg() {
        FieldListaggAgg fieldListaggAgg =
                new FieldListaggAggFactory()
                        .create(
                                DataTypes.STRING(),
                                CoreOptions.fromMap(Collections.emptyMap()),
                                null);
        assertThatThrownBy(() -> fieldListaggAgg.retract(1, 1))
                .hasMessageContaining("Aggregate function 'listagg' does not support retraction");
    }

    @Test
    public void testFieldBoolAndAgg() {
        FieldBoolAndAgg boolAndAgg =
                new FieldBoolAndAggFactory().create(DataTypes.BOOLEAN(), null, null);
        assertThatThrownBy(() -> boolAndAgg.retract(true, true))
                .hasMessageContaining("Aggregate function 'bool_and' does not support retraction");
    }

    @Test
    public void testFieldBoolOrAgg() {
        FieldBoolOrAgg boolOrAgg =
                new FieldBoolOrAggFactory().create(DataTypes.BOOLEAN(), null, null);
        assertThatThrownBy(() -> boolOrAgg.retract(true, true))
                .hasMessageContaining("Aggregate function 'bool_or' does not support retraction");
    }

    @Test
    public void testLastValueAgg() {
        FieldLastValueAgg lastValueAgg =
                new FieldLastValueAggFactory().create(DataTypes.INT(), null, null);
        assertThat(lastValueAgg.retract(1, 1)).isNull();
    }

    @Test
    public void testLastNonNullValueAgg() {
        FieldLastNonNullValueAgg lastNonNullValueAgg =
                new FieldLastNonNullValueAggFactory().create(DataTypes.INT(), null, null);
        assertThat(lastNonNullValueAgg.retract(1, 1)).isNull();
    }

    @Test
    public void testFieldProductAgg() {
        FieldProductAgg fieldProductAgg =
                new FieldProductAggFactory().create(DataTypes.INT(), null, null);
        assertThat(fieldProductAgg.retract(1, 1)).isNotNull();
    }

    @Test
    public void testFieldCollectAgg() {
        FieldCollectAgg fieldCollectAgg =
                new FieldCollectAggFactory()
                        .create(
                                DataTypes.ARRAY(DataTypes.INT()),
                                CoreOptions.fromMap(Collections.emptyMap()),
                                null);
        assertThat(
                        fieldCollectAgg.retract(
                                new GenericArray(new int[0]), new GenericArray(new int[0])))
                .isNotNull();
    }

    @Test
    public void testFieldPrimaryKeyAgg() {
        FieldPrimaryKeyAgg fieldPrimaryKeyAgg =
                new FieldPrimaryKeyAggFactory().create(DataTypes.INT(), null, null);
        assertThat(fieldPrimaryKeyAgg.retract(1, 1)).isNotNull();
    }

    @Test
    public void testFieldMergeMapAgg() {
        FieldMergeMapAgg fieldMergeMapAgg =
                new FieldMergeMapAggFactory()
                        .create(DataTypes.MAP(DataTypes.INT(), DataTypes.INT()), null, null);
        assertThat(
                        fieldMergeMapAgg.retract(
                                new GenericMap(Collections.emptyMap()),
                                new GenericMap(Collections.emptyMap())))
                .isNotNull();
    }
}
