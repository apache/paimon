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

package org.apache.paimon.data.variant;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.VariantType;

import org.junit.jupiter.api.Test;

import static org.apache.paimon.data.variant.PaimonShreddingUtils.assembleVariant;
import static org.apache.paimon.data.variant.PaimonShreddingUtils.buildVariantSchema;
import static org.apache.paimon.data.variant.PaimonShreddingUtils.variantShreddingSchema;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link VariantShreddingWritePlan}. */
class VariantShreddingWritePlanTest {

    @Test
    void testConfiguredSchemaConvertsTopLevelVariant() {
        RowType logicalType =
                DataTypes.ROW(
                        DataTypes.FIELD(0, "id", DataTypes.INT()),
                        DataTypes.FIELD(1, "v", DataTypes.VARIANT()));
        RowType configuredSchema =
                DataTypes.ROW(
                        DataTypes.FIELD(
                                1,
                                "v",
                                DataTypes.ROW(
                                        DataTypes.FIELD(0, "age", DataTypes.BIGINT()),
                                        DataTypes.FIELD(1, "name", DataTypes.STRING()))));

        VariantShreddingWritePlan writePlan =
                VariantShreddingWritePlan.fromConfiguredSchema(logicalType, configuredSchema);

        assertThat(writePlan.physicalRowType().getTypeAt(1)).isInstanceOf(RowType.class);
        assertThat(writePlan.physicalRowType().getTypeAt(1)).isNotInstanceOf(VariantType.class);

        GenericVariant variant = GenericVariant.fromJson("{\"age\":30,\"name\":\"Alice\"}");
        InternalRow physicalRow = writePlan.toPhysicalRow(GenericRow.of(1, variant));
        RowType physicalVariantType = (RowType) writePlan.physicalRowType().getTypeAt(1);
        InternalRow shreddedVariant = physicalRow.getRow(1, physicalVariantType.getFieldCount());

        assertThat(physicalRow.getInt(0)).isEqualTo(1);
        assertThat(
                        assembleVariant(shreddedVariant, buildVariantSchema(physicalVariantType))
                                .toJson())
                .isEqualTo(variant.toJson());
    }

    @Test
    void testNestedVariantConvertsRecursively() {
        RowType nestedLogicalType =
                DataTypes.ROW(
                        DataTypes.FIELD(0, "payload", DataTypes.VARIANT()),
                        DataTypes.FIELD(1, "name", DataTypes.STRING()));
        RowType logicalType = DataTypes.ROW(DataTypes.FIELD(0, "nested", nestedLogicalType));

        RowType payloadPhysicalType =
                variantShreddingSchema(DataTypes.ROW(DataTypes.FIELD(0, "score", DataTypes.INT())));
        RowType nestedPhysicalType =
                DataTypes.ROW(
                        DataTypes.FIELD(0, "payload", payloadPhysicalType),
                        DataTypes.FIELD(1, "name", DataTypes.STRING()));
        RowType physicalType = DataTypes.ROW(DataTypes.FIELD(0, "nested", nestedPhysicalType));

        VariantShreddingWritePlan writePlan =
                new VariantShreddingWritePlan(logicalType, physicalType);
        GenericVariant variant = GenericVariant.fromJson("{\"score\":98}");
        InternalRow physicalRow =
                writePlan.toPhysicalRow(
                        GenericRow.of(
                                GenericRow.of(variant, BinaryString.fromString("attempt-1"))));

        InternalRow nestedRow = physicalRow.getRow(0, nestedPhysicalType.getFieldCount());
        InternalRow shreddedVariant = nestedRow.getRow(0, payloadPhysicalType.getFieldCount());

        assertThat(nestedRow.getString(1)).isEqualTo(BinaryString.fromString("attempt-1"));
        assertThat(
                        assembleVariant(shreddedVariant, buildVariantSchema(payloadPhysicalType))
                                .toJson())
                .isEqualTo(variant.toJson());
    }
}
