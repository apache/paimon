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

import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.shredding.ShreddingWritePlan;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.VariantType;
import org.apache.paimon.utils.Preconditions;

import java.util.ArrayList;
import java.util.List;

/** A physical write plan for Variant shredding. */
public class VariantShreddingWritePlan implements ShreddingWritePlan {

    private final RowType logicalRowType;
    private final RowType physicalRowType;
    private final RowConverter rowConverter;

    public VariantShreddingWritePlan(RowType logicalRowType, RowType physicalRowType) {
        this.logicalRowType = logicalRowType;
        this.physicalRowType = physicalRowType;
        this.rowConverter = RowConverter.create(logicalRowType, physicalRowType);
    }

    public static VariantShreddingWritePlan fromConfiguredSchema(
            RowType logicalRowType, RowType configuredShreddingSchema) {
        return new VariantShreddingWritePlan(
                logicalRowType,
                configuredPhysicalRowType(logicalRowType, configuredShreddingSchema));
    }

    @Override
    public RowType logicalRowType() {
        return logicalRowType;
    }

    @Override
    public RowType physicalRowType() {
        return physicalRowType;
    }

    @Override
    public InternalRow toPhysicalRow(InternalRow row) {
        return rowConverter.convert(row);
    }

    private static RowType configuredPhysicalRowType(
            RowType logicalRowType, RowType configuredShreddingSchema) {
        List<DataField> fields = new ArrayList<>();
        for (DataField field : logicalRowType.getFields()) {
            if (field.type() instanceof VariantType
                    && configuredShreddingSchema.containsField(field.name())) {
                RowType fieldShreddingSchema =
                        PaimonShreddingUtils.variantShreddingSchema(
                                configuredShreddingSchema.getField(field.name()).type());
                fields.add(field.newType(fieldShreddingSchema));
            } else {
                fields.add(field);
            }
        }
        return new RowType(logicalRowType.isNullable(), fields);
    }

    private interface FieldConverter {

        Object convert(InternalRow row);

        boolean isIdentity();
    }

    private static class RowConverter {

        private final FieldConverter[] fieldConverters;
        private final boolean identity;

        private RowConverter(FieldConverter[] fieldConverters, boolean identity) {
            this.fieldConverters = fieldConverters;
            this.identity = identity;
        }

        private static RowConverter create(RowType logicalRowType, RowType physicalRowType) {
            Preconditions.checkArgument(
                    logicalRowType.getFieldCount() == physicalRowType.getFieldCount(),
                    "Logical and physical row types should have the same field count.");

            FieldConverter[] converters = new FieldConverter[logicalRowType.getFieldCount()];
            boolean identity = true;
            for (int i = 0; i < converters.length; i++) {
                DataType logicalType = logicalRowType.getTypeAt(i);
                DataType physicalType = physicalRowType.getTypeAt(i);
                converters[i] = createFieldConverter(i, logicalType, physicalType);
                identity = identity && converters[i].isIdentity();
            }
            return new RowConverter(converters, identity);
        }

        private InternalRow convert(InternalRow row) {
            if (identity) {
                return row;
            }

            GenericRow physicalRow = new GenericRow(row.getRowKind(), fieldConverters.length);
            for (int i = 0; i < fieldConverters.length; i++) {
                physicalRow.setField(i, fieldConverters[i].convert(row));
            }
            return physicalRow;
        }

        private static FieldConverter createFieldConverter(
                int fieldIndex, DataType logicalType, DataType physicalType) {
            if (logicalType instanceof VariantType && physicalType instanceof RowType) {
                return new VariantFieldConverter(
                        fieldIndex,
                        PaimonShreddingUtils.buildVariantSchema((RowType) physicalType));
            }

            if (logicalType instanceof RowType && physicalType instanceof RowType) {
                RowConverter nestedConverter =
                        RowConverter.create((RowType) logicalType, (RowType) physicalType);
                if (!nestedConverter.identity) {
                    return new RowFieldConverter(
                            fieldIndex, (RowType) logicalType, nestedConverter);
                }
            }

            return new IdentityFieldConverter(fieldIndex, logicalType);
        }
    }

    private static class IdentityFieldConverter implements FieldConverter {

        private final InternalRow.FieldGetter fieldGetter;

        private IdentityFieldConverter(int fieldIndex, DataType logicalType) {
            this.fieldGetter = InternalRow.createFieldGetter(logicalType, fieldIndex);
        }

        @Override
        public Object convert(InternalRow row) {
            return fieldGetter.getFieldOrNull(row);
        }

        @Override
        public boolean isIdentity() {
            return true;
        }
    }

    private static class RowFieldConverter implements FieldConverter {

        private final int fieldIndex;
        private final int fieldCount;
        private final RowConverter rowConverter;

        private RowFieldConverter(
                int fieldIndex, RowType logicalRowType, RowConverter rowConverter) {
            this.fieldIndex = fieldIndex;
            this.fieldCount = logicalRowType.getFieldCount();
            this.rowConverter = rowConverter;
        }

        @Override
        public Object convert(InternalRow row) {
            if (row.isNullAt(fieldIndex)) {
                return null;
            }
            return rowConverter.convert(row.getRow(fieldIndex, fieldCount));
        }

        @Override
        public boolean isIdentity() {
            return false;
        }
    }

    private static class VariantFieldConverter implements FieldConverter {

        private final int fieldIndex;
        private final VariantSchema variantSchema;

        private VariantFieldConverter(int fieldIndex, VariantSchema variantSchema) {
            this.fieldIndex = fieldIndex;
            this.variantSchema = variantSchema;
        }

        @Override
        public Object convert(InternalRow row) {
            if (row.isNullAt(fieldIndex)) {
                return null;
            }
            return PaimonShreddingUtils.castShredded(
                    (GenericVariant) row.getVariant(fieldIndex), variantSchema);
        }

        @Override
        public boolean isIdentity() {
            return false;
        }
    }
}
