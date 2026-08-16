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

package org.apache.paimon.arrow.vector;

import org.apache.paimon.arrow.ArrowFieldTypeConversion;
import org.apache.paimon.arrow.converter.Arrow2PaimonVectorConverter;
import org.apache.paimon.arrow.writer.ArrowFieldWriter;
import org.apache.paimon.arrow.writer.ArrowFieldWriterFactory;
import org.apache.paimon.arrow.writer.ArrowFieldWriterFactoryVisitor;
import org.apache.paimon.data.DataGetters;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.columnar.ColumnVector;
import org.apache.paimon.data.columnar.DecimalColumnVector;
import org.apache.paimon.types.DecimalType;

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.FieldType;

import javax.annotation.Nullable;

/**
 * Test for custom ArrowFormatWriter and ArrowBatchReader that converts Paimon decimal type to Arrow
 * binary format and vice versa.
 */
public class CustomDecimalArrowConversion {
    /** Custom ArrowFieldTypeFactory for decimal type. */
    public static class CustomArrowFieldTypeFactory
            extends ArrowFieldTypeConversion.ArrowFieldTypeVisitor {
        @Override
        public FieldType visit(DecimalType decimalType) {
            return new FieldType(decimalType.isNullable(), new ArrowType.Binary(), null);
        }
    }

    /** Custom ArrowFieldWriterFactory for decimal type. */
    public static class CustomArrowFieldWriterFactory extends ArrowFieldWriterFactoryVisitor {
        @Override
        public ArrowFieldWriterFactory visit(DecimalType decimalType) {
            return (fieldVector, isNullable) ->
                    new DecimalWriter(
                            fieldVector,
                            decimalType.getPrecision(),
                            decimalType.getScale(),
                            isNullable);
        }

        /** Custom ArrowFieldWriter for decimal type. */
        public static class DecimalWriter extends ArrowFieldWriter {
            // decimal precision
            private final int precision;
            // decimal scale.
            private final int scale;

            public DecimalWriter(
                    FieldVector fieldVector, int precision, int scale, boolean isNullable) {
                super(fieldVector, isNullable);
                this.precision = precision;
                this.scale = scale;
            }

            protected void doWrite(
                    ColumnVector columnVector,
                    @Nullable int[] pickedInColumn,
                    int startIndex,
                    int batchRows) {
                VarBinaryVector decimalVector = (VarBinaryVector) this.fieldVector;

                for (int i = 0; i < batchRows; ++i) {
                    int row = this.getRowNumber(startIndex, i, pickedInColumn);
                    if (columnVector.isNullAt(row)) {
                        decimalVector.setNull(i);
                    } else {
                        Decimal value =
                                ((DecimalColumnVector) columnVector)
                                        .getDecimal(row, this.precision, this.scale);
                        byte[] bytes = value.toUnscaledBytes();
                        decimalVector.setSafe(i, bytes);
                    }
                }
            }

            protected void doWrite(int rowIndex, DataGetters getters, int pos) {
                ((VarBinaryVector) this.fieldVector)
                        .setSafe(
                                rowIndex,
                                getters.getDecimal(pos, this.precision, this.scale)
                                        .toUnscaledBytes());
            }
        }
    }

    /** Custom Arrow2PaimonVectorConvertorVisitor for decimal type. */
    public static class CustomArrow2PaimonVectorConvertorVisitor
            extends Arrow2PaimonVectorConverter.Arrow2PaimonVectorConvertorVisitor {
        @Override
        public Arrow2PaimonVectorConverter visit(DecimalType decimalType) {
            return vector ->
                    new DecimalColumnVector() {

                        @Override
                        public boolean isNullAt(int index) {
                            return vector.isNull(index);
                        }

                        @Override
                        public Decimal getDecimal(int index, int precision, int scale) {
                            byte[] bytes = ((VarBinaryVector) vector).getObject(index);
                            return Decimal.fromUnscaledBytes(bytes, precision, scale);
                        }
                    };
        }
    }
}
