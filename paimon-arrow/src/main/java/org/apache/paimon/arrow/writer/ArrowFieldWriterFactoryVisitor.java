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

package org.apache.paimon.arrow.writer;

import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.BinaryType;
import org.apache.paimon.types.BooleanType;
import org.apache.paimon.types.CharType;
import org.apache.paimon.types.DataTypeVisitor;
import org.apache.paimon.types.DateType;
import org.apache.paimon.types.DecimalType;
import org.apache.paimon.types.DoubleType;
import org.apache.paimon.types.FloatType;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.LocalZonedTimestampType;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.MultisetType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.SmallIntType;
import org.apache.paimon.types.TimeType;
import org.apache.paimon.types.TimestampType;
import org.apache.paimon.types.TinyIntType;
import org.apache.paimon.types.VarBinaryType;
import org.apache.paimon.types.VarCharType;
import org.apache.paimon.types.VariantType;

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;

import java.util.List;

/** Visitor to create {@link ArrowFieldWriterFactory}. */
public class ArrowFieldWriterFactoryVisitor implements DataTypeVisitor<ArrowFieldWriterFactory> {

    public static final ArrowFieldWriterFactoryVisitor INSTANCE =
            new ArrowFieldWriterFactoryVisitor();

    @Override
    public ArrowFieldWriterFactory visit(CharType charType) {
        return ArrowFieldWriters.StringWriter::new;
    }

    @Override
    public ArrowFieldWriterFactory visit(VarCharType varCharType) {
        return ArrowFieldWriters.StringWriter::new;
    }

    @Override
    public ArrowFieldWriterFactory visit(BooleanType booleanType) {
        return ArrowFieldWriters.BooleanWriter::new;
    }

    @Override
    public ArrowFieldWriterFactory visit(BinaryType binaryType) {
        return ArrowFieldWriters.BinaryWriter::new;
    }

    @Override
    public ArrowFieldWriterFactory visit(VarBinaryType varBinaryType) {
        return ArrowFieldWriters.BinaryWriter::new;
    }

    @Override
    public ArrowFieldWriterFactory visit(DecimalType decimalType) {
        return fieldVector ->
                new ArrowFieldWriters.DecimalWriter(
                        fieldVector, decimalType.getPrecision(), decimalType.getScale());
    }

    @Override
    public ArrowFieldWriterFactory visit(TinyIntType tinyIntType) {
        return ArrowFieldWriters.TinyIntWriter::new;
    }

    @Override
    public ArrowFieldWriterFactory visit(SmallIntType smallIntType) {
        return ArrowFieldWriters.SmallIntWriter::new;
    }

    @Override
    public ArrowFieldWriterFactory visit(IntType intType) {
        return ArrowFieldWriters.IntWriter::new;
    }

    @Override
    public ArrowFieldWriterFactory visit(BigIntType bigIntType) {
        return ArrowFieldWriters.BigIntWriter::new;
    }

    @Override
    public ArrowFieldWriterFactory visit(FloatType floatType) {
        return ArrowFieldWriters.FloatWriter::new;
    }

    @Override
    public ArrowFieldWriterFactory visit(DoubleType doubleType) {
        return ArrowFieldWriters.DoubleWriter::new;
    }

    @Override
    public ArrowFieldWriterFactory visit(DateType dateType) {
        return ArrowFieldWriters.DateWriter::new;
    }

    @Override
    public ArrowFieldWriterFactory visit(TimeType timeType) {
        return ArrowFieldWriters.TimeWriter::new;
    }

    @Override
    public ArrowFieldWriterFactory visit(TimestampType timestampType) {
        return fieldVector ->
                new ArrowFieldWriters.TimestampWriter(
                        fieldVector, timestampType.getPrecision(), null);
    }

    @Override
    public ArrowFieldWriterFactory visit(LocalZonedTimestampType localZonedTimestampType) {
        return fieldVector ->
                new ArrowFieldWriters.TimestampWriter(
                        fieldVector, localZonedTimestampType.getPrecision(), null);
    }

    @Override
    public ArrowFieldWriterFactory visit(VariantType variantType) {
        throw new UnsupportedOperationException("Doesn't support VariantType.");
    }

    @Override
    public ArrowFieldWriterFactory visit(ArrayType arrayType) {
        ArrowFieldWriterFactory elementWriterFactory = arrayType.getElementType().accept(this);
        return fieldVector ->
                new ArrowFieldWriters.ArrayWriter(
                        fieldVector,
                        elementWriterFactory.create(((ListVector) fieldVector).getDataVector()));
    }

    @Override
    public ArrowFieldWriterFactory visit(MultisetType multisetType) {
        throw new UnsupportedOperationException("Doesn't support MultisetType.");
    }

    @Override
    public ArrowFieldWriterFactory visit(MapType mapType) {
        ArrowFieldWriterFactory keyWriterFactory = mapType.getKeyType().accept(this);
        ArrowFieldWriterFactory valueWriterFactory = mapType.getValueType().accept(this);
        return fieldVector -> {
            MapVector mapVector = (MapVector) fieldVector;
            mapVector.reAlloc();
            List<FieldVector> keyValueVectors = mapVector.getDataVector().getChildrenFromFields();
            return new ArrowFieldWriters.MapWriter(
                    fieldVector,
                    keyWriterFactory.create(keyValueVectors.get(0)),
                    valueWriterFactory.create(keyValueVectors.get(1)));
        };
    }

    @Override
    public ArrowFieldWriterFactory visit(RowType rowType) {
        return fieldVector -> {
            List<FieldVector> children = fieldVector.getChildrenFromFields();
            ArrowFieldWriter[] fieldWriters = new ArrowFieldWriter[children.size()];
            for (int i = 0; i < children.size(); i++) {
                fieldWriters[i] = rowType.getTypeAt(i).accept(this).create(children.get(i));
            }
            return new ArrowFieldWriters.RowWriter(fieldVector, fieldWriters);
        };
    }
}
