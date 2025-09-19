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

package org.apache.paimon.format.lance;

import org.apache.paimon.arrow.vector.ArrowFormatWriter;
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.format.FileFormatFactory;
import org.apache.paimon.format.FormatReaderFactory;
import org.apache.paimon.format.FormatWriterFactory;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.BinaryType;
import org.apache.paimon.types.BlobType;
import org.apache.paimon.types.BooleanType;
import org.apache.paimon.types.CharType;
import org.apache.paimon.types.DataField;
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

import javax.annotation.Nullable;

import java.util.List;

/** Lance {@link FileFormat} for native. */
public class LanceFileFormat extends FileFormat {

    static {
        System.setProperty("arrow.enable_unsafe_memory_access", "true");
    }

    private final FileFormatFactory.FormatContext formatContext;

    public LanceFileFormat(FileFormatFactory.FormatContext formatContext) {
        super("lance");
        this.formatContext = formatContext;
    }

    @Override
    public FormatReaderFactory createReaderFactory(
            RowType dataSchemaRowType, RowType projectedRowType, @Nullable List<Predicate> list) {
        return new LanceReaderFactory(projectedRowType, formatContext.readBatchSize());
    }

    @Override
    public FormatWriterFactory createWriterFactory(RowType type) {
        return new LanceWriterFactory(
                () ->
                        new ArrowFormatWriter(
                                type,
                                formatContext.writeBatchSize(),
                                true,
                                formatContext.writeBatchMemory().getBytes()));
    }

    @Override
    public void validateDataFields(RowType rowType) {
        rowType.accept(new LanceRowTypeVisitor());
    }

    static class LanceRowTypeVisitor implements DataTypeVisitor<Void> {

        @Override
        public Void visit(CharType charType) {
            return null;
        }

        @Override
        public Void visit(VarCharType varCharType) {
            return null;
        }

        @Override
        public Void visit(BooleanType booleanType) {
            return null;
        }

        @Override
        public Void visit(BinaryType binaryType) {
            return null;
        }

        @Override
        public Void visit(VarBinaryType varBinaryType) {
            return null;
        }

        @Override
        public Void visit(DecimalType decimalType) {
            return null;
        }

        @Override
        public Void visit(TinyIntType tinyIntType) {
            return null;
        }

        @Override
        public Void visit(SmallIntType smallIntType) {
            return null;
        }

        @Override
        public Void visit(IntType intType) {
            return null;
        }

        @Override
        public Void visit(BigIntType bigIntType) {
            return null;
        }

        @Override
        public Void visit(FloatType floatType) {
            return null;
        }

        @Override
        public Void visit(DoubleType doubleType) {
            return null;
        }

        @Override
        public Void visit(DateType dateType) {
            return null;
        }

        @Override
        public Void visit(TimeType timeType) {
            return null;
        }

        @Override
        public Void visit(TimestampType timestampType) {
            return null;
        }

        @Override
        public Void visit(LocalZonedTimestampType localZonedTimestampType) {
            // lance has a bug here, if the local in GMT-10:00, it failed by reson: called
            // `Result::unwrap()` on an `Err` value: Schema { message: "Unsupported timestamp type:
            // timestamp:us:GMT-10:00", location: Location { file:
            // "rust/lance-core/src/datatypes.rs", line: 326, column: 39 } }
            // Disable it for now.
            throw new UnsupportedOperationException(
                    "Lance file format does not support type LOCAL_ZONED_TIMESTAMP");
        }

        @Override
        public Void visit(VariantType variantType) {
            return null;
        }

        @Override
        public Void visit(BlobType blobType) {
            return null;
        }

        @Override
        public Void visit(ArrayType arrayType) {
            return null;
        }

        @Override
        public Void visit(MultisetType multisetType) {
            return null;
        }

        @Override
        public Void visit(MapType mapType) {
            throw new UnsupportedOperationException("Lance file format does not support type MAP");
        }

        @Override
        public Void visit(RowType rowType) {
            for (DataField field : rowType.getFields()) {
                field.type().accept(this);
            }
            return null;
        }
    }
}
