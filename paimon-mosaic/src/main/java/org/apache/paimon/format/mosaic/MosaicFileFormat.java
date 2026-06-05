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

package org.apache.paimon.format.mosaic;

import org.apache.paimon.format.FileFormat;
import org.apache.paimon.format.FileFormatFactory;
import org.apache.paimon.format.FormatReaderFactory;
import org.apache.paimon.format.FormatWriterFactory;
import org.apache.paimon.format.SimpleStatsExtractor;
import org.apache.paimon.options.ConfigOption;
import org.apache.paimon.options.ConfigOptions;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.statistics.SimpleColStatsCollector;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.BinaryType;
import org.apache.paimon.types.BlobType;
import org.apache.paimon.types.BooleanType;
import org.apache.paimon.types.CharType;
import org.apache.paimon.types.DataType;
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
import org.apache.paimon.types.VectorType;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Optional;

/** Mosaic {@link FileFormat}. */
public class MosaicFileFormat extends FileFormat {

    public static final ConfigOption<String> STATS_COLUMNS =
            ConfigOptions.key("mosaic.stats-columns")
                    .stringType()
                    .defaultValue("")
                    .withDescription(
                            "Comma-separated list of column names to collect statistics for. "
                                    + "Empty means no statistics collection.");

    public static final ConfigOption<Integer> NUM_BUCKETS =
            ConfigOptions.key("mosaic.num-buckets")
                    .intType()
                    .noDefaultValue()
                    .withDescription("Number of column buckets for parallel IO.");

    static {
        System.setProperty("arrow.enable_unsafe_memory_access", "true");
    }

    private final FileFormatFactory.FormatContext formatContext;

    public MosaicFileFormat(FileFormatFactory.FormatContext formatContext) {
        super("mosaic");
        this.formatContext = formatContext;
    }

    @Override
    public FormatReaderFactory createReaderFactory(
            RowType dataSchemaRowType,
            RowType projectedRowType,
            @Nullable List<Predicate> predicates) {
        return new MosaicReaderFactory(dataSchemaRowType, projectedRowType, predicates);
    }

    @Override
    public FormatWriterFactory createWriterFactory(RowType type) {
        return new MosaicWriterFactory(type, formatContext);
    }

    @Override
    public void validateDataFields(RowType rowType) {
        MosaicRowTypeVisitor visitor = new MosaicRowTypeVisitor();
        for (DataType fieldType : rowType.getFieldTypes()) {
            fieldType.accept(visitor);
        }
    }

    @Override
    public Optional<SimpleStatsExtractor> createStatsExtractor(
            RowType type, SimpleColStatsCollector.Factory[] statsCollectors) {
        return Optional.of(new MosaicSimpleStatsExtractor(type, statsCollectors));
    }

    static class MosaicRowTypeVisitor implements DataTypeVisitor<Void> {

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
            return null;
        }

        @Override
        public Void visit(VariantType variantType) {
            throw new UnsupportedOperationException(
                    "Mosaic file format does not support type VARIANT");
        }

        @Override
        public Void visit(BlobType blobType) {
            throw new UnsupportedOperationException(
                    "Mosaic file format does not support type BLOB");
        }

        @Override
        public Void visit(ArrayType arrayType) {
            throw new UnsupportedOperationException(
                    "Mosaic file format does not support type ARRAY");
        }

        @Override
        public Void visit(VectorType vectorType) {
            throw new UnsupportedOperationException(
                    "Mosaic file format does not support type VECTOR");
        }

        @Override
        public Void visit(MultisetType multisetType) {
            throw new UnsupportedOperationException(
                    "Mosaic file format does not support type MULTISET");
        }

        @Override
        public Void visit(MapType mapType) {
            throw new UnsupportedOperationException("Mosaic file format does not support type MAP");
        }

        @Override
        public Void visit(RowType rowType) {
            throw new UnsupportedOperationException("Mosaic file format does not support type ROW");
        }
    }
}
