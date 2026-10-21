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

package org.apache.paimon.format.vortex;

import org.apache.paimon.arrow.vector.ArrowFormatCWriter;
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
import org.apache.paimon.types.VectorType;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Optional;

/** Vortex {@link FileFormat}. */
public class VortexFileFormat extends FileFormat {

    public static final ConfigOption<Integer> WORKER_THREADS =
            ConfigOptions.key("vortex.worker-threads")
                    .intType()
                    .defaultValue(4)
                    .withDescription(
                            "Minimum number of native threads that decode and encode Vortex data "
                                    + "in the background. The pool is shared by the whole JVM "
                                    + "and only grows. With 0 threads, only the thread that "
                                    + "reads or writes a file drives the native work for it.");

    /**
     * Each written batch becomes a native chunk and costs a native call, so small batches make
     * writes slow. The write batch memory still bounds every batch.
     */
    private static final int MIN_WRITE_BATCH_ROWS = 8192;

    static {
        System.setProperty("arrow.enable_unsafe_memory_access", "true");
    }

    private final FileFormatFactory.FormatContext formatContext;

    public VortexFileFormat(FileFormatFactory.FormatContext formatContext) {
        super("vortex");
        this.formatContext = formatContext;
    }

    @Override
    public FormatReaderFactory createReaderFactory(
            RowType dataSchemaRowType,
            RowType projectedRowType,
            @Nullable List<Predicate> predicates) {
        return new VortexReaderFactory(
                dataSchemaRowType, projectedRowType, predicates, workerThreads());
    }

    @Override
    public FormatWriterFactory createWriterFactory(RowType type) {
        return new VortexWriterFactory(
                () ->
                        new ArrowFormatCWriter(
                                type,
                                Math.max(formatContext.writeBatchSize(), MIN_WRITE_BATCH_ROWS),
                                true,
                                formatContext.writeBatchMemory().getBytes(),
                                null),
                workerThreads());
    }

    @Override
    public Optional<SimpleStatsExtractor> createStatsExtractor(
            RowType type, SimpleColStatsCollector.Factory[] statsCollectors) {
        return Optional.of(new VortexSimpleStatsExtractor(type, statsCollectors));
    }

    private int workerThreads() {
        return formatContext.options().get(WORKER_THREADS);
    }

    @Override
    public void validateDataFields(RowType rowType) {
        rowType.accept(new VortexRowTypeVisitor());
    }

    static class VortexRowTypeVisitor implements DataTypeVisitor<Void> {

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
                    "Vortex file format does not support type VARIANT");
        }

        @Override
        public Void visit(BlobType blobType) {
            throw new UnsupportedOperationException(
                    "Vortex file format does not support type BLOB");
        }

        @Override
        public Void visit(ArrayType arrayType) {
            return arrayType.getElementType().accept(this);
        }

        @Override
        public Void visit(VectorType vectorType) {
            return null;
        }

        @Override
        public Void visit(MultisetType multisetType) {
            throw new UnsupportedOperationException(
                    "Vortex file format does not support type MULTISET");
        }

        @Override
        public Void visit(MapType mapType) {
            throw new UnsupportedOperationException("Vortex file format does not support type MAP");
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
