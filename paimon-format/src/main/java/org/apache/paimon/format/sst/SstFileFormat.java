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

package org.apache.paimon.format.sst;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.format.FileFormatFactory.FormatContext;
import org.apache.paimon.format.FormatReaderFactory;
import org.apache.paimon.format.FormatWriter;
import org.apache.paimon.format.FormatWriterFactory;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.reader.FileRecordReader;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeRoot;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.BloomFilter;
import org.apache.paimon.utils.RoaringBitmap32;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * A {@link FileFormat} for SST Files. Please refer to {@link org.apache.paimon.sst.SstFileWriter}
 * for more information.
 */
public class SstFileFormat extends FileFormat {
    private final Options options;
    private final MemorySize writeBatchMemory;

    public SstFileFormat(FormatContext context) {
        super(SstFileFormatFactory.IDENTIFIER);
        this.options = context.options();
        this.writeBatchMemory = context.writeBatchMemory();
    }

    @Override
    public FormatReaderFactory createReaderFactory(
            RowType dataSchemaRowType,
            RowType projectedRowType,
            @Nullable List<Predicate> filters) {
        throw new RuntimeException(
                "SST files are row-oriented kv store, please specify key type and value type on creating factories.");
    }

    @Override
    public FormatWriterFactory createWriterFactory(RowType type) {
        throw new RuntimeException(
                "SST files are row-oriented kv store, please specify key type and value type on creating factories.");
    }

    @Override
    public FormatReaderFactory createReaderFactory(
            RowType dataSchemaRowType,
            RowType projectedRowType,
            @Nullable List<Predicate> filters,
            RowType keyType,
            RowType valueType) {
        return new SstFileFormatReaderFactory(projectedRowType, keyType, valueType);
    }

    @Override
    public FormatWriterFactory createWriterFactory(
            RowType rowType, RowType keyType, RowType valueType) {
        return new SstFileFormatWriterFactory(
                options, rowType, keyType, valueType, writeBatchMemory);
    }

    @Override
    public void validateDataFields(RowType rowType) {
        List<DataType> fieldTypes = rowType.getFieldTypes();
        for (DataType dataType : fieldTypes) {
            validateDataType(dataType);
        }
    }

    private void validateDataType(DataType dataType) {
        // SST Files will serialize values into bytes, so that actually all data types are
        // supported.
        // todo: check key types are comparable
        DataTypeRoot typeRoot = dataType.getTypeRoot();
        switch (typeRoot) {
            case CHAR:
            case VARCHAR:
            case BOOLEAN:
            case BINARY:
            case VARBINARY:
            case DECIMAL:
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
            case FLOAT:
            case DOUBLE:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
            case ARRAY:
            case MAP:
            case ROW:
                // All types are supported in SST Files
                break;
            default:
                throw new UnsupportedOperationException(
                        "Unsupported data type for SST format: " + dataType);
        }
    }

    /** The {@link FormatReaderFactory} for SST Files. */
    private static class SstFileFormatReaderFactory implements FormatReaderFactory {
        private final RowType projectedRowType;
        private final RowType keyType;
        private final RowType valueType;

        public SstFileFormatReaderFactory(
                RowType projectedRowType, RowType keyType, RowType valueType) {
            this.projectedRowType = projectedRowType;
            this.keyType = keyType;
            this.valueType = valueType;
        }

        @Override
        public FileRecordReader<InternalRow> createReader(Context context) throws IOException {
            return new SstFormatReader(
                    context.fileIO(),
                    context.filePath(),
                    context.fileSize(),
                    convertSelection(context.selection()),
                    projectedRowType,
                    keyType,
                    valueType);
        }

        private List<Integer> convertSelection(RoaringBitmap32 selection) {
            if (selection == null) {
                return null;
            }
            List<Integer> result = new ArrayList<>();
            selection.iterator().forEachRemaining(result::add);
            return result;
        }
    }

    /** The {@link FormatWriterFactory} for SST Files. */
    private static class SstFileFormatWriterFactory implements FormatWriterFactory {
        private final Options options;
        private final RowType dataType;
        private final RowType keyType;
        private final RowType valueType;
        private final MemorySize writeBatchMemory;

        public SstFileFormatWriterFactory(
                Options options,
                RowType dataType,
                RowType keyType,
                RowType valueType,
                MemorySize writeBatchMemory) {
            this.options = options;
            this.keyType = keyType;
            this.valueType = valueType;
            this.dataType = dataType;
            this.writeBatchMemory = writeBatchMemory;
        }

        @Override
        public FormatWriter create(PositionOutputStream out, String compression)
                throws IOException {
            BloomFilter.Builder bloomFilter = null;
            boolean enableBloomFilter = options.get(SstOptions.BLOOM_FILTER_ENABLED);
            if (enableBloomFilter) {
                double fpp = options.get(SstOptions.BLOOM_FILTER_FPP);
                int estimatedEntryNum = options.get(SstOptions.BLOOM_FILTER_EXPECTED_ENTRY_NUM);
                bloomFilter = BloomFilter.builder(estimatedEntryNum, fpp);
            }

            return new SstFormatWriter(
                    out,
                    compression,
                    writeBatchMemory.getBytes(),
                    bloomFilter,
                    dataType,
                    keyType,
                    valueType);
        }
    }
}
