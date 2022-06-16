/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.store.file.data;

import org.apache.flink.connector.file.src.FileSourceSplit;
import org.apache.flink.connector.file.src.reader.BulkFormat;
import org.apache.flink.connector.file.src.util.RecordAndPosition;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.connector.Projection;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.store.file.KeyValue;
import org.apache.flink.table.store.file.KeyValueSerializer;
import org.apache.flink.table.store.file.format.FileFormat;
import org.apache.flink.table.store.file.utils.FileStorePathFactory;
import org.apache.flink.table.store.file.utils.FileUtils;
import org.apache.flink.table.store.file.utils.RecordReader;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.BinaryType;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.types.logical.utils.LogicalTypeCasts;
import org.apache.flink.table.types.utils.LogicalTypeDataTypeConverter;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.apache.flink.table.types.logical.LogicalTypeFamily.APPROXIMATE_NUMERIC;
import static org.apache.flink.table.types.logical.LogicalTypeFamily.EXACT_NUMERIC;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.BINARY;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.CHAR;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE;
import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.getLength;
import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.getPrecision;
import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.getScale;

/**
 * Reads {@link KeyValue}s from data files.
 *
 * <p>NOTE: If the key exists, the data is sorted according to the key and the key projection will
 * cause the orderliness of the data to fail.
 */
public class DataFileReader {

    private final RowType keyType;
    private final RowType valueType;
    private final BulkFormat<RowData, FileSourceSplit> readerFactory;
    private final DataFilePathFactory pathFactory;

    private DataFileReader(
            RowType keyType,
            RowType valueType,
            BulkFormat<RowData, FileSourceSplit> readerFactory,
            DataFilePathFactory pathFactory) {
        this.keyType = keyType;
        this.valueType = valueType;
        this.readerFactory = readerFactory;
        this.pathFactory = pathFactory;
    }

    public RecordReader<KeyValue> read(String fileName) throws IOException {
        return new DataFileRecordReader(pathFactory.toPath(fileName));
    }

    private class DataFileRecordReader implements RecordReader<KeyValue> {

        private final BulkFormat.Reader<RowData> reader;
        private final KeyValueSerializer serializer;

        private DataFileRecordReader(Path path) throws IOException {
            long fileSize = FileUtils.getFileSize(path);
            FileSourceSplit split = new FileSourceSplit("ignore", path, 0, fileSize, 0, fileSize);
            this.reader = readerFactory.createReader(FileUtils.DEFAULT_READER_CONFIG, split);
            this.serializer = new KeyValueSerializer(keyType, valueType);
        }

        @Nullable
        @Override
        public RecordIterator<KeyValue> readBatch() throws IOException {
            BulkFormat.RecordIterator<RowData> iterator = reader.readBatch();
            return iterator == null ? null : new DataFileRecordIterator(iterator, serializer);
        }

        @Override
        public void close() throws IOException {
            reader.close();
        }
    }

    private static class DataFileRecordIterator implements RecordReader.RecordIterator<KeyValue> {

        private final BulkFormat.RecordIterator<RowData> iterator;
        private final KeyValueSerializer serializer;

        private DataFileRecordIterator(
                BulkFormat.RecordIterator<RowData> iterator, KeyValueSerializer serializer) {
            this.iterator = iterator;
            this.serializer = serializer;
        }

        @Override
        public KeyValue next() throws IOException {
            RecordAndPosition<RowData> result = iterator.next();
            return result == null ? null : serializer.fromRow(result.getRecord());
        }

        @Override
        public void releaseBatch() {
            iterator.releaseBatch();
        }
    }

    /** Creates {@link DataFileReader}. */
    public static class Factory {

        private final RowType keyType;
        private final RowType valueType;
        private final FileFormat fileFormat;
        private final FileStorePathFactory pathFactory;

        private int[][] keyProjection;
        private int[][] valueProjection;
        private RowType projectedKeyType;
        private RowType projectedValueType;

        public Factory(
                RowType keyType,
                RowType valueType,
                FileFormat fileFormat,
                FileStorePathFactory pathFactory) {
            this.keyType = keyType;
            this.valueType = valueType;
            this.fileFormat = fileFormat;
            this.pathFactory = pathFactory;

            this.keyProjection = Projection.range(0, keyType.getFieldCount()).toNestedIndexes();
            this.valueProjection = Projection.range(0, valueType.getFieldCount()).toNestedIndexes();
            applyProjection();
            translateProjection();
        }

        public Factory withKeyProjection(int[][] projection) {
            keyProjection = projection;
            applyProjection();
            return this;
        }

        public Factory withValueProjection(int[][] projection) {
            valueProjection = projection;
            applyProjection();
            return this;
        }

        public DataFileReader create(BinaryRowData partition, int bucket) {
            RowType recordType = KeyValue.schema(keyType, valueType);
            int[][] projection =
                    KeyValue.project(keyProjection, valueProjection, keyType.getFieldCount());
            return new DataFileReader(
                    projectedKeyType,
                    projectedValueType,
                    fileFormat.createReaderFactory(recordType, projection),
                    pathFactory.createDataFilePathFactory(partition, bucket));
        }

        private void applyProjection() {
            projectedKeyType = (RowType) Projection.of(keyProjection).project(keyType);
            projectedValueType = (RowType) Projection.of(valueProjection).project(valueType);
        }
        private void translateProjection(){
            projectedKeyType = translateOldToNewByRowType(projectedKeyType);
            projectedValueType = translateOldToNewByRowType(projectedValueType);
        }
    
        private RowType translateOldToNewByRowType(RowType oldData) {
            List<RowType.RowField> newData = oldData.getFields().stream().map(x -> {
                DataType newDataType = Objects.requireNonNull(findDataTypeOfRoot(LogicalTypeDataTypeConverter.toDataType(x.getType()), x.getType().getTypeRoot()));
                LogicalType newLogicalType =  LogicalTypeDataTypeConverter.toLogicalType(newDataType);
                return new RowType.RowField(x.getName(), newLogicalType);
            }).collect(Collectors.toList());
            return new RowType(newData);
        }
    
        /**
         * Returns a data type for the given data type and expected root.
         *
         * <p>This method is aligned with {@link LogicalTypeCasts#supportsImplicitCast(LogicalType,
         * LogicalType)}.
         *
         * <p>The "fallback" data type for each root represents the default data type for a NULL
         * literal. NULL literals will receive the smallest precision possible for having little impact
         * when finding a common type. The output of this method needs to be checked again if an
         * implicit cast is supported.
         */
        private static @Nullable DataType findDataTypeOfRoot(
            DataType actualDataType, LogicalTypeRoot expectedRoot) {
            final LogicalType actualType = actualDataType.getLogicalType();
            if (actualType.is(expectedRoot)) {
                return actualDataType;
            }
            switch (expectedRoot) {
                case CHAR:
                    return DataTypes.CHAR(CharType.DEFAULT_LENGTH);
                case VARCHAR:
                    if (actualType.is(CHAR)) {
                        return DataTypes.VARCHAR(getLength(actualType));
                    }
                    return DataTypes.VARCHAR(VarCharType.DEFAULT_LENGTH);
                case BOOLEAN:
                    return DataTypes.BOOLEAN();
                case BINARY:
                    return DataTypes.BINARY(BinaryType.DEFAULT_LENGTH);
                case VARBINARY:
                    if (actualType.is(BINARY)) {
                        return DataTypes.VARBINARY(getLength(actualType));
                    }
                    return DataTypes.VARBINARY(VarBinaryType.DEFAULT_LENGTH);
                case DECIMAL:
                    if (actualType.is(EXACT_NUMERIC)) {
                        return DataTypes.DECIMAL(getPrecision(actualType), getScale(actualType));
                    } else if (actualType.is(APPROXIMATE_NUMERIC)) {
                        final int precision = getPrecision(actualType);
                        // we don't know where the precision occurs (before or after the dot)
                        return DataTypes.DECIMAL(precision * 2, precision);
                    }
                    return DataTypes.DECIMAL(DecimalType.MIN_PRECISION, DecimalType.MIN_SCALE);
                case TINYINT:
                    return DataTypes.TINYINT();
                case SMALLINT:
                    return DataTypes.SMALLINT();
                case INTEGER:
                    return DataTypes.INT();
                case BIGINT:
                    return DataTypes.BIGINT();
                case FLOAT:
                    return DataTypes.FLOAT();
                case DOUBLE:
                    return DataTypes.DOUBLE();
                case DATE:
                    return DataTypes.DATE();
                case TIME_WITHOUT_TIME_ZONE:
                    if (actualType.is(TIMESTAMP_WITHOUT_TIME_ZONE)) {
                        return DataTypes.TIME(getPrecision(actualType));
                    }
                    return DataTypes.TIME();
                case TIMESTAMP_WITHOUT_TIME_ZONE:
                    return DataTypes.TIMESTAMP();
                case TIMESTAMP_WITH_TIME_ZONE:
                    return DataTypes.TIMESTAMP_WITH_TIME_ZONE();
                case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                    return DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE();
                case INTERVAL_YEAR_MONTH:
                    return DataTypes.INTERVAL(DataTypes.MONTH());
                case INTERVAL_DAY_TIME:
                    return DataTypes.INTERVAL(DataTypes.SECOND());
                case NULL:
                    return DataTypes.NULL();
                case ARRAY:
                case MULTISET:
                case MAP:
                case ROW:
                case DISTINCT_TYPE:
                case STRUCTURED_TYPE:
                case RAW:
                case SYMBOL:
                case UNRESOLVED:
                default:
                    return null;
            }
        }
    }
}
