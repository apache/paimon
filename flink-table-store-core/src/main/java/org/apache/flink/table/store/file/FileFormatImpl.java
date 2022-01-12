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

package org.apache.flink.table.store.file;

import org.apache.flink.api.common.serialization.BulkWriter;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.file.src.FileSourceSplit;
import org.apache.flink.connector.file.src.reader.BulkFormat;
import org.apache.flink.connector.file.table.factories.BulkReaderFormatFactory;
import org.apache.flink.connector.file.table.factories.BulkWriterFormatFactory;
import org.apache.flink.connector.file.table.format.BulkDecodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import java.util.List;

import static org.apache.flink.table.types.utils.TypeConversions.fromLogicalToDataType;

/** A {@link FileFormat} which discovers reader and writer from format identifier. */
public class FileFormatImpl implements FileFormat {

    private final ClassLoader classLoader;
    private final String formatIdentifier;
    private final ReadableConfig formatOptions;

    public FileFormatImpl(
            ClassLoader classLoader, String formatIdentifier, ReadableConfig formatOptions) {
        this.classLoader = classLoader;
        this.formatIdentifier = formatIdentifier;
        this.formatOptions = formatOptions;
    }

    @Override
    public BulkFormat<RowData, FileSourceSplit> createReaderFactory(
            RowType rowType, List<ResolvedExpression> filters) {
        BulkDecodingFormat<RowData> decodingFormat =
                FactoryUtil.discoverFactory(
                                classLoader, BulkReaderFormatFactory.class, formatIdentifier)
                        .createDecodingFormat(null, formatOptions); // context is useless
        decodingFormat.applyFilters(filters);
        return decodingFormat.createRuntimeDecoder(SOURCE_CONTEXT, fromLogicalToDataType(rowType));
    }

    @Override
    public BulkWriter.Factory<RowData> createWriterFactory(RowType rowType) {
        return FactoryUtil.discoverFactory(
                        classLoader, BulkWriterFormatFactory.class, formatIdentifier)
                .createEncodingFormat(null, formatOptions) // context is useless
                .createRuntimeEncoder(SINK_CONTEXT, fromLogicalToDataType(rowType));
    }

    private static final DynamicTableSink.Context SINK_CONTEXT =
            new DynamicTableSink.Context() {

                @Override
                public boolean isBounded() {
                    return false;
                }

                @Override
                public <T> TypeInformation<T> createTypeInformation(DataType consumedDataType) {
                    return InternalTypeInfo.of(consumedDataType.getLogicalType());
                }

                @Override
                public <T> TypeInformation<T> createTypeInformation(
                        LogicalType consumedLogicalType) {
                    return InternalTypeInfo.of(consumedLogicalType);
                }

                @Override
                public DynamicTableSink.DataStructureConverter createDataStructureConverter(
                        DataType consumedDataType) {
                    throw new UnsupportedOperationException();
                }
            };

    private static final DynamicTableSource.Context SOURCE_CONTEXT =
            new DynamicTableSource.Context() {

                @Override
                public <T> TypeInformation<T> createTypeInformation(DataType consumedDataType) {
                    return InternalTypeInfo.of(consumedDataType.getLogicalType());
                }

                @Override
                public <T> TypeInformation<T> createTypeInformation(
                        LogicalType producedLogicalType) {
                    return InternalTypeInfo.of(producedLogicalType);
                }

                @Override
                public DynamicTableSource.DataStructureConverter createDataStructureConverter(
                        DataType consumedDataType) {
                    throw new UnsupportedOperationException();
                }
            };
}
