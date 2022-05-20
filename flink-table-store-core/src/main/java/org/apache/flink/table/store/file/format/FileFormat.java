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

package org.apache.flink.table.store.file.format;

import org.apache.flink.api.common.serialization.BulkWriter;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DelegatingConfiguration;
import org.apache.flink.connector.file.src.FileSourceSplit;
import org.apache.flink.connector.file.src.reader.BulkFormat;
import org.apache.flink.connector.file.table.format.BulkDecodingFormat;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.format.ProjectableDecodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.store.file.stats.FileStatsExtractor;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Preconditions;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.ServiceLoader;

import static org.apache.flink.table.types.utils.TypeConversions.fromLogicalToDataType;

/**
 * Factory class which creates reader and writer factories for specific file format.
 *
 * <p>NOTE: This class must be thread safe.
 */
public abstract class FileFormat {

    protected String formatIdentifier;

    protected FileFormat(String formatIdentifier) {
        this.formatIdentifier = formatIdentifier;
    }

    protected abstract BulkDecodingFormat<RowData> getDecodingFormat();

    protected abstract EncodingFormat<BulkWriter.Factory<RowData>> getEncodingFormat();

    public String getFormatIdentifier() {
        return formatIdentifier;
    }

    /**
     * Create a {@link BulkFormat} from the type, with projection pushed down.
     *
     * @param type Type without projection.
     * @param projection See {@link org.apache.flink.table.connector.Projection#toNestedIndexes()}.
     * @param filters A list of filters in conjunctive form for filtering on a best-effort basis.
     */
    @SuppressWarnings("unchecked")
    public BulkFormat<RowData, FileSourceSplit> createReaderFactory(
            RowType type, int[][] projection, List<ResolvedExpression> filters) {
        BulkDecodingFormat<RowData> decodingFormat = getDecodingFormat();
        // TODO use ProjectingBulkFormat if not supported
        Preconditions.checkState(
                decodingFormat instanceof ProjectableDecodingFormat,
                "Format "
                        + decodingFormat.getClass().getName()
                        + " does not support projection push down");
        decodingFormat.applyFilters(filters);
        return ((ProjectableDecodingFormat<BulkFormat<RowData, FileSourceSplit>>) decodingFormat)
                .createRuntimeDecoder(SOURCE_CONTEXT, fromLogicalToDataType(type), projection);
    }

    /** Create a {@link BulkWriter.Factory} from the type. */
    public BulkWriter.Factory<RowData> createWriterFactory(RowType type) {
        return getEncodingFormat().createRuntimeEncoder(SINK_CONTEXT, fromLogicalToDataType(type));
    }

    public BulkFormat<RowData, FileSourceSplit> createReaderFactory(RowType rowType) {
        int[][] projection = new int[rowType.getFieldCount()][];
        for (int i = 0; i < projection.length; i++) {
            projection[i] = new int[] {i};
        }
        return createReaderFactory(rowType, projection);
    }

    public BulkFormat<RowData, FileSourceSplit> createReaderFactory(
            RowType rowType, int[][] projection) {
        return createReaderFactory(rowType, projection, new ArrayList<>());
    }

    public Optional<FileStatsExtractor> createStatsExtractor(RowType type) {
        return Optional.empty();
    }

    /** Create a {@link FileFormatImpl} from table options. */
    public static FileFormat fromTableOptions(
            ClassLoader classLoader,
            Configuration tableOptions,
            ConfigOption<String> formatOption) {
        String formatIdentifier = tableOptions.get(formatOption);
        DelegatingConfiguration formatOptions =
                new DelegatingConfiguration(tableOptions, formatIdentifier + ".");
        return fromIdentifier(classLoader, formatIdentifier, formatOptions);
    }

    /** Create a {@link FileFormatImpl} from format identifier and format options. */
    public static FileFormat fromIdentifier(
            ClassLoader classLoader, String formatIdentifier, Configuration formatOptions) {
        ServiceLoader<FileFormatFactory> serviceLoader =
                ServiceLoader.load(FileFormatFactory.class);
        for (FileFormatFactory factory : serviceLoader) {
            if (factory.identifier().equals(formatIdentifier.toLowerCase())) {
                return factory.create(formatOptions);
            }
        }
        return new FileFormatImpl(classLoader, formatIdentifier, formatOptions);
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
