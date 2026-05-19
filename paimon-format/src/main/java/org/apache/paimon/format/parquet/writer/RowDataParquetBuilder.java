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

package org.apache.paimon.format.parquet.writer;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.format.FormatWriter;
import org.apache.paimon.format.parquet.ColumnConfigParser;
import org.apache.paimon.format.parquet.MapShreddingKeyExtractor;
import org.apache.paimon.format.parquet.MapShreddingUtils;
import org.apache.paimon.format.parquet.VariantUtils;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.options.Options;
import org.apache.paimon.types.RowType;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.hadoop.ParquetOutputFormat;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.OutputFile;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/** A {@link ParquetBuilder} for {@link InternalRow}. */
public class RowDataParquetBuilder implements ParquetBuilder<InternalRow> {

    private final RowType rowType;
    private final Configuration conf;
    private final Options mapShreddingOptions;
    @Nullable private RowType shreddingSchemas;

    public RowDataParquetBuilder(RowType rowType, Options options) {
        this(rowType, options, options);
    }

    public RowDataParquetBuilder(RowType rowType, Options options, Options mapShreddingOptions) {
        this.rowType = rowType;
        this.conf = new Configuration(false);
        this.mapShreddingOptions = mapShreddingOptions;
        this.shreddingSchemas = VariantUtils.shreddingSchemasFromOptions(options);
        options.toMap().forEach(conf::set);
    }

    public RowDataParquetBuilder withShreddingSchemas(RowType shreddingSchemas) {
        this.shreddingSchemas = shreddingSchemas;
        return this;
    }

    @Override
    public ParquetWriter<InternalRow> createWriter(OutputFile out, String compression)
            throws IOException {
        return createWriter(out, compression, Collections.emptyMap());
    }

    public FormatWriter createFormatWriter(PositionOutputStream stream, String compression)
            throws IOException {
        if (MapShreddingUtils.isMapShreddingEnabled(mapShreddingOptions)) {
            return new MapShreddingFormatWriter(stream, compression);
        }
        OutputFile out = new StreamOutputFile(stream);
        return new ParquetBulkWriter(createWriter(out, compression));
    }

    private ParquetWriter<InternalRow> createWriter(
            OutputFile out, String compression, Map<String, List<String>> dynamicMapKeys)
            throws IOException {
        ParquetRowDataBuilder builder =
                new ParquetRowDataBuilder(out, rowType, shreddingSchemas, dynamicMapKeys)
                        .withConf(conf)
                        .withCompressionCodec(
                                CompressionCodecName.fromConf(getCompression(compression)))
                        .withRowGroupSize(
                                conf.getLong(
                                        ParquetOutputFormat.BLOCK_SIZE,
                                        ParquetWriter.DEFAULT_BLOCK_SIZE))
                        .withPageSize(
                                conf.getInt(
                                        ParquetOutputFormat.PAGE_SIZE,
                                        ParquetWriter.DEFAULT_PAGE_SIZE))
                        .withPageRowCountLimit(
                                conf.getInt(
                                        ParquetOutputFormat.PAGE_ROW_COUNT_LIMIT,
                                        ParquetProperties.DEFAULT_PAGE_ROW_COUNT_LIMIT))
                        .withDictionaryPageSize(
                                conf.getInt(
                                        ParquetOutputFormat.DICTIONARY_PAGE_SIZE,
                                        ParquetProperties.DEFAULT_DICTIONARY_PAGE_SIZE))
                        .withMaxPaddingSize(
                                conf.getInt(
                                        ParquetOutputFormat.MAX_PADDING_BYTES,
                                        ParquetWriter.MAX_PADDING_SIZE_DEFAULT))
                        .withDictionaryEncoding(
                                conf.getBoolean(
                                        ParquetOutputFormat.ENABLE_DICTIONARY,
                                        ParquetProperties.DEFAULT_IS_DICTIONARY_ENABLED))
                        .withValidation(conf.getBoolean(ParquetOutputFormat.VALIDATION, false))
                        .withWriterVersion(
                                ParquetProperties.WriterVersion.fromString(
                                        conf.get(
                                                ParquetOutputFormat.WRITER_VERSION,
                                                ParquetProperties.DEFAULT_WRITER_VERSION
                                                        .toString())))
                        .withBloomFilterEnabled(
                                conf.getBoolean(
                                        ParquetOutputFormat.BLOOM_FILTER_ENABLED,
                                        ParquetProperties.DEFAULT_BLOOM_FILTER_ENABLED));
        new ColumnConfigParser()
                .withColumnConfig(
                        ParquetOutputFormat.ENABLE_DICTIONARY,
                        key -> conf.getBoolean(key, false),
                        builder::withDictionaryEncoding)
                .withColumnConfig(
                        ParquetOutputFormat.BLOOM_FILTER_ENABLED,
                        key -> conf.getBoolean(key, false),
                        builder::withBloomFilterEnabled)
                .withColumnConfig(
                        ParquetOutputFormat.BLOOM_FILTER_EXPECTED_NDV,
                        key -> conf.getLong(key, -1L),
                        builder::withBloomFilterNDV)
                .withColumnConfig(
                        ParquetOutputFormat.BLOOM_FILTER_FPP,
                        key -> conf.getDouble(key, ParquetProperties.DEFAULT_BLOOM_FILTER_FPP),
                        builder::withBloomFilterFPP)
                .parseConfig(conf);
        return builder.build();
    }

    public String getCompression(String compression) {
        return conf.get("parquet.compression", compression);
    }

    private class MapShreddingFormatWriter implements FormatWriter {

        private final PositionOutputStream stream;
        private final String compression;
        private final MapShreddingKeyExtractor extractor;
        private FormatWriter delegate;

        private MapShreddingFormatWriter(PositionOutputStream stream, String compression) {
            this.stream = stream;
            this.compression = compression;
            this.extractor = new MapShreddingKeyExtractor(rowType, mapShreddingOptions);
        }

        @Override
        public void addElement(InternalRow row) throws IOException {
            if (delegate != null) {
                delegate.addElement(row);
                return;
            }

            if (!extractor.finished()) {
                extractor.add(row);
            }
            if (extractor.finished()) {
                initializeDelegate();
            }
        }

        @Override
        public boolean reachTargetSize(boolean suggestedCheck, long targetSize) throws IOException {
            if (delegate != null) {
                return delegate.reachTargetSize(suggestedCheck, targetSize);
            }
            return suggestedCheck && stream.getPos() >= targetSize;
        }

        @Override
        public void close() throws IOException {
            if (delegate == null) {
                initializeDelegate();
            }
            delegate.close();
        }

        @Nullable
        @Override
        public Object writerMetadata() {
            return delegate == null ? null : delegate.writerMetadata();
        }

        private void initializeDelegate() throws IOException {
            Map<String, List<String>> dynamicKeys = extractor.finish();
            OutputFile out = new StreamOutputFile(stream);
            delegate = new ParquetBulkWriter(createWriter(out, compression, dynamicKeys));
            for (InternalRow bufferedRow : extractor.bufferedRows()) {
                delegate.addElement(bufferedRow);
            }
        }
    }
}
