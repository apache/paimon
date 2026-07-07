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
import org.apache.paimon.format.HadoopCompressionType;
import org.apache.paimon.format.parquet.ColumnConfigParser;
import org.apache.paimon.options.Options;
import org.apache.paimon.types.RowType;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.hadoop.ParquetOutputFormat;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.OutputFile;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

/** A {@link ParquetBuilder} for {@link InternalRow}. */
public class RowDataParquetBuilder implements MetadataParquetBuilder<InternalRow> {

    private final RowType rowType;
    private final Configuration conf;

    public RowDataParquetBuilder(RowType rowType, Options options) {
        this.rowType = rowType;
        this.conf = new Configuration(false);
        options.toMap().forEach(conf::set);
    }

    private RowDataParquetBuilder(RowType rowType, Configuration conf) {
        this.rowType = rowType;
        this.conf = conf;
    }

    public RowDataParquetBuilder withRowType(RowType rowType) {
        return new RowDataParquetBuilder(rowType, conf);
    }

    @Override
    public ParquetWriter<InternalRow> createWriter(OutputFile out, String compression)
            throws IOException {
        return createWriter(out, compression, HashMap::new);
    }

    @Override
    public ParquetWriter<InternalRow> createWriter(
            OutputFile out, String compression, Supplier<Map<String, byte[]>> metadataSupplier)
            throws IOException {
        ParquetRowDataBuilder builder =
                new ParquetRowDataBuilder(out, rowType)
                        .withMetadataSupplier(metadataSupplier)
                        .withConf(conf)
                        .withCompressionCodec(getCompressionCodec(getCompression(compression)))
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
                                        ParquetProperties.DEFAULT_BLOOM_FILTER_ENABLED))
                        .withMinRowCountForPageSizeCheck(
                                conf.getInt(
                                        ParquetOutputFormat.MIN_ROW_COUNT_FOR_PAGE_SIZE_CHECK,
                                        ParquetProperties.DEFAULT_MINIMUM_RECORD_COUNT_FOR_CHECK))
                        .withMaxRowCountForPageSizeCheck(
                                conf.getInt(
                                        ParquetOutputFormat.MAX_ROW_COUNT_FOR_PAGE_SIZE_CHECK,
                                        ParquetProperties.DEFAULT_MAXIMUM_RECORD_COUNT_FOR_CHECK))
                        .withStatisticsTruncateLength(
                                conf.getInt(
                                        ParquetOutputFormat.STATISTICS_TRUNCATE_LENGTH,
                                        ParquetProperties.DEFAULT_STATISTICS_TRUNCATE_LENGTH))
                        .withColumnIndexTruncateLength(
                                conf.getInt(
                                        ParquetOutputFormat.COLUMN_INDEX_TRUNCATE_LENGTH,
                                        ParquetProperties.DEFAULT_COLUMN_INDEX_TRUNCATE_LENGTH));
        new ColumnConfigParser()
                .withColumnConfig(
                        ParquetOutputFormat.COMPRESSION,
                        key -> getCompressionCodec(conf.get(key)),
                        builder::withCompressionCodec)
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

    private CompressionCodecName getCompressionCodec(String compression) {
        if (HadoopCompressionType.NONE.value().equalsIgnoreCase(compression)) {
            return CompressionCodecName.UNCOMPRESSED;
        }
        return CompressionCodecName.fromConf(compression);
    }
}
