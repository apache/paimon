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

package org.apache.paimon.format.parquet;

import org.apache.paimon.options.Options;
import org.apache.paimon.types.RowType;

import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.hadoop.ParquetOutputFormat;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.MessageType;

import javax.annotation.Nullable;

import java.util.Map;

/**
 * Checks whether the writer configuration and the input Parquet files of one compaction batch are
 * compatible with the RowGroup copy fast path (see {@link ParquetRowGroupCopier}). One instance
 * serves one batch: it keeps the reference codec so that all checked files share the same codec.
 */
public class ParquetRowGroupCopyChecker {

    /** Reasons why the RowGroup copy fast path is not applicable. */
    public enum Incompatibility {
        ENCRYPTION,
        WRITER_V2_ENCODING,
        WRITER_V2_CONFIGURED,
        BLOOM_FILTER,
        SCHEMA_MISMATCH,
        CODEC_MISMATCH
    }

    private final MessageType expectedSchema;
    private final String expectedCodec;
    private final Options options;

    @Nullable private CompressionCodecName referenceCodec;

    public ParquetRowGroupCopyChecker(RowType writeType, String fileCompression, Options options) {
        this.expectedSchema = ParquetSchemaConverter.convertToParquetMessageType(writeType);
        this.expectedCodec = normalizeCompression(fileCompression);
        this.options = options;
    }

    /** Checks table-level writer configuration (bloom filter and Parquet writer v2). */
    @Nullable
    public Incompatibility checkConfiguration() {
        if (isBloomFilterConfigured(options)) {
            return Incompatibility.BLOOM_FILTER;
        }
        if (isParquetWriterV2Configured(options)) {
            return Incompatibility.WRITER_V2_CONFIGURED;
        }
        return null;
    }

    /**
     * Checks a single file footer against the expected schema and codec, and enforces codec
     * consistency across all files checked with this instance.
     */
    @Nullable
    public Incompatibility checkFooter(ParquetMetadata footer) {
        FileMetaData fileMetaData = footer.getFileMetaData();
        if (fileMetaData.getEncryptionType() != FileMetaData.EncryptionType.UNENCRYPTED) {
            return Incompatibility.ENCRYPTION;
        }
        if (hasParquetV2Encoding(footer)) {
            return Incompatibility.WRITER_V2_ENCODING;
        }
        if (!fileMetaData.getSchema().equals(expectedSchema)) {
            return Incompatibility.SCHEMA_MISMATCH;
        }
        CompressionCodecName codec = null;
        for (BlockMetaData block : footer.getBlocks()) {
            for (ColumnChunkMetaData column : block.getColumns()) {
                CompressionCodecName columnCodec = column.getCodec();
                if (codec == null) {
                    codec = columnCodec;
                } else if (!codec.equals(columnCodec)) {
                    return Incompatibility.CODEC_MISMATCH;
                }
                if (!codecMatches(columnCodec, expectedCodec)) {
                    return Incompatibility.CODEC_MISMATCH;
                }
            }
        }
        if (codec == null) {
            codec = CompressionCodecName.UNCOMPRESSED;
        }
        if (referenceCodec == null) {
            referenceCodec = codec;
        } else if (!referenceCodec.equals(codec)) {
            return Incompatibility.CODEC_MISMATCH;
        }
        return null;
    }

    /** The normalized codec expected by the table configuration, for logging. */
    public String expectedCodec() {
        return expectedCodec;
    }

    private static boolean hasParquetV2Encoding(ParquetMetadata footer) {
        for (BlockMetaData block : footer.getBlocks()) {
            for (ColumnChunkMetaData column : block.getColumns()) {
                if (column.getEncodings().contains(Encoding.BYTE_STREAM_SPLIT)) {
                    return true;
                }
            }
        }
        return false;
    }

    private static boolean isParquetWriterV2Configured(Options options) {
        String writerVersion = options.get(ParquetOutputFormat.WRITER_VERSION);
        if (writerVersion == null || writerVersion.isEmpty()) {
            return false;
        }
        try {
            return ParquetProperties.WriterVersion.fromString(writerVersion)
                    == ParquetProperties.WriterVersion.PARQUET_2_0;
        } catch (IllegalArgumentException ignored) {
            return "v2".equalsIgnoreCase(writerVersion)
                    || "PARQUET_2_0".equalsIgnoreCase(writerVersion);
        }
    }

    private static boolean codecMatches(CompressionCodecName codec, String expectedCodec) {
        if ("none".equalsIgnoreCase(expectedCodec)
                || "uncompressed".equalsIgnoreCase(expectedCodec)) {
            return codec == CompressionCodecName.UNCOMPRESSED;
        }
        return codec.name().equalsIgnoreCase(expectedCodec)
                || codec.toString().equalsIgnoreCase(expectedCodec);
    }

    private static boolean isBloomFilterConfigured(Options options) {
        Map<String, String> config = options.toMap();
        for (Map.Entry<String, String> entry : config.entrySet()) {
            String key = entry.getKey();
            if (ParquetOutputFormat.BLOOM_FILTER_ENABLED.equals(key)
                    && Boolean.parseBoolean(entry.getValue())) {
                return true;
            }
            if (key.startsWith(ParquetOutputFormat.BLOOM_FILTER_ENABLED + "#")
                    && Boolean.parseBoolean(entry.getValue())) {
                return true;
            }
            if ("parquet.bloom.filter.columns".equals(key) && !entry.getValue().isEmpty()) {
                return true;
            }
        }
        return false;
    }

    private static String normalizeCompression(String compression) {
        if (compression == null || compression.isEmpty()) {
            return CompressionCodecName.UNCOMPRESSED.name().toLowerCase();
        }
        if ("none".equalsIgnoreCase(compression) || "uncompressed".equalsIgnoreCase(compression)) {
            return CompressionCodecName.UNCOMPRESSED.name().toLowerCase();
        }
        return compression.toLowerCase();
    }
}
