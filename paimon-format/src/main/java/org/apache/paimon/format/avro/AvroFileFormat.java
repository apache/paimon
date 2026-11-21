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

package org.apache.paimon.format.avro;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.format.FileFormatFactory.FormatContext;
import org.apache.paimon.format.FormatReaderFactory;
import org.apache.paimon.format.FormatWriter;
import org.apache.paimon.format.FormatWriterFactory;
import org.apache.paimon.format.SimpleStatsExtractor;
import org.apache.paimon.fs.CloseShieldOutputStream;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.options.ConfigOption;
import org.apache.paimon.options.ConfigOptions;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.statistics.SimpleColStatsCollector;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.RowType;

import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.EncoderFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.avro.file.DataFileConstants.SNAPPY_CODEC;

/** Avro {@link FileFormat}. */
public class AvroFileFormat extends FileFormat {

    public static final String IDENTIFIER = "avro";

    private static final ConfigOption<String> AVRO_OUTPUT_CODEC =
            ConfigOptions.key("avro.codec")
                    .stringType()
                    .defaultValue(SNAPPY_CODEC)
                    .withDescription("The compression codec for avro");

    private static final ConfigOption<Map<String, String>> AVRO_ROW_NAME_MAPPING =
            ConfigOptions.key("avro.row-name-mapping").mapType().defaultValue(new HashMap<>());

    private final Options options;
    private final int zstdLevel;
    /** Bounds enforced by {@code DataFileWriter#setSyncInterval}. */
    private static final long MIN_SYNC_INTERVAL = 32;

    private static final long MAX_SYNC_INTERVAL = 1 << 30;

    @Nullable private final MemorySize blockSize;

    public AvroFileFormat(FormatContext context) {
        super(IDENTIFIER);

        this.options = getIdentifierPrefixOptions(context.options());
        this.zstdLevel = context.zstdLevel();
        this.blockSize = context.blockSize();
    }

    @Override
    public FormatReaderFactory createReaderFactory(
            RowType dataSchemaRowType,
            RowType projectedRowType,
            @Nullable List<Predicate> filters) {
        return new AvroBulkFormat(projectedRowType);
    }

    @Override
    public FormatWriterFactory createWriterFactory(RowType type) {
        return new RowAvroWriterFactory(type);
    }

    public AvroBlockWriter createBlockWriter(
            PositionOutputStream out, RowType rowType, String compression) throws IOException {
        // Retain Avro's direct encoder for pre-encoded manifest records. The buffered encoder
        // copies array-backed ByteBuffers into a temporary byte array for each appendEncoded call.
        return createBlockWriter(out, rowType, compression, false);
    }

    private AvroBlockWriter createBlockWriter(
            PositionOutputStream out,
            RowType rowType,
            String compression,
            boolean useBufferedEncoder)
            throws IOException {
        Schema schema =
                AvroSchemaConverter.convertToSchema(rowType, options.get(AVRO_ROW_NAME_MAPPING));
        AvroRowDatumWriter datumWriter = new AvroRowDatumWriter(rowType);
        DataFileWriter<InternalRow> writer = new DataFileWriter<>(datumWriter);
        if (useBufferedEncoder) {
            // Batch data-file field encodings before writing them to the Avro block buffer.
            writer.setEncoder(
                    outputStream -> EncoderFactory.get().binaryEncoder(outputStream, null));
        }
        writer.setCodec(createCodecFactory(compression));
        if (blockSize != null) {
            writer.setSyncInterval(avroSyncInterval(blockSize));
        }
        writer.setFlushOnEveryBlock(false);
        writer.create(schema, new CloseShieldOutputStream(out));
        return new AvroBlockWriter(writer, out, schema);
    }

    @Override
    public Optional<SimpleStatsExtractor> createStatsExtractor(
            RowType type, SimpleColStatsCollector.Factory[] statsCollectors) {
        return Optional.of(new AvroSimpleStatsExtractor(type, statsCollectors));
    }

    @Override
    public void validateDataFields(RowType rowType) {
        List<DataType> fieldTypes = rowType.getFieldTypes();
        for (DataType dataType : fieldTypes) {
            AvroSchemaConverter.convertToSchema(dataType, new HashMap<>());
        }
    }

    /**
     * Avro only accepts a sync interval between 32 bytes and 1 GiB; check it here so a bad {@code
     * file.block-size} fails with the option name instead of inside the writer on an executor.
     */
    static int avroSyncInterval(MemorySize blockSize) {
        long bytes = blockSize.getBytes();
        if (bytes < MIN_SYNC_INTERVAL || bytes > MAX_SYNC_INTERVAL) {
            throw new IllegalArgumentException(
                    String.format(
                            "%s for avro must be between 32 bytes and 1 gb, but was %s bytes.",
                            CoreOptions.FILE_BLOCK_SIZE.key(), bytes));
        }
        return (int) bytes;
    }

    private CodecFactory createCodecFactory(String compression) {
        if (options.contains(AVRO_OUTPUT_CODEC)) {
            return CodecFactory.fromString(options.get(AVRO_OUTPUT_CODEC));
        }

        if (compression.equalsIgnoreCase("zstd")) {
            return CodecFactory.zstandardCodec(zstdLevel);
        }
        return CodecFactory.fromString(compression);
    }

    /** A {@link FormatWriterFactory} to write {@link InternalRow}. */
    private class RowAvroWriterFactory implements FormatWriterFactory {

        private final RowType rowType;

        private RowAvroWriterFactory(RowType rowType) {
            this.rowType = rowType;
        }

        @Override
        public FormatWriter create(PositionOutputStream out, String compression)
                throws IOException {
            return createBlockWriter(out, rowType, compression, true);
        }
    }
}
