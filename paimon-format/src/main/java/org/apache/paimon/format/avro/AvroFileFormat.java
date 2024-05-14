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

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.format.FormatReaderFactory;
import org.apache.paimon.format.FormatWriter;
import org.apache.paimon.format.FormatWriterFactory;
import org.apache.paimon.format.SimpleStatsExtractor;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.options.ConfigOption;
import org.apache.paimon.options.ConfigOptions;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.statistics.SimpleColStatsCollector;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.RowType;

import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

import static org.apache.avro.file.DataFileConstants.SNAPPY_CODEC;

/** Avro {@link FileFormat}. */
public class AvroFileFormat extends FileFormat {

    public static final String IDENTIFIER = "avro";

    public static final ConfigOption<String> AVRO_OUTPUT_CODEC =
            ConfigOptions.key("codec")
                    .stringType()
                    .defaultValue(SNAPPY_CODEC)
                    .withDescription("The compression codec for avro");

    private final Options formatOptions;

    public AvroFileFormat(Options formatOptions) {
        super(IDENTIFIER);
        this.formatOptions = formatOptions;
    }

    @Override
    public FormatReaderFactory createReaderFactory(
            RowType projectedRowType, @Nullable List<Predicate> filters) {
        return new AvroBulkFormat(projectedRowType);
    }

    @Override
    public FormatWriterFactory createWriterFactory(RowType type) {
        return new RowAvroWriterFactory(type, formatOptions.get(AVRO_OUTPUT_CODEC));
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
            AvroSchemaConverter.convertToSchema(dataType);
        }
    }

    /** A {@link FormatWriterFactory} to write {@link InternalRow}. */
    private static class RowAvroWriterFactory implements FormatWriterFactory {

        private final AvroWriterFactory<InternalRow> factory;

        private RowAvroWriterFactory(RowType rowType, String codec) {
            this.factory =
                    new AvroWriterFactory<>(
                            out -> {
                                Schema schema = AvroSchemaConverter.convertToSchema(rowType);
                                AvroRowDatumWriter datumWriter = new AvroRowDatumWriter(rowType);
                                DataFileWriter<InternalRow> dataFileWriter =
                                        new DataFileWriter<>(datumWriter);

                                if (codec != null) {
                                    dataFileWriter.setCodec(CodecFactory.fromString(codec));
                                }
                                dataFileWriter.create(schema, out);
                                return dataFileWriter;
                            });
        }

        @Override
        public FormatWriter create(PositionOutputStream out, String compression)
                throws IOException {
            AvroBulkWriter<InternalRow> writer = factory.create(out);
            return new FormatWriter() {

                @Override
                public void addElement(InternalRow element) throws IOException {
                    writer.addElement(element);
                }

                @Override
                public void flush() throws IOException {
                    writer.flush();
                }

                @Override
                public void finish() throws IOException {
                    writer.finish();
                }

                @Override
                public boolean reachTargetSize(boolean suggestedCheck, long targetSize)
                        throws IOException {
                    if (out != null) {
                        return suggestedCheck && out.getPos() >= targetSize;
                    }
                    throw new IOException("Failed to get stream length: no open stream");
                }
            };
        }
    }
}
