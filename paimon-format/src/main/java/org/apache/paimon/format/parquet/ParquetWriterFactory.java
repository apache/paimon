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

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.shredding.ShreddingWritePlan;
import org.apache.paimon.format.FormatMetadataUtils;
import org.apache.paimon.format.FormatWriter;
import org.apache.paimon.format.FormatWriterFactory;
import org.apache.paimon.format.HadoopCompressionType;
import org.apache.paimon.format.SupportsWriterMetadata;
import org.apache.paimon.format.parquet.writer.MetadataParquetBuilder;
import org.apache.paimon.format.parquet.writer.ParquetBuilder;
import org.apache.paimon.format.parquet.writer.ParquetBulkWriter;
import org.apache.paimon.format.parquet.writer.ParquetMetadataBulkWriter;
import org.apache.paimon.format.parquet.writer.RowDataParquetBuilder;
import org.apache.paimon.format.parquet.writer.StreamOutputFile;
import org.apache.paimon.format.shredding.SupportsShreddingWritePlan;
import org.apache.paimon.fs.PositionOutputStream;

import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.io.OutputFile;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/** A factory that creates a Parquet {@link FormatWriter}. */
public class ParquetWriterFactory implements FormatWriterFactory, SupportsShreddingWritePlan {

    /** The builder to construct the ParquetWriter. */
    private final ParquetBuilder<InternalRow> writerBuilder;

    /**
     * Creates a new ParquetWriterFactory using the given builder to assemble the ParquetWriter.
     *
     * @param writerBuilder The builder to construct the ParquetWriter.
     */
    public ParquetWriterFactory(ParquetBuilder<InternalRow> writerBuilder) {
        this.writerBuilder = writerBuilder;
    }

    @Override
    public FormatWriter create(PositionOutputStream stream, String compression) throws IOException {
        final OutputFile out = new StreamOutputFile(stream);
        if (HadoopCompressionType.NONE.value().equals(compression)) {
            compression = null;
        }

        if (writerBuilder instanceof MetadataParquetBuilder) {
            return createMetadataWriter(
                    (MetadataParquetBuilder<InternalRow>) writerBuilder, out, compression);
        }

        final ParquetWriter<InternalRow> writer = writerBuilder.createWriter(out, compression);
        return new ParquetBulkWriter(writer);
    }

    @Override
    public FormatWriter createWithShreddingWritePlan(
            PositionOutputStream stream, String compression, ShreddingWritePlan writePlan)
            throws IOException {
        final OutputFile out = new StreamOutputFile(stream);
        if (HadoopCompressionType.NONE.value().equals(compression)) {
            compression = null;
        }

        RowDataParquetBuilder newBuilder =
                ((RowDataParquetBuilder) writerBuilder).withRowType(writePlan.physicalRowType());
        return createMetadataWriter(newBuilder, out, compression);
    }

    @Override
    public void commitShreddingMetadata(
            FormatWriter writer, ShreddingWritePlan writePlan, String compression) {
        Map<String, Map<String, String>> fieldMetadata = writePlan.fieldMetadata(compression);
        if (fieldMetadata.isEmpty()) {
            return;
        }

        Map<String, byte[]> metadata = new HashMap<>();
        metadata.put(
                FormatMetadataUtils.ARROW_SCHEMA_METADATA_KEY,
                FormatMetadataUtils.buildArrowSchemaMetadata(
                        writePlan.physicalRowType(),
                        fieldMetadata,
                        FormatMetadataUtils.PARQUET_FIELD_ID_KEY));
        ((SupportsWriterMetadata) writer).addMetadata(metadata);
    }

    private FormatWriter createMetadataWriter(
            MetadataParquetBuilder<InternalRow> builder, OutputFile out, String compression)
            throws IOException {
        // Keep this exact map instance shared by ParquetBulkWriter and WriteSupport. The writer
        // collects metadata before close, and WriteSupport reads it when finalizing the footer.
        Map<String, byte[]> metadata = new HashMap<>();
        final ParquetWriter<InternalRow> writer =
                builder.createWriter(out, compression, () -> metadata);
        return new ParquetMetadataBulkWriter(writer, metadata);
    }
}
