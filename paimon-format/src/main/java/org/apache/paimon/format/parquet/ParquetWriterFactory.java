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
import org.apache.paimon.format.FormatWriter;
import org.apache.paimon.format.FormatWriterFactory;
import org.apache.paimon.format.HadoopCompressionType;
import org.apache.paimon.format.parquet.writer.ParquetBuilder;
import org.apache.paimon.format.parquet.writer.ParquetBulkWriter;
import org.apache.paimon.format.parquet.writer.StreamOutputFile;
import org.apache.paimon.fs.PositionOutputStream;

import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.io.OutputFile;

import java.io.IOException;

/** A factory that creates a Parquet {@link FormatWriter}. */
public class ParquetWriterFactory implements FormatWriterFactory {

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
        final ParquetWriter<InternalRow> writer = writerBuilder.createWriter(out, compression);
        return new ParquetBulkWriter(writer);
    }
}
