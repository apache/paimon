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

import org.apache.parquet.hadoop.ParquetWriter;

import java.io.IOException;

import static org.apache.paimon.utils.Preconditions.checkNotNull;

/** A simple {@link FormatWriter} implementation that wraps a {@link ParquetWriter}. */
public class ParquetBulkWriter implements FormatWriter {

    /** The ParquetWriter to write to. */
    private final ParquetWriter<InternalRow> parquetWriter;

    /**
     * Creates a new ParquetBulkWriter wrapping the given ParquetWriter.
     *
     * @param parquetWriter The ParquetWriter to write to.
     */
    public ParquetBulkWriter(ParquetWriter<InternalRow> parquetWriter) {
        this.parquetWriter = checkNotNull(parquetWriter, "parquetWriter");
    }

    @Override
    public void addElement(InternalRow datum) throws IOException {
        parquetWriter.write(datum);
    }

    @Override
    public void close() throws IOException {
        parquetWriter.close();
    }

    @Override
    public boolean reachTargetSize(boolean suggestedCheck, long targetSize) throws IOException {
        return suggestedCheck && parquetWriter.getDataSize() >= targetSize;
    }
}
