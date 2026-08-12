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
import org.apache.paimon.format.FormatWriter;
import org.apache.paimon.fs.PositionOutputStream;

import org.apache.avro.file.DataFileWriter;

import java.io.IOException;
import java.nio.ByteBuffer;

/** Avro writer which accepts normal rows, encoded records and compressed blocks. */
public final class AvroBlockWriter implements FormatWriter {

    private final DataFileWriter<InternalRow> writer;
    private final PositionOutputStream out;

    public AvroBlockWriter(DataFileWriter<InternalRow> writer, PositionOutputStream out) {
        this.writer = writer;
        this.out = out;
    }

    @Override
    public void addElement(InternalRow element) throws IOException {
        writer.append(element);
    }

    public void addEncoded(ByteBuffer record) throws IOException {
        writer.appendEncoded(record);
    }

    public void addEncodedBlock(AvroRawBlock block) throws IOException {
        writer.appendAllFrom(block.asStream(), false);
    }

    @Override
    public boolean reachTargetSize(boolean suggestedCheck, long targetSize) throws IOException {
        return suggestedCheck && out.getPos() >= targetSize;
    }

    @Override
    public void close() throws IOException {
        writer.close();
    }
}
