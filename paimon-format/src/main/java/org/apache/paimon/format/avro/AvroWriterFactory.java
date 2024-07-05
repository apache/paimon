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

import org.apache.paimon.fs.PositionOutputStream;

import org.apache.avro.file.DataFileWriter;

import java.io.IOException;

/**
 * A factory that creates an {@link AvroBulkWriter}.
 *
 * @param <T> The type of record to write.
 */
public class AvroWriterFactory<T> {

    /** The builder to construct the Avro {@link DataFileWriter}. */
    private final AvroBuilder<T> avroBuilder;

    /** Creates a new AvroWriterFactory using the given builder to assemble the ParquetWriter. */
    public AvroWriterFactory(AvroBuilder<T> avroBuilder) {
        this.avroBuilder = avroBuilder;
    }

    public AvroBulkWriter<T> create(PositionOutputStream out, String compression)
            throws IOException {
        return new AvroBulkWriter<>(
                avroBuilder.createWriter(new CloseShieldOutputStream(out), compression));
    }
}
