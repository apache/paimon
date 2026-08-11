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

package org.apache.avro.file;

import org.apache.avro.Schema;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;

import java.io.IOException;
import java.io.InputStream;

/** Package bridge exposing Avro's compressed blocks without reflection. */
public final class RawBlockReader extends DataFileStream<Void> {

    public RawBlockReader(InputStream input) throws IOException {
        super(input, new NoOpDatumReader<Void>());
    }

    public boolean hasNextRawBlock() {
        return super.hasNextBlock();
    }

    public RawBlock nextRawBlock(RawBlock reuse) throws IOException {
        DataBlock raw = super.nextRawBlock(reuse == null ? null : reuse.dataBlock());
        return reuse == null
                ? new RawBlock(raw, resolveCodec(), getSchema())
                : reuse.replace(raw, resolveCodec(), getSchema());
    }

    private static final class NoOpDatumReader<D> implements DatumReader<D> {

        @Override
        public void setSchema(Schema schema) {}

        @Override
        public D read(D reuse, Decoder decoder) {
            throw new UnsupportedOperationException();
        }
    }
}
