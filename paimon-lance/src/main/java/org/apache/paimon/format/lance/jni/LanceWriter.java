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

package org.apache.paimon.format.lance.jni;

import com.lancedb.lance.file.LanceFileWriter;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;

import java.io.IOException;
import java.util.Map;

/** Wrapper for Native Lance Writer. */
public class LanceWriter {

    private final String path;
    private final Map<String, String> storageOptions;
    private LanceFileWriter writer;
    private long bytesWritten = 0;

    public LanceWriter(String path, Map<String, String> storageOptions) {
        this.path = path;
        this.storageOptions = storageOptions;
    }

    public Long getWrittenPosition() {
        return bytesWritten;
    }

    public void writeVsr(VectorSchemaRoot vsr) throws IOException {
        initWriteLazy(vsr.getVector(0).getAllocator());
        this.bytesWritten +=
                vsr.getFieldVectors().stream().mapToLong(FieldVector::getBufferSize).sum();
        this.writer.write(vsr);
    }

    public void close() throws IOException {
        if (writer != null) {
            try {
                this.writer.close();
            } catch (Exception e) {
                throw new IOException(e);
            }
            this.writer = null;
        }
    }

    public String path() {
        return path;
    }

    private void initWriteLazy(BufferAllocator bufferAllocator) throws IOException {
        if (writer == null) {
            writer = LanceFileWriter.open(path, bufferAllocator, null, storageOptions);
        }
    }
}
