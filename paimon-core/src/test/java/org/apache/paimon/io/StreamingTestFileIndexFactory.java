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

package org.apache.paimon.io;

import org.apache.paimon.fileindex.FileIndexReader;
import org.apache.paimon.fileindex.FileIndexWriter;
import org.apache.paimon.fileindex.FileIndexer;
import org.apache.paimon.fileindex.FileIndexerFactory;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.options.Options;
import org.apache.paimon.types.DataType;

import java.io.IOException;
import java.io.OutputStream;

/** Test-only index whose streaming payload can exceed the byte-array limit. */
public class StreamingTestFileIndexFactory implements FileIndexerFactory {

    private static final byte MARKER = 7;

    @Override
    public String identifier() {
        return "stream-test";
    }

    @Override
    public FileIndexer create(DataType type, Options options) {
        boolean large = options.getBoolean("large", false);
        boolean fail = options.getBoolean("fail", false);
        return new FileIndexer() {
            @Override
            public FileIndexWriter createWriter() {
                return new FileIndexWriter() {
                    @Override
                    public void write(Object key) {}

                    @Override
                    public void writeTo(OutputStream output) throws IOException {
                        if (large) {
                            byte[] block = new byte[1024 * 1024];
                            for (int i = 0; i < 2049; i++) {
                                output.write(block);
                            }
                        }
                        output.write(MARKER);
                        if (fail) {
                            throw new IOException("Test index write failure");
                        }
                    }
                };
            }

            @Override
            public FileIndexReader createReader(
                    SeekableInputStream inputStream, int start, int length) {
                return createReader(inputStream, (long) start, (long) length);
            }

            @Override
            public FileIndexReader createReader(
                    SeekableInputStream inputStream, long start, long length) {
                try {
                    inputStream.seek(start + length - 1);
                    if (inputStream.read() != MARKER) {
                        throw new IOException("Missing streaming index marker");
                    }
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                return new FileIndexReader() {};
            }
        };
    }
}
