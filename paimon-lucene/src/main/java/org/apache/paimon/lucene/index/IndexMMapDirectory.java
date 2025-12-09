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

package org.apache.paimon.lucene.index;

import org.apache.paimon.fs.SeekableInputStream;

import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.MMapDirectory;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.UUID;

/** A wrapper of MMapDirectory for vector index. */
public class IndexMMapDirectory implements AutoCloseable {

    private final Path path;
    private final MMapDirectory mmapDirectory;

    public IndexMMapDirectory() throws IOException {
        this.path = java.nio.file.Files.createTempDirectory("paimon-lucene-" + UUID.randomUUID());
        this.mmapDirectory = new MMapDirectory(path);
    }

    public MMapDirectory directory() {
        return mmapDirectory;
    }

    public void close() throws Exception {
        mmapDirectory.close();
        if (java.nio.file.Files.exists(path)) {
            java.nio.file.Files.walk(path)
                    .sorted(java.util.Comparator.reverseOrder())
                    .forEach(
                            p -> {
                                try {
                                    java.nio.file.Files.delete(p);
                                } catch (IOException e) {
                                    // Ignore cleanup errors
                                }
                            });
        }
    }

    public void serialize(OutputStream out) throws IOException {
        String[] files = this.directory().listAll();
        out.write(intToBytes(files.length));

        for (String fileName : files) {
            byte[] nameBytes = fileName.getBytes(StandardCharsets.UTF_8);
            out.write(intToBytes(nameBytes.length));
            out.write(nameBytes);
            long fileLength = this.directory().fileLength(fileName);
            out.write(ByteBuffer.allocate(8).putLong(fileLength).array());

            try (IndexInput input = this.directory().openInput(fileName, IOContext.DEFAULT)) {
                byte[] buffer = new byte[32768];
                long remaining = fileLength;

                while (remaining > 0) {
                    int toRead = (int) Math.min(buffer.length, remaining);
                    input.readBytes(buffer, 0, toRead);
                    out.write(buffer, 0, toRead);
                    remaining -= toRead;
                }
            }
        }
    }

    public static IndexMMapDirectory deserialize(SeekableInputStream in) throws IOException {
        IndexMMapDirectory indexMMapDirectory = new IndexMMapDirectory();
        try {
            int numFiles = readInt(in);
            byte[] buffer = new byte[32768];
            for (int i = 0; i < numFiles; i++) {
                int nameLength = readInt(in);
                byte[] nameBytes = new byte[nameLength];
                readFully(in, nameBytes);
                String fileName = new String(nameBytes, StandardCharsets.UTF_8);
                long fileLength = readLong(in);
                try (IndexOutput output =
                        indexMMapDirectory.directory().createOutput(fileName, IOContext.DEFAULT)) {
                    long remaining = fileLength;
                    while (remaining > 0) {
                        int toRead = (int) Math.min(buffer.length, remaining);
                        readFully(in, buffer, 0, toRead);
                        output.writeBytes(buffer, 0, toRead);
                        remaining -= toRead;
                    }
                }
            }
            return indexMMapDirectory;
        } catch (Exception e) {
            try {
                indexMMapDirectory.close();
            } catch (Exception closeEx) {
            }
            if (e instanceof IOException) {
                throw (IOException) e;
            } else {
                throw new IOException("Failed to deserialize directory", e);
            }
        }
    }

    private static int readInt(SeekableInputStream in) throws IOException {
        byte[] bytes = new byte[4];
        readFully(in, bytes);
        return ByteBuffer.wrap(bytes).getInt();
    }

    private static long readLong(SeekableInputStream in) throws IOException {
        byte[] bytes = new byte[8];
        readFully(in, bytes);
        return ByteBuffer.wrap(bytes).getLong();
    }

    private static void readFully(SeekableInputStream in, byte[] buffer) throws IOException {
        readFully(in, buffer, 0, buffer.length);
    }

    private static void readFully(SeekableInputStream in, byte[] buffer, int offset, int length)
            throws IOException {
        int totalRead = 0;
        while (totalRead < length) {
            int read = in.read(buffer, offset + totalRead, length - totalRead);
            if (read == -1) {
                throw new IOException("Unexpected end of stream");
            }
            totalRead += read;
        }
    }

    private byte[] intToBytes(int value) {
        return ByteBuffer.allocate(4).putInt(value).array();
    }
}
