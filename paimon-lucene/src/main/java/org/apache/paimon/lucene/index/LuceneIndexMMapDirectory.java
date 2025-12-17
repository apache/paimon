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

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;

/** A wrapper of Lucene MMapDirectory for vector index. */
public class LuceneIndexMMapDirectory implements AutoCloseable {

    private static final int VERSION = 1;

    private final Path path;
    private final MMapDirectory mmapDirectory;

    public LuceneIndexMMapDirectory() throws IOException {
        this.path = Files.createTempDirectory("paimon-lucene-" + UUID.randomUUID());
        this.mmapDirectory = new MMapDirectory(path);
    }

    public MMapDirectory directory() {
        return mmapDirectory;
    }

    public void close() throws Exception {
        mmapDirectory.close();
        if (Files.exists(path)) {
            Files.walk(path)
                    .forEach(
                            p -> {
                                try {
                                    Files.delete(p);
                                } catch (IOException e) {
                                    // Ignore cleanup errors
                                }
                            });
        }
    }

    public void serialize(OutputStream out) throws IOException {
        DataOutputStream dataOutputStream = new DataOutputStream(out);
        dataOutputStream.writeInt(VERSION);
        String[] files = this.directory().listAll();
        dataOutputStream.writeInt(files.length);

        for (String fileName : files) {
            byte[] nameBytes = fileName.getBytes(StandardCharsets.UTF_8);
            dataOutputStream.writeInt(nameBytes.length);
            dataOutputStream.write(nameBytes);
            long fileLength = this.directory().fileLength(fileName);
            dataOutputStream.writeLong(fileLength);

            try (IndexInput input = this.directory().openInput(fileName, IOContext.DEFAULT)) {
                byte[] buffer = new byte[32768];
                long remaining = fileLength;

                while (remaining > 0) {
                    int toRead = (int) Math.min(buffer.length, remaining);
                    input.readBytes(buffer, 0, toRead);
                    dataOutputStream.write(buffer, 0, toRead);
                    remaining -= toRead;
                }
            }
        }
    }

    public static LuceneIndexMMapDirectory deserialize(SeekableInputStream in) throws IOException {
        LuceneIndexMMapDirectory luceneIndexMMapDirectory = new LuceneIndexMMapDirectory();
        try {
            DataInputStream dataInputStream = new DataInputStream(in);
            int version = dataInputStream.readInt();
            if (version != VERSION) {
                throw new IOException("Unsupported version: " + version);
            }
            int numFiles = dataInputStream.readInt();
            byte[] buffer = new byte[32768];
            for (int i = 0; i < numFiles; i++) {
                int nameLength = dataInputStream.readInt();
                byte[] nameBytes = new byte[nameLength];
                dataInputStream.readFully(nameBytes);
                String fileName = new String(nameBytes, StandardCharsets.UTF_8);
                long fileLength = dataInputStream.readLong();
                try (IndexOutput output =
                        luceneIndexMMapDirectory
                                .directory()
                                .createOutput(fileName, IOContext.READONCE)) {
                    long remaining = fileLength;
                    while (remaining > 0) {
                        int toRead = (int) Math.min(buffer.length, remaining);
                        dataInputStream.readFully(buffer, 0, toRead);
                        output.writeBytes(buffer, 0, toRead);
                        remaining -= toRead;
                    }
                }
            }
            return luceneIndexMMapDirectory;
        } catch (Exception e) {
            try {
                luceneIndexMMapDirectory.close();
            } catch (Exception ignored) {
            }
            if (e instanceof IOException) {
                throw (IOException) e;
            } else {
                throw new IOException("Failed to deserialize directory", e);
            }
        }
    }
}
