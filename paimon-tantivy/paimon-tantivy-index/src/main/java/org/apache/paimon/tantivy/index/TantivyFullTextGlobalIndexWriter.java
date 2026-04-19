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

package org.apache.paimon.tantivy.index;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.globalindex.GlobalIndexSingletonWriter;
import org.apache.paimon.globalindex.ResultEntry;
import org.apache.paimon.globalindex.io.GlobalIndexFileWriter;
import org.apache.paimon.tantivy.TantivyIndexWriter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Full-text global index writer using Tantivy.
 *
 * <p>Text data is written to a local Tantivy index via JNI. On {@link #finish()}, the index
 * directory is packed into a single file and written to the global index file system.
 */
public class TantivyFullTextGlobalIndexWriter implements GlobalIndexSingletonWriter, Closeable {

    private static final String FILE_NAME_PREFIX = "tantivy";
    private static final Logger LOG =
            LoggerFactory.getLogger(TantivyFullTextGlobalIndexWriter.class);

    private final GlobalIndexFileWriter fileWriter;
    private File tempIndexDir;
    private TantivyIndexWriter writer;
    private long rowId;
    private boolean closed;

    public TantivyFullTextGlobalIndexWriter(GlobalIndexFileWriter fileWriter) {
        this.fileWriter = fileWriter;
        this.rowId = 0;
        this.closed = false;

        try {
            this.tempIndexDir = Files.createTempDirectory("tantivy-index-").toFile();
            this.tempIndexDir.deleteOnExit();
            this.writer = new TantivyIndexWriter(tempIndexDir.getAbsolutePath());
        } catch (IOException e) {
            throw new RuntimeException("Failed to create temp index directory", e);
        }
    }

    @Override
    public void write(Object fieldData) {
        if (fieldData == null) {
            rowId++;
            return;
        }

        String text;
        if (fieldData instanceof BinaryString) {
            text = fieldData.toString();
        } else if (fieldData instanceof String) {
            text = (String) fieldData;
        } else {
            throw new IllegalArgumentException(
                    "Unsupported field type: " + fieldData.getClass().getName());
        }

        writer.addDocument(rowId, text);
        rowId++;
    }

    @Override
    public List<ResultEntry> finish() {
        try {
            if (rowId == 0) {
                return Collections.emptyList();
            }

            writer.commit();
            writer.close();
            writer = null;

            return Collections.singletonList(packIndex());
        } catch (IOException e) {
            throw new RuntimeException("Failed to write Tantivy full-text global index", e);
        } finally {
            if (writer != null) {
                writer.close();
                writer = null;
            }
            closed = true;
            deleteTempDir();
        }
    }

    private ResultEntry packIndex() throws IOException {
        LOG.info("Packing Tantivy index: {} documents", rowId);

        String fileName = fileWriter.newFileName(FILE_NAME_PREFIX);
        try (PositionOutputStream out = fileWriter.newOutputStream(fileName)) {
            // Write all files in the index directory as a simple archive:
            // For each file: [nameLen(4)] [name(utf8)] [dataLen(8)] [data]
            File[] allFiles = tempIndexDir.listFiles();
            if (allFiles == null) {
                throw new IOException("Index directory is empty");
            }

            // Filter to regular files only before writing count
            List<File> indexFiles = new ArrayList<>();
            for (File file : allFiles) {
                if (file.isFile() && !file.getName().endsWith(".store")) {
                    indexFiles.add(file);
                }
            }

            // Write file count
            writeInt(out, indexFiles.size());

            for (File file : indexFiles) {
                byte[] nameBytes = file.getName().getBytes(StandardCharsets.UTF_8);
                long fileLen = file.length();

                writeInt(out, nameBytes.length);
                out.write(nameBytes);
                writeLong(out, fileLen);

                try (FileInputStream fis = new FileInputStream(file)) {
                    byte[] buf = new byte[8192];
                    int read;
                    while ((read = fis.read(buf)) != -1) {
                        out.write(buf, 0, read);
                    }
                }
            }
            out.flush();
        }

        LOG.info("Tantivy index packed: {} documents", rowId);
        return new ResultEntry(fileName, rowId, null);
    }

    private static void writeInt(PositionOutputStream out, int value) throws IOException {
        out.write((value >>> 24) & 0xFF);
        out.write((value >>> 16) & 0xFF);
        out.write((value >>> 8) & 0xFF);
        out.write(value & 0xFF);
    }

    private static void writeLong(PositionOutputStream out, long value) throws IOException {
        writeInt(out, (int) (value >>> 32));
        writeInt(out, (int) value);
    }

    private void deleteTempDir() {
        if (tempIndexDir != null) {
            try {
                Files.walkFileTree(
                        tempIndexDir.toPath(),
                        new SimpleFileVisitor<Path>() {
                            @Override
                            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
                                    throws IOException {
                                Files.delete(file);
                                return FileVisitResult.CONTINUE;
                            }

                            @Override
                            public FileVisitResult postVisitDirectory(Path dir, IOException exc)
                                    throws IOException {
                                Files.delete(dir);
                                return FileVisitResult.CONTINUE;
                            }
                        });
            } catch (IOException ignored) {
            }
            tempIndexDir = null;
        }
    }

    @Override
    public void close() {
        if (!closed) {
            closed = true;
            if (writer != null) {
                writer.close();
                writer = null;
            }
            deleteTempDir();
        }
    }
}
