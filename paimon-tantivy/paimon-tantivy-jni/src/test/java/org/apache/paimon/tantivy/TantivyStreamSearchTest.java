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

package org.apache.paimon.tantivy;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/** Test for stream-based TantivySearcher via JNI Directory callback. */
class TantivyStreamSearchTest {

    @Test
    void testStreamBasedSearch(@TempDir Path tempDir) throws IOException {
        // 1. Create index on disk
        String indexPath = tempDir.resolve("test_index").toString();
        try (TantivyIndexWriter writer = new TantivyIndexWriter(indexPath)) {
            writer.addDocument(0L, "Apache Paimon is a streaming data lake platform");
            writer.addDocument(1L, "Tantivy is a full-text search engine written in Rust");
            writer.addDocument(2L, "Paimon supports real-time data ingestion");
            writer.commit();
        }

        // 2. Pack the index into an archive (same format as TantivyFullTextGlobalIndexWriter)
        File indexDir = new File(indexPath);
        File[] indexFiles = indexDir.listFiles();
        assertNotNull(indexFiles);

        ByteArrayOutputStream archiveOut = new ByteArrayOutputStream();
        List<String> fileNames = new ArrayList<>();
        List<Long> fileOffsets = new ArrayList<>();
        List<Long> fileLengths = new ArrayList<>();

        // Write file count
        writeInt(archiveOut, indexFiles.length);

        // First pass: write archive and track offsets
        // We need to compute offsets relative to the start of the archive
        // Build the archive in memory
        ByteArrayOutputStream headerOut = new ByteArrayOutputStream();
        ByteArrayOutputStream dataOut = new ByteArrayOutputStream();

        // Compute header size first
        int headerSize = 4; // file count
        for (File file : indexFiles) {
            if (!file.isFile()) continue;
            byte[] nameBytes = file.getName().getBytes("UTF-8");
            headerSize += 4 + nameBytes.length + 8; // nameLen + name + dataLen
        }

        long dataOffset = headerSize;
        ByteArrayOutputStream fullArchive = new ByteArrayOutputStream();
        writeInt(fullArchive, indexFiles.length);

        for (File file : indexFiles) {
            if (!file.isFile()) continue;
            byte[] nameBytes = file.getName().getBytes("UTF-8");
            long fileLen = file.length();

            writeInt(fullArchive, nameBytes.length);
            fullArchive.write(nameBytes);
            writeLong(fullArchive, fileLen);

            fileNames.add(file.getName());
            fileOffsets.add((long) fullArchive.size());
            fileLengths.add(fileLen);

            try (FileInputStream fis = new FileInputStream(file)) {
                byte[] buf = new byte[8192];
                int read;
                while ((read = fis.read(buf)) != -1) {
                    fullArchive.write(buf, 0, read);
                }
            }
        }

        byte[] archiveBytes = fullArchive.toByteArray();

        // 3. Write archive to a file and open as RandomAccessFile-backed stream
        File archiveFile = tempDir.resolve("archive.bin").toFile();
        try (java.io.FileOutputStream fos = new java.io.FileOutputStream(archiveFile)) {
            fos.write(archiveBytes);
        }

        // 4. Create a StreamFileInput backed by RandomAccessFile
        RandomAccessFile raf = new RandomAccessFile(archiveFile, "r");
        StreamFileInput streamInput =
                new StreamFileInput() {
                    @Override
                    public synchronized void seek(long position) throws IOException {
                        raf.seek(position);
                    }

                    @Override
                    public synchronized int read(byte[] buf, int off, int len) throws IOException {
                        return raf.read(buf, off, len);
                    }
                };

        // 5. Open searcher from stream
        try (TantivySearcher searcher =
                new TantivySearcher(
                        fileNames.toArray(new String[0]),
                        fileOffsets.stream().mapToLong(Long::longValue).toArray(),
                        fileLengths.stream().mapToLong(Long::longValue).toArray(),
                        streamInput)) {

            SearchResult result = searcher.search("paimon", 10);
            assertTrue(result.size() > 0, "Should find at least one result");
            assertEquals(2, result.size(), "Both doc 0 and doc 2 mention paimon");

            for (int i = 0; i < result.size(); i++) {
                long rowId = result.getRowIds()[i];
                assertTrue(rowId == 0L || rowId == 2L, "Unexpected rowId: " + rowId);
                assertTrue(result.getScores()[i] > 0, "Score should be positive");
            }
        } finally {
            raf.close();
        }
    }

    private static void writeInt(ByteArrayOutputStream out, int value) {
        out.write((value >>> 24) & 0xFF);
        out.write((value >>> 16) & 0xFF);
        out.write((value >>> 8) & 0xFF);
        out.write(value & 0xFF);
    }

    private static void writeLong(ByteArrayOutputStream out, long value) {
        writeInt(out, (int) (value >>> 32));
        writeInt(out, (int) value);
    }
}
