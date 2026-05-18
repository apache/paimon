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

package org.apache.paimon.elasticsearch.index.util;

import org.apache.paimon.fs.PositionOutputStream;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Utility for packing / unpacking Paimon GlobalIndex archive files.
 *
 * <p>Archive format: {@code [fileCount(4)] ( [nameLen(4)] [name(utf8)] [dataLen(8)] [data] )...}
 *
 * <p>CLI usage: {@code java -cp paimon-elasticsearch.jar
 * org.apache.paimon.elasticsearch.index.ESIndexArchiveUtils <archive.index> [outputDir]}
 */
public class ESIndexArchiveUtils {

    private ESIndexArchiveUtils() {}

    // ========================= pack =========================

    /**
     * Pack all regular files under {@code sourceDir} into the archive format, writing to {@code
     * out}.
     *
     * @return file offsets map: fileName → [dataOffset, dataLen]
     */
    public static Map<String, long[]> pack(Path sourceDir, PositionOutputStream out)
            throws IOException {
        File[] allFiles = sourceDir.toFile().listFiles();
        if (allFiles == null) {
            throw new IOException("Index directory is empty or not a directory: " + sourceDir);
        }

        List<File> indexFiles = new ArrayList<>();
        for (File file : allFiles) {
            if (file.isFile()) {
                indexFiles.add(file);
            }
        }

        Map<String, long[]> fileOffsets = new LinkedHashMap<>();

        writeInt(out, indexFiles.size());

        for (File file : indexFiles) {
            byte[] nameBytes = file.getName().getBytes(StandardCharsets.UTF_8);
            long fileLen = file.length();

            writeInt(out, nameBytes.length);
            out.write(nameBytes);
            writeLong(out, fileLen);

            long dataOffset = out.getPos();

            try (FileInputStream fis = new FileInputStream(file)) {
                byte[] buf = new byte[8192];
                int read;
                while ((read = fis.read(buf)) != -1) {
                    out.write(buf, 0, read);
                }
            }

            fileOffsets.put(file.getName(), new long[] {dataOffset, fileLen});
        }
        out.flush();

        return fileOffsets;
    }

    // ========================= unpack =========================

    /** Unpack an archive file into {@code outputDir}. */
    public static void unpack(Path archiveFile, Path outputDir) throws IOException {
        Files.createDirectories(outputDir);

        try (DataInputStream dis =
                new DataInputStream(new BufferedInputStream(Files.newInputStream(archiveFile)))) {

            int fileCount = dis.readInt();
            System.out.println("  " + fileCount + " files in archive");

            byte[] buf = new byte[8192];
            for (int i = 0; i < fileCount; i++) {
                int nameLen = dis.readInt();
                byte[] nameBytes = new byte[nameLen];
                dis.readFully(nameBytes);
                String fileName = new String(nameBytes, StandardCharsets.UTF_8);

                long dataLen = dis.readLong();

                Path filePath = outputDir.resolve(fileName);
                try (OutputStream fos = Files.newOutputStream(filePath)) {
                    long remaining = dataLen;
                    while (remaining > 0) {
                        int toRead = (int) Math.min(buf.length, remaining);
                        dis.readFully(buf, 0, toRead);
                        fos.write(buf, 0, toRead);
                        remaining -= toRead;
                    }
                }
                System.out.printf(
                        "  [%d/%d] %-20s (%,d bytes)%n", i + 1, fileCount, fileName, dataLen);
            }
        }
    }

    // ========================= I/O helpers =========================

    static void writeInt(PositionOutputStream out, int value) throws IOException {
        out.write((value >>> 24) & 0xFF);
        out.write((value >>> 16) & 0xFF);
        out.write((value >>> 8) & 0xFF);
        out.write(value & 0xFF);
    }

    static void writeLong(PositionOutputStream out, long value) throws IOException {
        writeInt(out, (int) (value >>> 32));
        writeInt(out, (int) value);
    }
}
