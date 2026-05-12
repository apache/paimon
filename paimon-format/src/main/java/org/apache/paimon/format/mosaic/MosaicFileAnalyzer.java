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

package org.apache.paimon.format.mosaic;

import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.SeekableInputStream;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import static org.apache.paimon.format.mosaic.MosaicUtils.readLong;
import static org.apache.paimon.format.mosaic.MosaicUtils.readVarint;
import static org.apache.paimon.utils.IOUtils.readFully;

/** Utility to analyze the storage breakdown of a Mosaic file. */
public class MosaicFileAnalyzer {

    public static String analyze(FileIO fileIO, Path path) throws IOException {
        long fileSize = fileIO.getFileSize(path);
        try (SeekableInputStream in = fileIO.newInputStream(path)) {
            return analyze(in, fileSize);
        }
    }

    public static String analyze(SeekableInputStream in, long fileSize) throws IOException {
        in.seek(fileSize - MosaicSpec.FOOTER_SIZE);
        byte[] footerBytes = new byte[MosaicSpec.FOOTER_SIZE];
        readFully(in, footerBytes);
        ByteBuffer footer = ByteBuffer.wrap(footerBytes).order(ByteOrder.BIG_ENDIAN);
        long indexOffset = footer.getLong();
        long schemaBlockOffset = footer.getLong();
        int numBuckets = footer.getInt();
        int numRowGroups = footer.getInt();
        byte compression = footer.get();
        byte version = footer.get();

        long schemaBlockSize = indexOffset - schemaBlockOffset;
        long indexSize = fileSize - MosaicSpec.FOOTER_SIZE - indexOffset;

        // Schema uncompressed size
        in.seek(schemaBlockOffset);
        byte[] lenBuf = new byte[4];
        readFully(in, lenBuf);
        int schemaUncompressed = ByteBuffer.wrap(lenBuf).order(ByteOrder.BIG_ENDIAN).getInt();
        long schemaCompressed = schemaBlockSize - 4;

        // Per-bucket stats from row group index (varint encoded, non-empty only)
        in.seek(indexOffset);
        byte[] indexBytes = new byte[(int) indexSize];
        readFully(in, indexBytes);
        int[] idxPos = {0};

        long totalCompressed = 0;
        long totalUncompressed = 0;
        int nonEmptyBuckets = 0;
        int totalRows = 0;

        for (int rg = 0; rg < numRowGroups; rg++) {
            totalRows += readVarint(indexBytes, idxPos);
            int nonEmpty = readVarint(indexBytes, idxPos);
            nonEmptyBuckets += nonEmpty;
            for (int i = 0; i < nonEmpty; i++) {
                readVarint(indexBytes, idxPos); // bucketId
                readLong(indexBytes, idxPos); // offset
                int cs = readVarint(indexBytes, idxPos);
                int us = readVarint(indexBytes, idxPos);
                totalCompressed += cs;
                totalUncompressed += us;
            }
        }

        return String.format(
                        "=== Mosaic File Analysis ===%n"
                                + "File size:    %,d bytes (%.1f KB)%n"
                                + "Version:      %d%n"
                                + "Compression:  %d%n"
                                + "Buckets:      %d (%d non-empty)%n"
                                + "Row groups:   %d%n"
                                + "Total rows:   %,d%n%n",
                        fileSize,
                        fileSize / 1024.0,
                        version,
                        compression,
                        numBuckets,
                        nonEmptyBuckets,
                        numRowGroups,
                        totalRows)
                + String.format(
                        "--- Section Sizes ---%n"
                                + "Bucket data:     %,9d bytes (%5.1f KB, %5.1f%%)%n"
                                + "Schema block:    %,9d bytes (%5.1f KB, %5.1f%%)%n"
                                + "Row group index: %,9d bytes (%5.1f KB, %5.1f%%)%n"
                                + "Footer:          %,9d bytes (%5.1f KB, %5.1f%%)%n%n",
                        schemaBlockOffset,
                        schemaBlockOffset / 1024.0,
                        100.0 * schemaBlockOffset / fileSize,
                        schemaBlockSize,
                        schemaBlockSize / 1024.0,
                        100.0 * schemaBlockSize / fileSize,
                        indexSize,
                        indexSize / 1024.0,
                        100.0 * indexSize / fileSize,
                        (long) MosaicSpec.FOOTER_SIZE,
                        MosaicSpec.FOOTER_SIZE / 1024.0,
                        100.0 * MosaicSpec.FOOTER_SIZE / fileSize)
                + String.format(
                        "--- Compression ---%n"
                                + "Schema:      %,9d -> %,9d bytes (%.1fx)%n"
                                + "Bucket data: %,9d -> %,9d bytes (%.1fx)%n",
                        schemaUncompressed,
                        schemaCompressed,
                        schemaCompressed > 0 ? (double) schemaUncompressed / schemaCompressed : 0,
                        totalUncompressed,
                        totalCompressed,
                        totalCompressed > 0 ? (double) totalUncompressed / totalCompressed : 0);
    }
}
