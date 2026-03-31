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

package org.apache.paimon.globalindex.btree;

import org.apache.paimon.compression.BlockCompressionFactory;
import org.apache.paimon.compression.CompressOptions;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.globalindex.GlobalIndexFileReadWrite;
import org.apache.paimon.globalindex.ResultEntry;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestEntrySerializer;
import org.apache.paimon.memory.MemorySlice;
import org.apache.paimon.sst.BlockHandle;
import org.apache.paimon.sst.BloomFilterHandle;
import org.apache.paimon.sst.SstFileWriter;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * btree_file_meta writer: stores {@code fileName -> ManifestEntry bytes} in a single SST file.
 *
 * <p>Duplicate fileNames are silently ignored (only the first occurrence is written). Keys must be
 * written in lexicographic order of fileName, which is guaranteed because callers iterate over
 * {@link org.apache.paimon.io.DataFileMeta} lists that are already ordered.
 */
public class BinaryFileMetaWriter {

    /** Prefix used when creating the SST file name. */
    public static final String FILE_PREFIX = "btree-file-meta";

    private final String fileName;
    private final PositionOutputStream out;
    private final SstFileWriter writer;
    private final ManifestEntrySerializer manifestSerializer;
    private final KeySerializer keySerializer;
    private final Set<String> writtenFileNames = new LinkedHashSet<>();
    private long rowCount = 0;

    public BinaryFileMetaWriter(
            GlobalIndexFileReadWrite rw, ManifestEntrySerializer manifestSerializer)
            throws IOException {
        this.fileName = rw.newFileName(FILE_PREFIX);
        this.out = rw.newOutputStream(this.fileName);
        // Use no compression and default block size (64 KiB) for simplicity
        this.writer =
                new SstFileWriter(
                        out,
                        64 * 1024,
                        null,
                        BlockCompressionFactory.create(new CompressOptions("none", 1)));
        this.manifestSerializer = manifestSerializer;
        this.keySerializer = new KeySerializer.StringSerializer();
    }

    /**
     * Writes a single {@code fileName -> ManifestEntry} mapping. If {@code fileName} was already
     * written, this call is a no-op (idempotent).
     */
    public void write(String fileName, ManifestEntry entry) throws IOException {
        if (writtenFileNames.contains(fileName)) {
            return;
        }
        writtenFileNames.add(fileName);
        rowCount++;

        byte[] keyBytes =
                keySerializer.serialize(org.apache.paimon.data.BinaryString.fromString(fileName));
        byte[] valueBytes = manifestSerializer.serializeToBytes(entry);
        writer.put(keyBytes, valueBytes);
    }

    /**
     * Finalizes the SST file and returns a single {@link ResultEntry}. Must be called exactly once.
     */
    public List<ResultEntry> finish() throws IOException {
        if (rowCount == 0) {
            // Nothing was written – close and return empty (caller should handle this)
            out.close();
            return Collections.emptyList();
        }

        writer.flush();
        BloomFilterHandle bloomFilterHandle = writer.writeBloomFilter();
        BlockHandle indexBlockHandle = writer.writeIndexBlock();
        BTreeFileFooter footer = new BTreeFileFooter(bloomFilterHandle, indexBlockHandle, null);
        MemorySlice footerEncoding = BTreeFileFooter.writeFooter(footer);
        writer.writeSlice(footerEncoding);
        out.close();

        return Collections.singletonList(new ResultEntry(fileName, rowCount, null));
    }
}
