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

import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.globalindex.GlobalIndexIOMeta;
import org.apache.paimon.globalindex.GlobalIndexReader;
import org.apache.paimon.globalindex.GlobalIndexResult;
import org.apache.paimon.globalindex.ScoredGlobalIndexResult;
import org.apache.paimon.globalindex.io.GlobalIndexFileReader;
import org.apache.paimon.predicate.FieldRef;
import org.apache.paimon.predicate.FullTextSearch;
import org.apache.paimon.tantivy.SearchResult;
import org.apache.paimon.tantivy.StreamFileInput;
import org.apache.paimon.tantivy.TantivySearcher;
import org.apache.paimon.utils.RoaringNavigableMap64;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * Full-text global index reader using Tantivy.
 *
 * <p>Reads the archive header to get file layout, then opens a Tantivy searcher backed by JNI
 * callbacks to the {@link SeekableInputStream}. No temp files are created.
 *
 * <p>On {@link #close()}, the searcher is returned to the {@link TantivySearcherPool} rather than
 * destroyed, so the Rust-side index (including the FST term dictionary) stays warm across queries.
 */
public class TantivyFullTextGlobalIndexReader implements GlobalIndexReader {

    private final GlobalIndexIOMeta ioMeta;
    private final GlobalIndexFileReader fileReader;
    private final Map<String, ArchiveLayout> layoutCache;
    private final TantivySearcherPool searcherPool;
    private final String poolKey;

    private volatile TantivySearcherPool.PooledEntry borrowed;

    public TantivyFullTextGlobalIndexReader(
            GlobalIndexFileReader fileReader,
            List<GlobalIndexIOMeta> ioMetas,
            Map<String, ArchiveLayout> layoutCache,
            TantivySearcherPool searcherPool) {
        checkArgument(ioMetas.size() == 1, "Expected exactly one index file per shard");
        this.fileReader = fileReader;
        this.ioMeta = ioMetas.get(0);
        this.layoutCache = layoutCache;
        this.searcherPool = searcherPool;
        this.poolKey = this.ioMeta.filePath().toString() + "@" + this.ioMeta.fileSize();
    }

    @Override
    public Optional<ScoredGlobalIndexResult> visitFullTextSearch(FullTextSearch fullTextSearch) {
        try {
            ensureLoaded();
            SearchResult result =
                    borrowed.searcher.search(fullTextSearch.queryText(), fullTextSearch.limit());
            return Optional.of(toScoredResult(result));
        } catch (IOException e) {
            throw new RuntimeException("Failed to search Tantivy full-text index", e);
        }
    }

    private ScoredGlobalIndexResult toScoredResult(SearchResult result) {
        RoaringNavigableMap64 bitmap = new RoaringNavigableMap64();
        HashMap<Long, Float> id2scores = new HashMap<>(result.size());
        for (int i = 0; i < result.size(); i++) {
            long rowId = result.getRowIds()[i];
            bitmap.add(rowId);
            id2scores.put(rowId, result.getScores()[i]);
        }
        return new TantivyScoredGlobalIndexResult(bitmap, id2scores);
    }

    private void ensureLoaded() throws IOException {
        if (borrowed == null) {
            synchronized (this) {
                if (borrowed == null) {
                    TantivySearcherPool.PooledEntry entry = searcherPool.borrow(poolKey);
                    if (entry == null) {
                        entry = createEntry();
                    }
                    borrowed = entry;
                }
            }
        }
    }

    private TantivySearcherPool.PooledEntry createEntry() throws IOException {
        SeekableInputStream in = fileReader.getInputStream(ioMeta);
        try {
            ArchiveLayout layout = layoutCache.get(poolKey);
            if (layout == null) {
                layout = parseArchiveHeader(in);
                layoutCache.put(poolKey, layout);
            }
            StreamFileInput streamInput = new SynchronizedStreamFileInput(in);
            TantivySearcher searcher =
                    new TantivySearcher(
                            layout.fileNames, layout.fileOffsets, layout.fileLengths, streamInput);
            return new TantivySearcherPool.PooledEntry(searcher, in);
        } catch (Exception e) {
            in.close();
            throw e;
        }
    }

    /**
     * Parse the archive header to extract file names, offsets, and lengths. The archive format is:
     * [fileCount(4)] then for each file: [nameLen(4)] [name(utf8)] [dataLen(8)] [data].
     *
     * <p>This method reads the header sequentially and computes the absolute byte offset of each
     * file's data within the stream.
     */
    private static ArchiveLayout parseArchiveHeader(SeekableInputStream in) throws IOException {
        int fileCount = readInt(in);
        List<String> names = new ArrayList<>(fileCount);
        List<Long> offsets = new ArrayList<>(fileCount);
        List<Long> lengths = new ArrayList<>(fileCount);

        for (int i = 0; i < fileCount; i++) {
            int nameLen = readInt(in);
            byte[] nameBytes = new byte[nameLen];
            readFully(in, nameBytes);
            names.add(new String(nameBytes, StandardCharsets.UTF_8));

            long dataLen = readLong(in);
            long dataOffset = in.getPos();
            offsets.add(dataOffset);
            lengths.add(dataLen);

            // Skip past the file data
            in.seek(dataOffset + dataLen);
        }

        return new ArchiveLayout(
                names.toArray(new String[0]),
                offsets.stream().mapToLong(Long::longValue).toArray(),
                lengths.stream().mapToLong(Long::longValue).toArray());
    }

    private static int readInt(SeekableInputStream in) throws IOException {
        int b1 = in.read();
        int b2 = in.read();
        int b3 = in.read();
        int b4 = in.read();
        if ((b1 | b2 | b3 | b4) < 0) {
            throw new IOException("Unexpected end of stream");
        }
        return (b1 << 24) | (b2 << 16) | (b3 << 8) | b4;
    }

    private static long readLong(SeekableInputStream in) throws IOException {
        return ((long) readInt(in) << 32) | (readInt(in) & 0xFFFFFFFFL);
    }

    private static void readFully(SeekableInputStream in, byte[] buf) throws IOException {
        int off = 0;
        while (off < buf.length) {
            int read = in.read(buf, off, buf.length - off);
            if (read == -1) {
                throw new IOException("Unexpected end of stream");
            }
            off += read;
        }
    }

    @Override
    public void close() throws IOException {
        if (borrowed != null) {
            searcherPool.returnEntry(poolKey, borrowed);
            borrowed = null;
        }
    }

    // =================== unsupported =====================

    @Override
    public Optional<GlobalIndexResult> visitIsNotNull(FieldRef fieldRef) {
        return Optional.empty();
    }

    @Override
    public Optional<GlobalIndexResult> visitIsNull(FieldRef fieldRef) {
        return Optional.empty();
    }

    @Override
    public Optional<GlobalIndexResult> visitStartsWith(FieldRef fieldRef, Object literal) {
        return Optional.empty();
    }

    @Override
    public Optional<GlobalIndexResult> visitEndsWith(FieldRef fieldRef, Object literal) {
        return Optional.empty();
    }

    @Override
    public Optional<GlobalIndexResult> visitContains(FieldRef fieldRef, Object literal) {
        return Optional.empty();
    }

    @Override
    public Optional<GlobalIndexResult> visitLike(FieldRef fieldRef, Object literal) {
        return Optional.empty();
    }

    @Override
    public Optional<GlobalIndexResult> visitLessThan(FieldRef fieldRef, Object literal) {
        return Optional.empty();
    }

    @Override
    public Optional<GlobalIndexResult> visitGreaterOrEqual(FieldRef fieldRef, Object literal) {
        return Optional.empty();
    }

    @Override
    public Optional<GlobalIndexResult> visitNotEqual(FieldRef fieldRef, Object literal) {
        return Optional.empty();
    }

    @Override
    public Optional<GlobalIndexResult> visitLessOrEqual(FieldRef fieldRef, Object literal) {
        return Optional.empty();
    }

    @Override
    public Optional<GlobalIndexResult> visitEqual(FieldRef fieldRef, Object literal) {
        return Optional.empty();
    }

    @Override
    public Optional<GlobalIndexResult> visitGreaterThan(FieldRef fieldRef, Object literal) {
        return Optional.empty();
    }

    @Override
    public Optional<GlobalIndexResult> visitIn(FieldRef fieldRef, List<Object> literals) {
        return Optional.empty();
    }

    @Override
    public Optional<GlobalIndexResult> visitNotIn(FieldRef fieldRef, List<Object> literals) {
        return Optional.empty();
    }

    /**
     * Thread-safe wrapper around {@link SeekableInputStream} implementing {@link StreamFileInput}.
     * Rust JNI holds a Mutex across seek+read to prevent interleaving from concurrent threads.
     */
    private static class SynchronizedStreamFileInput implements StreamFileInput {
        private final SeekableInputStream in;

        SynchronizedStreamFileInput(SeekableInputStream in) {
            this.in = in;
        }

        @Override
        public synchronized void seek(long position) throws IOException {
            in.seek(position);
        }

        @Override
        public synchronized int read(byte[] buf, int off, int len) throws IOException {
            return in.read(buf, off, len);
        }
    }
}
