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

package org.apache.paimon.globalindex.rtree;

import org.apache.paimon.fs.FileRange;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.fs.VectoredReadUtils;
import org.apache.paimon.fs.VectoredReadable;
import org.apache.paimon.geo.BoundingBox;
import org.apache.paimon.globalindex.GlobalIndexIOMeta;
import org.apache.paimon.globalindex.GlobalIndexReader;
import org.apache.paimon.globalindex.GlobalIndexResult;
import org.apache.paimon.globalindex.io.GlobalIndexFileReader;
import org.apache.paimon.globalindex.rtree.RTreeIndexReadContext.PageBlock;
import org.apache.paimon.predicate.FieldRef;
import org.apache.paimon.types.DataField;
import org.apache.paimon.utils.Range;
import org.apache.paimon.utils.RoaringNavigableMap64;

import javax.annotation.Nullable;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.paimon.utils.FutureUtils.completedExceptionally;

/** Demand-loaded R-tree reader using a shared byte-bounded cache and bounded page batches. */
final class RTreeGlobalIndexReader implements GlobalIndexReader {

    private final DataField field;
    private final GlobalIndexFileReader fileReader;
    private final List<GlobalIndexIOMeta> files;
    private final List<RTreeIndexFile.Metadata> metadata = new ArrayList<>();
    private final long totalRowCount;
    private final ExecutorService executor;
    private final RTreeIndexReadContext readContext;
    private final List<RTreeIndexReadContext.FileKey> fileKeys = new ArrayList<>();
    private final int readBatchBytes;
    private final int readFileConcurrency;
    private volatile boolean closed;

    RTreeGlobalIndexReader(
            DataField field,
            GlobalIndexFileReader fileReader,
            List<GlobalIndexIOMeta> files,
            long totalRowCount,
            @Nullable List<Range> rowRanges,
            ExecutorService executor,
            RTreeIndexReadContext readContext,
            int readBatchBytes,
            int readFileConcurrency) {
        this.field = field;
        this.fileReader = fileReader;
        this.files = new ArrayList<>();
        this.totalRowCount = totalRowCount;
        this.executor = executor;
        this.readContext = readContext;
        this.readBatchBytes = readBatchBytes;
        this.readFileConcurrency = readFileConcurrency;
        for (GlobalIndexIOMeta file : files) {
            RTreeIndexFile.Metadata meta = RTreeIndexFile.Metadata.read(file, totalRowCount);
            if (rowRanges == null
                    || rowRanges.stream()
                            .anyMatch(
                                    range ->
                                            Range.intersect(
                                                    range.from,
                                                    range.to,
                                                    meta.firstRow,
                                                    meta.lastRow))) {
                this.files.add(file);
                metadata.add(meta);
                fileKeys.add(new RTreeIndexReadContext.FileKey(file, meta.bytes));
            }
        }
    }

    @Override
    public CompletableFuture<Optional<GlobalIndexResult>> visitBBoxIntersects(
            FieldRef fieldRef, double minX, double minY, double maxX, double maxY) {
        if (!fieldRef.type().equals(field.type())) {
            return unsupported();
        }
        BoundingBox query = BoundingBox.of(minX, minY, maxX, maxY);
        return search(query, false);
    }

    @Override
    public CompletableFuture<Optional<GlobalIndexResult>> visitIsNull(FieldRef fieldRef) {
        return search(null, true);
    }

    @Override
    public CompletableFuture<Optional<GlobalIndexResult>> visitIsNotNull(FieldRef fieldRef) {
        return search(null, false);
    }

    private CompletableFuture<Optional<GlobalIndexResult>> search(
            @Nullable BoundingBox query, boolean nulls) {
        if (closed) {
            return completedExceptionally(new IllegalStateException("R-tree reader is closed."));
        }
        if (files.isEmpty()) {
            return CompletableFuture.completedFuture(
                    Optional.of(GlobalIndexResult.create(new RoaringNavigableMap64())));
        }
        FileSearch search = new FileSearch(query, nulls);
        int workers = Math.min(readFileConcurrency, files.size());
        for (int i = 0; i < workers && search.hasWork(); i++) {
            try {
                executor.execute(search);
            } catch (RuntimeException e) {
                search.fail(e);
                break;
            }
        }
        return search.completion;
    }

    /**
     * Only workers that actually start own work. An executor may discard queued tasks on shutdown.
     */
    private final class FileSearch implements Runnable {
        @Nullable private final BoundingBox query;
        private final boolean nulls;
        private final AtomicInteger next = new AtomicInteger();
        private final CompletableFuture<Optional<GlobalIndexResult>> completion =
                new CompletableFuture<>();
        private volatile Throwable failure;
        // Guarded by this. Each running worker owns a separate bitmap until it finishes.
        private int running;
        private RoaringNavigableMap64 rows;

        private FileSearch(@Nullable BoundingBox query, boolean nulls) {
            this.query = query;
            this.nulls = nulls;
        }

        private boolean hasWork() {
            return failure == null && next.get() < files.size();
        }

        @Override
        public void run() {
            synchronized (this) {
                if (!hasWork()) {
                    return;
                }
                running++;
            }
            RoaringNavigableMap64 partial = null;
            try {
                partial = new RoaringNavigableMap64();
                int file;
                while (failure == null && (file = next.getAndIncrement()) < files.size()) {
                    searchFile(file, query, nulls, partial);
                }
            } catch (RuntimeException | Error e) {
                fail(e);
            } finally {
                finish(partial);
            }
        }

        private synchronized void fail(Throwable cause) {
            if (failure == null) {
                failure = cause;
            }
            if (running == 0) {
                completion.completeExceptionally(failure);
            }
        }

        private synchronized void finish(@Nullable RoaringNavigableMap64 partial) {
            running--;
            if (failure == null) {
                try {
                    if (rows == null) {
                        rows = partial;
                    } else {
                        rows.or(partial);
                    }
                } catch (RuntimeException | Error e) {
                    fail(e);
                }
            }
            if (running == 0) {
                if (failure != null) {
                    completion.completeExceptionally(failure);
                } else {
                    completion.complete(Optional.of(GlobalIndexResult.create(rows)));
                }
            }
        }
    }

    private void searchFile(
            int file, @Nullable BoundingBox query, boolean nulls, RoaringNavigableMap64 result) {
        RTreeIndexFile.Metadata meta = metadata.get(file);
        if (query != null
                ? !meta.bounds.intersects(query)
                : (nulls ? meta.nulls : meta.nonNulls) == 0) {
            return;
        }
        try (FileAccess access = new FileAccess(file)) {
            if (query == null) {
                readRows(access, nulls, result);
            } else {
                searchTree(access, query, result);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(
                    "Failed to read R-tree index " + files.get(file).filePath(), e);
        }
    }

    private void searchTree(FileAccess access, BoundingBox query, RoaringNavigableMap64 result)
            throws IOException {
        RTreeIndexFile.Metadata meta = metadata.get(access.file);
        ArrayDeque<Long> pending = new ArrayDeque<>();
        BitSet children = new BitSet();
        pending.push(((long) (meta.levelStarts.length - 1) << 32) | (meta.pages - 1L));
        while (!pending.isEmpty()) {
            int batchPages = Math.max(1, readBatchBytes / RTreeIndexFile.pageBytes(meta.pageSize));
            List<Long> nodes = new ArrayList<>();
            while (!pending.isEmpty() && nodes.size() < batchPages) {
                nodes.add(pending.pop());
            }
            Map<Integer, PageBlock> batch = access.pages(nodes);
            for (long node : nodes) {
                int pageId = (int) node;
                int expectedLevel = (int) (node >>> 32);
                ByteBuffer page = batch.get(pageId).page(pageId);
                int count = page.getInt();
                int level = page.getInt();
                if (count < 1 || count > meta.pageSize || level != expectedLevel) {
                    throw new IOException("Invalid R-tree page header.");
                }
                for (int i = 0; i < count; i++) {
                    BoundingBox box = RTreeIndexFile.getBox(page);
                    long id = page.getLong();
                    if (level == 0) {
                        validateRow(id, meta);
                    } else {
                        if (id < meta.levelStarts[level - 1]
                                || id >= meta.levelStarts[level]
                                || children.get((int) id)) {
                            throw new IOException("Invalid R-tree child page.");
                        }
                        children.set((int) id);
                    }
                    if (box.intersects(query)) {
                        if (level == 0) {
                            result.add(id);
                        } else {
                            pending.push(((long) (level - 1) << 32) | id);
                        }
                    }
                }
            }
        }
    }

    private void readRows(FileAccess access, boolean nulls, RoaringNavigableMap64 result)
            throws IOException {
        RTreeIndexFile.Metadata meta = metadata.get(access.file);
        long offset = meta.rowsOffset();
        int count = nulls ? meta.nulls : meta.nonNulls;
        if (!nulls) {
            offset += (long) meta.nulls * Long.BYTES + Integer.BYTES;
        }
        byte[] bytes = access.read(offset, count * Long.BYTES + Integer.BYTES);
        RTreeIndexFile.verify(bytes);
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        RoaringNavigableMap64 seen = new RoaringNavigableMap64();
        for (int i = 0; i < count; i++) {
            long id = buffer.getLong();
            validateRow(id, meta);
            if (seen.contains(id)) {
                throw new IOException("Duplicate R-tree source row ID.");
            }
            seen.add(id);
        }
        result.or(seen);
    }

    private void validateRow(long id, RTreeIndexFile.Metadata meta) throws IOException {
        if (id < meta.firstRow || id > meta.lastRow || id >= totalRowCount) {
            throw new IOException("Invalid R-tree row ID.");
        }
    }

    private final class FileAccess implements AutoCloseable {
        private final int file;
        private SeekableInputStream input;

        private FileAccess(int file) {
            this.file = file;
        }

        private void open() throws IOException {
            if (input == null) {
                input = fileReader.getInputStream(files.get(file));
                byte[] header = new byte[RTreeIndexFile.HEADER_SIZE];
                new DataInputStream(input).readFully(header);
                if (!Arrays.equals(header, metadata.get(file).bytes)) {
                    throw new IOException("R-tree file header disagrees with container metadata.");
                }
            }
        }

        private byte[] read(long offset, int length) throws IOException {
            open();
            input.seek(offset);
            byte[] bytes = new byte[length];
            new DataInputStream(input).readFully(bytes);
            return bytes;
        }

        private Map<Integer, PageBlock> pages(List<Long> nodes) throws IOException {
            RTreeIndexFile.Metadata meta = metadata.get(file);
            RTreeIndexReadContext.FileKey key = fileKeys.get(file);
            int pageBytes = RTreeIndexFile.pageBytes(meta.pageSize);
            Map<Integer, PageBlock> batch = new HashMap<>();
            List<Integer> missing = new ArrayList<>();
            for (long node : nodes) {
                int pageId = (int) node;
                if (pageId < 0 || pageId >= meta.pages) {
                    throw new IOException("Invalid R-tree page ID.");
                }
                PageBlock cached = readContext.get(key, pageId);
                if (cached == null) {
                    if (!batch.containsKey(pageId)) {
                        missing.add(pageId);
                    }
                }
                batch.put(pageId, cached);
            }
            if (!missing.isEmpty()) {
                open();
                Collections.sort(missing);
                List<FileRange> ranges = new ArrayList<>();
                for (int first = 0; first < missing.size(); ) {
                    int end = first + 1;
                    while (end < missing.size() && missing.get(end) == missing.get(end - 1) + 1) {
                        end++;
                    }
                    ranges.add(
                            FileRange.createFileRange(
                                    RTreeIndexFile.HEADER_SIZE
                                            + (long) missing.get(first) * pageBytes,
                                    (end - first) * pageBytes));
                    first = end;
                }
                if (input instanceof VectoredReadable) {
                    readPositioned((VectoredReadable) input, ranges);
                } else {
                    for (FileRange range : ranges) {
                        range.getData().complete(read(range.getOffset(), range.getLength()));
                    }
                }
                for (FileRange range : ranges) {
                    int first =
                            (int) ((range.getOffset() - RTreeIndexFile.HEADER_SIZE) / pageBytes);
                    byte[] bytes = range.getData().join();
                    PageBlock block = new PageBlock(first, pageBytes, bytes);
                    for (int offset = 0; offset < bytes.length; offset += pageBytes) {
                        RTreeIndexFile.verify(bytes, offset, pageBytes);
                        batch.put(first + offset / pageBytes, block);
                    }
                    readContext.put(key, block);
                }
            }
            return batch;
        }

        private void readPositioned(VectoredReadable input, List<FileRange> ranges)
                throws IOException {
            int parallelism = Math.min(4, Math.max(1, input.parallelismForVectorReads()));
            for (int first = 0; first < ranges.size(); first += parallelism) {
                List<CompletableFuture<byte[]>> submitted = new ArrayList<>();
                IOException failure = null;
                int end = Math.min(ranges.size(), first + parallelism);
                try {
                    for (int i = first; i < end; i++) {
                        FileRange range = ranges.get(i);
                        submitted.add(
                                CompletableFuture.supplyAsync(
                                        () -> {
                                            byte[] bytes = new byte[range.getLength()];
                                            try {
                                                input.preadFully(
                                                        range.getOffset(), bytes, 0, bytes.length);
                                                return bytes;
                                            } catch (IOException e) {
                                                throw new UncheckedIOException(e);
                                            }
                                        },
                                        VectoredReadUtils.IO_THREAD_POOL));
                    }
                } catch (RuntimeException e) {
                    failure = new IOException("Failed to schedule R-tree page batch.", e);
                }
                try {
                    // Join only submitted tasks, including on scheduling failure. The pool is
                    // independent of the query executor, and joining is uninterruptible, so an
                    // interrupted query cannot close the stream while reads are still active.
                    CompletableFuture.allOf(submitted.toArray(new CompletableFuture<?>[0])).join();
                } catch (CompletionException e) {
                    if (failure == null) {
                        failure =
                                new IOException("Failed to read R-tree page batch.", e.getCause());
                    } else {
                        failure.addSuppressed(e);
                    }
                }
                if (failure != null) {
                    throw failure;
                }
                for (int i = first; i < end; i++) {
                    ranges.get(i).getData().complete(submitted.get(i - first).join());
                }
            }
        }

        @Override
        public void close() throws IOException {
            if (input != null) {
                input.close();
            }
        }
    }

    private static CompletableFuture<Optional<GlobalIndexResult>> unsupported() {
        return CompletableFuture.completedFuture(Optional.empty());
    }

    @Override
    public CompletableFuture<Optional<GlobalIndexResult>> visitStartsWith(
            FieldRef fieldRef, Object literal) {
        return unsupported();
    }

    @Override
    public CompletableFuture<Optional<GlobalIndexResult>> visitEndsWith(
            FieldRef fieldRef, Object literal) {
        return unsupported();
    }

    @Override
    public CompletableFuture<Optional<GlobalIndexResult>> visitContains(
            FieldRef fieldRef, Object literal) {
        return unsupported();
    }

    @Override
    public CompletableFuture<Optional<GlobalIndexResult>> visitLike(
            FieldRef fieldRef, Object literal) {
        return unsupported();
    }

    @Override
    public CompletableFuture<Optional<GlobalIndexResult>> visitLessThan(
            FieldRef fieldRef, Object literal) {
        return unsupported();
    }

    @Override
    public CompletableFuture<Optional<GlobalIndexResult>> visitGreaterOrEqual(
            FieldRef fieldRef, Object literal) {
        return unsupported();
    }

    @Override
    public CompletableFuture<Optional<GlobalIndexResult>> visitNotEqual(
            FieldRef fieldRef, Object literal) {
        return unsupported();
    }

    @Override
    public CompletableFuture<Optional<GlobalIndexResult>> visitLessOrEqual(
            FieldRef fieldRef, Object literal) {
        return unsupported();
    }

    @Override
    public CompletableFuture<Optional<GlobalIndexResult>> visitEqual(
            FieldRef fieldRef, Object literal) {
        return unsupported();
    }

    @Override
    public CompletableFuture<Optional<GlobalIndexResult>> visitGreaterThan(
            FieldRef fieldRef, Object literal) {
        return unsupported();
    }

    @Override
    public CompletableFuture<Optional<GlobalIndexResult>> visitIn(
            FieldRef fieldRef, List<Object> literals) {
        return unsupported();
    }

    @Override
    public CompletableFuture<Optional<GlobalIndexResult>> visitNotIn(
            FieldRef fieldRef, List<Object> literals) {
        return unsupported();
    }

    @Override
    public void close() {
        closed = true;
    }
}
