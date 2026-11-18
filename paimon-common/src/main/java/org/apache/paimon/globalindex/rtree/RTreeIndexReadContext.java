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

import org.apache.paimon.fs.Path;
import org.apache.paimon.globalindex.GlobalIndexIOMeta;
import org.apache.paimon.globalindex.rtree.RTreeGlobalIndexReader.FileSearch;

import org.apache.paimon.shade.guava30.com.google.common.collect.MapMaker;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;

/** Byte-bounded page cache shared by readers created by one indexer. */
final class RTreeIndexReadContext {

    private final long capacity;
    private final Map<PageKey, PageBlock> pages = new LinkedHashMap<>(16, 0.75f, true);
    private long cachedBytes;

    // Separate supplied executors can share this context. The permit caps their combined I/O.
    final Semaphore filePermits;
    private final int fileConcurrency;
    private final Map<ExecutorService, FileWorkers> workersByExecutor =
            new MapMaker().weakKeys().weakValues().makeMap();

    RTreeIndexReadContext(long capacity, int fileConcurrency) {
        this.fileConcurrency = fileConcurrency;
        this.filePermits = new Semaphore(fileConcurrency);
        this.capacity = capacity;
    }

    synchronized FileWorkers workers(ExecutorService executor) {
        return workersByExecutor.computeIfAbsent(executor, e -> new FileWorkers(fileConcurrency));
    }

    /** Queue excess shards here so waiting for a file slot never occupies their shared executor. */
    static final class FileWorkers {
        private final int limit;
        private final ArrayDeque<FileSearch> queue = new ArrayDeque<>();
        private int workers;

        private FileWorkers(int limit) {
            this.limit = limit;
        }

        void execute(ExecutorService executor, FileSearch work) {
            synchronized (this) {
                if (executor.isShutdown()) {
                    throw new RejectedExecutionException("R-tree file executor is shut down.");
                }
                queue.addLast(work);
                if (workers == limit) {
                    return;
                }
                workers++;
            }
            try {
                executor.execute(this::drain);
            } catch (RuntimeException e) {
                List<FileSearch> rejected = new ArrayList<>();
                synchronized (this) {
                    queue.removeLastOccurrence(work);
                    if (--workers == 0) {
                        rejected.addAll(queue);
                        queue.clear();
                    }
                }
                rejected.forEach(task -> task.fail(e));
                throw e;
            }
        }

        synchronized void discardQueued(boolean terminated) {
            Iterator<FileSearch> tasks = queue.iterator();
            while (tasks.hasNext()) {
                FileSearch task = tasks.next();
                if (terminated || task.readerClosed()) {
                    tasks.remove();
                    task.discardIfIdle();
                }
            }
        }

        private void drain() {
            while (true) {
                FileSearch work;
                synchronized (this) {
                    work = queue.pollFirst();
                    if (work == null) {
                        workers--;
                        return;
                    }
                }
                work.run();
            }
        }
    }

    synchronized PageBlock get(FileKey file, int page) {
        return pages.get(new PageKey(file, page));
    }

    synchronized void put(FileKey file, PageBlock block) {
        if (block.pageBytes > capacity) {
            return;
        }
        if (block.bytes.length > capacity) {
            // Keep the most recently inserted pages without retaining an oversized backing array.
            int keepBytes = (int) (capacity / block.pageBytes) * block.pageBytes;
            int skipBytes = block.bytes.length - keepBytes;
            block =
                    new PageBlock(
                            block.firstPage + skipBytes / block.pageBytes,
                            block.pageBytes,
                            Arrays.copyOfRange(block.bytes, skipBytes, block.bytes.length));
        }

        if (block.cachedPages == 0) {
            cachedBytes += block.bytes.length;
        }
        for (int i = 0; i < block.pageCount(); i++) {
            PageBlock previous = pages.put(new PageKey(file, block.firstPage + i), block);
            block.cachedPages++;
            if (previous != null) {
                releasePage(previous);
            }
        }
        if (cachedBytes > capacity) {
            Iterator<PageBlock> oldest = pages.values().iterator();
            while (cachedBytes > capacity) {
                PageBlock evicted = oldest.next();
                oldest.remove();
                releasePage(evicted);
            }
        }
    }

    private void releasePage(PageBlock block) {
        if (--block.cachedPages == 0) {
            cachedBytes -= block.bytes.length;
        }
    }

    /** Pages from a contiguous physical read share the same immutable bytes. */
    static final class PageBlock {
        private final int firstPage;
        private final int pageBytes;
        private final byte[] bytes;
        // Guarded by the owning cache. A single cached page retains the entire array.
        private int cachedPages;

        PageBlock(int firstPage, int pageBytes, byte[] bytes) {
            this.firstPage = firstPage;
            this.pageBytes = pageBytes;
            this.bytes = bytes;
        }

        private int pageCount() {
            return bytes.length / pageBytes;
        }

        ByteBuffer page(int pageId) {
            return ByteBuffer.wrap(bytes, (pageId - firstPage) * pageBytes, pageBytes);
        }
    }

    synchronized long cachedBytes() {
        return cachedBytes;
    }

    /** Immutable file identity; page ordinals alone are not unique across readers. */
    static final class FileKey {
        private final Path path;
        private final long size;
        private final byte[] header;
        private final int hash;

        FileKey(GlobalIndexIOMeta file, byte[] header) {
            path = file.filePath();
            size = file.fileSize();
            this.header = Arrays.copyOf(header, header.length);
            hash = 31 * Objects.hash(path, size) + Arrays.hashCode(header);
        }

        @Override
        public int hashCode() {
            return hash;
        }

        @Override
        public boolean equals(Object other) {
            if (!(other instanceof FileKey)) {
                return false;
            }
            FileKey that = (FileKey) other;
            return size == that.size
                    && path.equals(that.path)
                    && Arrays.equals(header, that.header);
        }
    }

    private static final class PageKey {
        private final FileKey file;
        private final int page;

        private PageKey(FileKey file, int page) {
            this.file = file;
            this.page = page;
        }

        @Override
        public int hashCode() {
            return 31 * file.hashCode() + page;
        }

        @Override
        public boolean equals(Object other) {
            if (!(other instanceof PageKey)) {
                return false;
            }
            PageKey that = (PageKey) other;
            return page == that.page && file.equals(that.file);
        }
    }
}
