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

package org.apache.paimon.io.cache;

import org.apache.paimon.data.AbstractPagedInputView;
import org.apache.paimon.io.SeekableDataInputView;
import org.apache.paimon.memory.MemorySegment;
import org.apache.paimon.utils.MathUtils;

import java.io.Closeable;
import java.io.EOFException;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiConsumer;

/**
 * A {@link SeekableDataInputView} to read bytes from {@link RandomAccessFile}, the bytes can be
 * cached to {@link MemorySegment}s in {@link CacheManager}.
 */
public class CachedRandomInputView extends AbstractPagedInputView
        implements SeekableDataInputView, Closeable {

    private final RandomAccessFile file;
    private final long fileLength;
    private final CacheManager cacheManager;
    private final Map<CacheKey, MemorySegment> segments;
    private final int segmentSize;
    private final int segmentSizeBits;
    private final int segmentSizeMask;

    private int currentSegmentIndex;

    public CachedRandomInputView(File file, CacheManager cacheManager, int segmentSize)
            throws FileNotFoundException {
        this.file = new RandomAccessFile(file, "r");
        this.fileLength = file.length();
        this.cacheManager = cacheManager;
        this.segments = new HashMap<>();
        this.segmentSize = segmentSize;
        this.segmentSizeBits = MathUtils.log2strict(segmentSize);
        this.segmentSizeMask = segmentSize - 1;

        this.currentSegmentIndex = -1;
    }

    @Override
    public void setReadPosition(long position) {
        final int offset = (int) (position & this.segmentSizeMask);
        this.currentSegmentIndex = positionToSegmentIndex(position);
        MemorySegment segment = getCurrentPage();
        seekInput(segment, offset, getLimitForSegment(segment));
    }

    private MemorySegment getCurrentPage() {
        long position = segmentIndexToPosition(currentSegmentIndex);
        int length = (int) Math.min(segmentSize, fileLength - position);
        CacheKey cacheKey = new CacheKey(position, length);
        MemorySegment segment = segments.get(cacheKey);
        if (segment == null) {
            segment = cacheManager.getPage(file, position, length, this::invalidPage);
            segments.put(cacheKey, segment);
        }
        return segment;
    }

    @Override
    protected MemorySegment nextSegment(MemorySegment current) throws EOFException {
        currentSegmentIndex++;
        if (segmentIndexToPosition(currentSegmentIndex) >= fileLength) {
            throw new EOFException();
        }

        return getCurrentPage();
    }

    @Override
    protected int getLimitForSegment(MemorySegment segment) {
        return segment.size();
    }

    private void invalidPage(long position, int length) {
        segments.remove(new CacheKey(position, length));
    }

    /**
     * Read a continuous segment of the specified length at a given position.
     *
     * @param position The start reading position.
     * @param length The length to read.
     * @return The wrapped segment. NOTE: this segment should not be cached outside this class.
     */
    public MemorySegment read(int position, int length, BiConsumer<Long, Integer> cleanCallback) {
        final int offset = position & this.segmentSizeMask;
        length = (int) Math.min(length, fileLength - offset);
        CacheKey cacheKey = new CacheKey(position, length);
        MemorySegment segment = segments.get(cacheKey);
        if (segment == null) {
            segment =
                    cacheManager.getPage(
                            file,
                            position,
                            length,
                            new BiConsumer<Long, Integer>() {
                                @Override
                                public void accept(Long position, Integer length) {
                                    invalidPage(position, length);
                                    cleanCallback.accept(position, length);
                                }
                            });
            segments.put(cacheKey, segment);
        }
        return segment;
    }

    @Override
    public void close() throws IOException {
        // copy out to avoid ConcurrentModificationException
        List<CacheKey> pages = new ArrayList<>(segments.keySet());
        pages.forEach(
                page -> {
                    cacheManager.invalidPage(file, page.readOffset, page.readLength);
                });

        file.close();
    }

    private int positionToSegmentIndex(long position) {
        return (int) (position >>> this.segmentSizeBits);
    }

    private long segmentIndexToPosition(int segmentIndex) {
        return (long) segmentIndex << segmentSizeBits;
    }

    private static class CacheKey {
        private final long readOffset;
        private final int readLength;

        public CacheKey(long readOffset, int readLength) {
            this.readOffset = readOffset;
            this.readLength = readLength;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof CacheKey)) {
                return false;
            }
            CacheKey cacheKey = (CacheKey) o;
            return readOffset == cacheKey.readOffset && readLength == cacheKey.readLength;
        }

        @Override
        public int hashCode() {
            return Objects.hash(readOffset, readLength);
        }
    }
}
