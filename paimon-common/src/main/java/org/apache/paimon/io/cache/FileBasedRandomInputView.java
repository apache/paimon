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
import org.apache.paimon.io.PageFileInput;
import org.apache.paimon.io.SeekableDataInputView;
import org.apache.paimon.io.cache.CacheKey.PageIndexCacheKey;
import org.apache.paimon.memory.MemorySegment;
import org.apache.paimon.utils.MathUtils;
import org.apache.paimon.io.cache.CacheManager.SegmentContainer;

import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.paimon.io.cache.CacheManager.REFRESH_COUNT;

/**
 * A {@link SeekableDataInputView} to read bytes from {@link RandomAccessFile}, the bytes can be
 * cached to {@link MemorySegment}s in {@link CacheManager}.
 */
public class FileBasedRandomInputView extends AbstractPagedInputView
        implements SeekableDataInputView, Closeable {

    private final PageFileInput input;
    private final CacheManager cacheManager;
    private final Map<Integer, SegmentContainer> segments;
    private final int segmentSizeBits;
    private final int segmentSizeMask;

    private int currentSegmentIndex;

    public FileBasedRandomInputView(PageFileInput input, CacheManager cacheManager) {
        this.input = input;
        this.cacheManager = cacheManager;
        this.segments = new HashMap<>();
        int segmentSize = input.pageSize();
        this.segmentSizeBits = MathUtils.log2strict(segmentSize);
        this.segmentSizeMask = segmentSize - 1;

        this.currentSegmentIndex = -1;
    }

    @Override
    public void setReadPosition(long position) {
        int offset = (int) (position & this.segmentSizeMask);
        this.currentSegmentIndex = (int) (position >>> this.segmentSizeBits);
        MemorySegment segment = getCurrentPage();
        seekInput(segment, offset, getLimitForSegment(segment));
    }

    private MemorySegment getCurrentPage() {
        SegmentContainer container = segments.get(currentSegmentIndex);
        if (container == null || container.getAccessCount() == REFRESH_COUNT) {
            int pageIndex = currentSegmentIndex;
            MemorySegment segment =
                    cacheManager.getPage(
                            CacheKey.forPageIndex(input.file(), input.pageSize(), pageIndex),
                            key -> input.readPage(pageIndex),
                            this::invalidPage);
            container = new SegmentContainer(segment);
            segments.put(currentSegmentIndex, container);
        }
        return container.access();
    }

    @Override
    protected MemorySegment nextSegment(MemorySegment current) throws EOFException {
        currentSegmentIndex++;
        if ((long) currentSegmentIndex << segmentSizeBits >= input.uncompressBytes()) {
            throw new EOFException();
        }

        return getCurrentPage();
    }

    @Override
    protected int getLimitForSegment(MemorySegment segment) {
        return segment.size();
    }

    private void invalidPage(CacheKey key) {
        segments.remove(((PageIndexCacheKey) key).pageIndex());
    }

    @Override
    public void close() throws IOException {
        // copy out to avoid ConcurrentModificationException
        List<Integer> pages = new ArrayList<>(segments.keySet());
        pages.forEach(
                page ->
                        cacheManager.invalidPage(
                                CacheKey.forPageIndex(input.file(), input.pageSize(), page)));

        input.close();
    }
}
