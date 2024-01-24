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

/**
 * A {@link SeekableDataInputView} to read bytes from {@link RandomAccessFile}, the bytes can be
 * cached to {@link MemorySegment}s in {@link CacheManager}.
 */
public class CachedRandomInputView extends AbstractPagedInputView
        implements SeekableDataInputView, Closeable {

    private final RandomAccessFile file;
    private final long fileLength;
    private final CacheManager cacheManager;
    private final Map<Integer, MemorySegment> segments;
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
        MemorySegment segment = segments.get(currentSegmentIndex);
        if (segment == null) {
            long offset = segmentIndexToPosition(currentSegmentIndex);
            int length = (int) Math.min(segmentSize, fileLength - offset);
            segment = cacheManager.getPage(file, offset, length, this::invalidPage);
            segments.put(currentSegmentIndex, segment);
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
        segments.remove(positionToSegmentIndex(position));
    }

    @Override
    public void close() throws IOException {
        // copy out to avoid ConcurrentModificationException
        List<Integer> pages = new ArrayList<>(segments.keySet());
        pages.forEach(
                page -> {
                    long offset = segmentIndexToPosition(page);
                    int length = (int) Math.min(segmentSize, fileLength - offset);
                    cacheManager.invalidPage(file, offset, length);
                });

        file.close();
    }

    private int positionToSegmentIndex(long position) {
        return (int) (position >>> this.segmentSizeBits);
    }

    private long segmentIndexToPosition(int segmentIndex) {
        return (long) segmentIndex << segmentSizeBits;
    }

    public RandomAccessFile file() {
        return file;
    }
}
