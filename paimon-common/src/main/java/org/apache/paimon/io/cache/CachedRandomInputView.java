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
    private final int segmentSizeBits;
    private final int segmentSizeMask;

    private int currentSegmentIndex;

    public CachedRandomInputView(File file, CacheManager cacheManager)
            throws FileNotFoundException {
        this.file = new RandomAccessFile(file, "r");
        this.fileLength = file.length();
        this.cacheManager = cacheManager;
        this.segments = new HashMap<>();
        int segmentSize = cacheManager.pageSize();
        this.segmentSizeBits = MathUtils.log2strict(segmentSize);
        this.segmentSizeMask = segmentSize - 1;

        this.currentSegmentIndex = -1;
    }

    @Override
    public void setReadPosition(long position) {
        final int pageNumber = (int) (position >>> this.segmentSizeBits);
        final int offset = (int) (position & this.segmentSizeMask);
        this.currentSegmentIndex = pageNumber;
        MemorySegment segment = getCurrentPage();
        seekInput(segment, offset, getLimitForSegment(segment));
    }

    private MemorySegment getCurrentPage() {
        MemorySegment segment = segments.get(currentSegmentIndex);
        if (segment == null) {
            segment =
                    cacheManager.getPage(file, fileLength, currentSegmentIndex, this::invalidPage);
            segments.put(currentSegmentIndex, segment);
        }
        return segment;
    }

    @Override
    protected MemorySegment nextSegment(MemorySegment current) throws EOFException {
        currentSegmentIndex++;
        if ((long) currentSegmentIndex << segmentSizeBits >= fileLength) {
            throw new EOFException();
        }

        return getCurrentPage();
    }

    @Override
    protected int getLimitForSegment(MemorySegment segment) {
        return segment.size();
    }

    private void invalidPage(int pageNumber) {
        segments.remove(pageNumber);
    }

    @Override
    public void close() throws IOException {
        // copy out to avoid ConcurrentModificationException
        List<Integer> pages = new ArrayList<>(segments.keySet());
        pages.forEach(page -> cacheManager.invalidPage(file, page));

        file.close();
    }
}
