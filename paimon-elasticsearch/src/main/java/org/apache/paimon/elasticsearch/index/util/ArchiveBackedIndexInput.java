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

import org.apache.lucene.store.BufferedIndexInput;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Lucene {@link IndexInput} backed by a slice of bytes inside the paimon-elasticsearch archive
 * file. Each {@code readInternal} maps to a Range Read into the underlying archive starting at
 * {@code archiveBase + currentLogicalPos}.
 *
 * <p>Modeled after the OpenStore plugin's {@code HybridOpenIOIndexInput} but read-only and with no
 * Alluxio page cache; the archive {@link ArchiveFlatVectorReader.ArchiveByteRangeReader} is
 * responsible for whatever caching it provides (today: a single re-used {@code
 * SeekableInputStream}).
 */
public class ArchiveBackedIndexInput extends BufferedIndexInput {

    /** 4KB matches OpenStore's HybridOpenIOIndexInput default. */
    private static final int DEFAULT_BUFFER_SIZE = 4096;

    private final ArchiveFlatVectorReader.ArchiveByteRangeReader archive;

    /** Absolute offset within the archive where this slice begins. */
    private final long archiveBase;

    /** Length of this slice (this {@link IndexInput}'s logical length). */
    private final long sliceLength;

    private volatile boolean closed;

    public ArchiveBackedIndexInput(
            String resourceDescription,
            ArchiveFlatVectorReader.ArchiveByteRangeReader archive,
            long archiveBase,
            long sliceLength,
            IOContext context) {
        super(resourceDescription, DEFAULT_BUFFER_SIZE);
        this.archive = archive;
        this.archiveBase = archiveBase;
        this.sliceLength = sliceLength;
    }

    private ArchiveBackedIndexInput(
            String resourceDescription,
            ArchiveFlatVectorReader.ArchiveByteRangeReader archive,
            long archiveBase,
            long sliceLength,
            int bufferSize) {
        super(resourceDescription, bufferSize);
        this.archive = archive;
        this.archiveBase = archiveBase;
        this.sliceLength = sliceLength;
    }

    @Override
    protected void readInternal(ByteBuffer b) throws IOException {
        long pos = getFilePointer();
        int n = b.remaining();
        if (pos + n > sliceLength) {
            throw new EOFException(
                    "read past EOF: pos=" + pos + " n=" + n + " sliceLength=" + sliceLength);
        }
        byte[] bytes = archive.readRange(archiveBase + pos, n);
        b.put(bytes);
    }

    @Override
    protected void seekInternal(long pos) throws IOException {
        if (pos < 0 || pos > sliceLength) {
            throw new IOException(
                    "seek out of range: " + pos + " (sliceLength=" + sliceLength + ")");
        }
        // BufferedIndexInput tracks the logical pointer itself; no extra state needed here.
    }

    @Override
    public long length() {
        return sliceLength;
    }

    @Override
    public IndexInput slice(String sliceDescription, long offset, long length) throws IOException {
        if (offset < 0 || length < 0 || offset + length > this.sliceLength) {
            throw new IllegalArgumentException(
                    "slice("
                            + sliceDescription
                            + "): offset="
                            + offset
                            + " length="
                            + length
                            + " sliceLength="
                            + this.sliceLength);
        }
        ArchiveBackedIndexInput child =
                new ArchiveBackedIndexInput(
                        sliceDescription,
                        archive,
                        this.archiveBase + offset,
                        length,
                        getBufferSize());
        // Slices are independent positions — start at 0.
        return child;
    }

    @Override
    public ArchiveBackedIndexInput clone() {
        ArchiveBackedIndexInput c = (ArchiveBackedIndexInput) super.clone();
        // BufferedIndexInput.clone() handles buffer cloning; nothing extra to copy here since
        // archive/base/length are immutable references.
        return c;
    }

    @Override
    public void close() throws IOException {
        closed = true;
        // Underlying archive stream lifecycle is owned by the caller (paimon-elasticsearch reader).
    }
}
