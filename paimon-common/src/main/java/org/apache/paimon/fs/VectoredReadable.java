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

package org.apache.paimon.fs;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.function.IntFunction;

/** Stream that permits vectored reading. */
public interface VectoredReadable {

    /**
     * Read up to the specified number of bytes, from a given position within a file, and return the
     * number of bytes read. This does not change the current offset of a file, and is thread-safe.
     */
    int pread(long position, byte[] buffer, int offset, int length) throws IOException;

    /**
     * Read the specified number of bytes fully, from a given position within a file. This does not
     * change the current offset of a file, and is thread-safe.
     */
    default void preadFully(long position, byte[] buffer, int offset, int length)
            throws IOException {
        int readBytes = 0;
        while (readBytes < length) {
            int readBytesCurr = pread(position, buffer, offset + readBytes, length - readBytes);
            if (readBytesCurr < 0) {
                throw new EOFException(
                        String.format(
                                "Input Stream closed before all bytes were read."
                                        + " Expected %,d bytes but only read %,d bytes. Current position %,d",
                                length, readBytes, position));
            }
            readBytes += readBytesCurr;
            position += readBytesCurr;
        }
    }

    /** The smallest reasonable seek. */
    default int minSeekForVectorReads() {
        return 4 * 1024;
    }

    /** The largest size that we should group ranges together as. */
    default int maxReadSizeForVectorReads() {
        return 1024 * 1024;
    }

    /**
     * Read fully a list of file ranges asynchronously from this file.
     *
     * <p>As a result of the call, each range will have FileRange.setData(CompletableFuture) called
     * with a future that when complete will have a ByteBuffer with the data from the file's range.
     */
    default void readVectored(List<? extends FileRange> ranges, IntFunction<ByteBuffer> allocate)
            throws IOException {
        VectoredReadUtils.readVectored(this, ranges, allocate);
    }
}
