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

package org.apache.paimon.sst;

import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.fs.VectoredReadable;
import org.apache.paimon.io.cache.CacheKey;
import org.apache.paimon.io.cache.CacheManager;
import org.apache.paimon.memory.MemorySegment;
import org.apache.paimon.memory.MemorySlice;
import org.apache.paimon.utils.IOUtils;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.io.IOException;
import java.util.function.Function;

/** Cache for block reading. */
public class BlockCache implements Closeable {

    private final Path filePath;
    private final SeekableInputStream input;
    private final CacheManager cacheManager;

    public BlockCache(Path filePath, SeekableInputStream input, CacheManager cacheManager) {
        this.filePath = filePath;
        this.input = input;
        this.cacheManager = cacheManager;
    }

    private byte[] readFrom(long offset, int length) throws IOException {
        byte[] buffer = new byte[length];
        if (input instanceof VectoredReadable) {
            ((VectoredReadable) input).preadFully(offset, buffer, 0, length);
        } else {
            synchronized (input) {
                input.seek(offset);
                IOUtils.readFully(input, buffer);
            }
        }
        return buffer;
    }

    public MemorySegment getBlock(
            long position, int length, Function<byte[], byte[]> decompressFunc, boolean isIndex) {
        CacheKey cacheKey = CacheKey.forPosition(filePath, position, length, isIndex);

        // Construct the capturing loader only on misses; hits use the shared cache directly.
        MemorySegment cached = cacheManager.getPageIfPresent(cacheKey);
        if (cached != null) {
            return cached;
        }
        return cacheManager.getPage(
                cacheKey,
                key -> {
                    byte[] bytes = readFrom(position, length);
                    return decompressFunc.apply(bytes);
                });
    }

    /** Returns a decoded block if resident, without reading the file. */
    @Nullable
    public MemorySlice getBlockSliceIfPresent(long position, int length, boolean isIndex) {
        return cacheManager.getPageSliceIfPresent(
                CacheKey.forPosition(filePath, position, length, isIndex));
    }

    /**
     * Reads and caches a decoded block without copying an uncompressed payload out of its buffer.
     */
    public MemorySlice getBlockSlice(
            long position, int length, Function<byte[], MemorySlice> decoder, boolean isIndex) {
        CacheKey cacheKey = CacheKey.forPosition(filePath, position, length, isIndex);
        MemorySlice cached = cacheManager.getPageSliceIfPresent(cacheKey);
        if (cached != null) {
            return cached;
        }
        return cacheManager.getPageSlice(cacheKey, key -> readFrom(position, length), decoder);
    }

    @Override
    public void close() throws IOException {
        cacheManager.invalidFile(filePath);
    }
}
