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

package org.apache.paimon.lookup.sort;

import org.apache.paimon.io.cache.CacheKey;
import org.apache.paimon.io.cache.CacheManager;
import org.apache.paimon.io.cache.CacheManager.SegmentContainer;
import org.apache.paimon.memory.MemorySegment;

import java.io.Closeable;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

/** Cache for block reading. */
public class BlockCache implements Closeable {

    private final RandomAccessFile file;
    private final FileChannel channel;
    private final CacheManager cacheManager;
    private final Map<CacheKey, SegmentContainer> blocks;

    public BlockCache(RandomAccessFile file, CacheManager cacheManager) {
        this.file = file;
        this.channel = this.file.getChannel();
        this.cacheManager = cacheManager;
        this.blocks = new HashMap<>();
    }

    private byte[] readFrom(long offset, int length) throws IOException {
        byte[] buffer = new byte[length];
        int read = channel.read(ByteBuffer.wrap(buffer), offset);

        if (read != length) {
            throw new IOException("Could not read all the data");
        }
        return buffer;
    }

    public MemorySegment getBlock(
            long position, int length, Function<byte[], byte[]> decompressFunc, boolean isIndex) {

        CacheKey cacheKey = CacheKey.forPosition(file, position, length, isIndex);

        SegmentContainer container = blocks.get(cacheKey);
        if (container == null || container.getAccessCount() == CacheManager.REFRESH_COUNT) {
            MemorySegment segment =
                    cacheManager.getPage(
                            cacheKey,
                            key -> {
                                byte[] bytes = readFrom(position, length);
                                return decompressFunc.apply(bytes);
                            },
                            blocks::remove);
            container = new SegmentContainer(segment);
            blocks.put(cacheKey, container);
        }
        return container.access();
    }

    @Override
    public void close() throws IOException {
        Set<CacheKey> sets = new HashSet<>(blocks.keySet());
        for (CacheKey key : sets) {
            cacheManager.invalidPage(key);
        }
    }
}
