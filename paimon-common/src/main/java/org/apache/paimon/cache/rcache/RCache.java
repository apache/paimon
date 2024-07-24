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

package org.apache.paimon.cache.rcache;

import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.cache.BlockCache;
import org.apache.paimon.cache.BlockCacheConfig;
import org.apache.paimon.cache.CacheKey;
import org.apache.paimon.utils.ExecutorThreadFactory;
import org.apache.paimon.utils.FileIOUtils;
import org.apache.paimon.utils.Preconditions;

import org.apache.paimon.shade.caffeine2.com.github.benmanes.caffeine.cache.Cache;
import org.apache.paimon.shade.caffeine2.com.github.benmanes.caffeine.cache.Caffeine;
import org.apache.paimon.shade.caffeine2.com.github.benmanes.caffeine.cache.Policy;
import org.apache.paimon.shade.caffeine2.com.github.benmanes.caffeine.cache.RemovalCause;
import org.apache.paimon.shade.caffeine2.com.github.benmanes.caffeine.cache.stats.CacheStats;
import org.apache.paimon.shade.guava30.com.google.common.util.concurrent.MoreExecutors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * The rcache (read cache) currently only supports disk caching, consisting of two parts.
 * <li>The caffeine cache will maintain the CacheKey to the blockId mapping.
 * <li>The actual data will be stored in a local file.
 *
 *     <p>When the cache is full, the coldest block will be re-enqueued to the blockIds. This action
 *     ensures that the block in the file becomes available for caching incoming data.
 */
public class RCache implements BlockCache<CacheKey, byte[]> {

    private static final Logger LOG = LoggerFactory.getLogger(RCache.class);
    private static final String NAME = "rcache";
    private static final String CACHE_FILE_NAME = "paimon_rcache.bin";

    private static final Integer MB = 1024 * 1024;
    private static volatile RCache instance;
    private final BlockingQueue<Integer> blockIds;
    private final Cache<CacheKey, Integer> cache;
    private final ReentrantReadWriteLock lock;
    private final ScheduledExecutorService executorService;
    private final File cacheFile;
    private final FileChannel fileChannel;

    private RCache(BlockCacheConfig blockCacheConfig) {
        try {
            File cacheDir = new File(blockCacheConfig.localPath, NAME);
            if (cacheDir.exists()) {
                FileIOUtils.deleteFileOrDirectory(cacheDir);
                LOG.info("The cache file {} already exists, deleting", cacheDir.toPath());
            }
            this.cacheFile = new File(cacheDir, CACHE_FILE_NAME);
            Preconditions.checkArgument(cacheDir.mkdirs(), "Can not create cache dir %s", cacheDir);
            Preconditions.checkArgument(
                    cacheFile.createNewFile(), "Can not create cache file %s", cacheFile);
            this.fileChannel = new RandomAccessFile(cacheFile, "rw").getChannel();

            // Extra 1024 (1GB) block.
            int blockCount = (int) ((blockCacheConfig.disk.getBytes()) / MB) + 1024;
            this.blockIds = new ArrayBlockingQueue<>(blockCount);
            for (int i = 0; i < blockCount; i++) {
                blockIds.add(i);
            }
            this.lock = new ReentrantReadWriteLock();
            this.cache =
                    Caffeine.newBuilder()
                            .maximumSize(blockCount)
                            .removalListener(this::onRemoval)
                            .recordStats()
                            .executor(MoreExecutors.directExecutor())
                            .build();
            executorService =
                    Executors.newSingleThreadScheduledExecutor(
                            new ExecutorThreadFactory("rcache-statistics-thread"));
            executorService.scheduleAtFixedRate(this::printStatistics, 1, 2, TimeUnit.MINUTES);
            LOG.info("Init rcache with blockCount {}", blockCount);
        } catch (Exception e) {
            LOG.error("Failed to init rcache", e);
            throw new RuntimeException(e);
        }
    }

    private void onRemoval(CacheKey cacheKey, Integer value, RemovalCause removalCause) {
        // If explicit removed, the block will already be used. So we should not add it back.
        if (removalCause != RemovalCause.EXPLICIT) {
            try {
                lock.writeLock().lock();
                blockIds.add(value);
            } finally {
                lock.writeLock().unlock();
            }
        }
    }

    @Override
    public Optional<byte[]> read(CacheKey key) {
        try {
            lock.readLock().lock();
            Integer blockId = cache.getIfPresent(key);
            if (blockId != null) {
                byte[] values = new byte[MB];
                fileChannel.read(ByteBuffer.wrap(values), ((long) blockId) << 20);
                return Optional.of(values);
            } else {
                return Optional.empty();
            }
        } catch (Throwable t) {
            LOG.error("Failed to read from cache", t);
            throw new RuntimeException(t);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void write(CacheKey key, byte[] value) {
        Integer id = null;
        Throwable t = null;
        try {
            // Get the next free block.
            lock.writeLock().lock();
            id = blockIds.poll();
            if (id == null) {
                Policy.Eviction<CacheKey, Integer> evictions = cache.policy().eviction().get();
                // This can be cost, so we have extra 1024 block to avoid get the coldest item
                // frequently.
                Map<CacheKey, Integer> toEvict = evictions.coldest(1);
                Preconditions.checkArgument(toEvict.size() == 1);
                Map.Entry<CacheKey, Integer> item = toEvict.entrySet().iterator().next();
                id = item.getValue();
                toEvict.forEach((k, v) -> cache.invalidate(item.getKey()));
            }
        } catch (Throwable e) {
            t = e;
            throw new RuntimeException(e);
        } finally {
            if (t != null && id != null) {
                blockIds.add(id);
            }
            lock.writeLock().unlock();
        }

        try {
            Preconditions.checkNotNull(id, "The block id should not be null");
            fileChannel.write(ByteBuffer.wrap(value), ((long) id) << 20);
            cache.put(key, id);
        } catch (Exception e) {
            blockIds.add(id);
            throw new RuntimeException(e);
        }
    }

    @Override
    public void printStatistics() {
        CacheStats stats = cache.stats();
        LOG.info(
                "hit rate: {}, eviction count: {}, hit count {}, request count {} ",
                stats.hitRate(),
                stats.evictionCount(),
                stats.hitCount(),
                stats.requestCount());
    }

    public static RCache getCache(BlockCacheConfig blockCacheConfig) {
        if (instance == null) {
            synchronized (RCache.class) {
                if (instance == null) {
                    instance = new RCache(blockCacheConfig);
                }
            }
        }
        return instance;
    }

    @VisibleForTesting
    public Cache<CacheKey, Integer> getCache() {
        return cache;
    }

    @VisibleForTesting
    public File getCacheFile() {
        return cacheFile;
    }
}
