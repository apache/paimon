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

package org.apache.paimon.fs.cache;

import org.apache.paimon.annotation.VisibleForTesting;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/** Block-level local disk cache with LRU eviction. Thread-safe. */
public class BlockDiskCache implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(BlockDiskCache.class);

    private static final Map<CacheKey, BlockDiskCache> SHARED_CACHES = new ConcurrentHashMap<>();

    private final File cacheDir;
    private final long maxSizeBytes;
    private final int blockSize;
    private final Object lock = new Object();
    private long currentSize;

    public BlockDiskCache(String cacheDir, long maxSizeBytes, int blockSize) {
        this.cacheDir = new File(cacheDir);
        this.maxSizeBytes = maxSizeBytes;
        this.blockSize = blockSize;
        this.cacheDir.mkdirs();
        this.currentSize = scanSize();
    }

    /** Returns a shared instance for the given parameters, creating one if needed. */
    public static BlockDiskCache getOrCreate(String cacheDir, long maxSizeBytes, int blockSize) {
        CacheKey key = new CacheKey(cacheDir, maxSizeBytes, blockSize);
        return SHARED_CACHES.computeIfAbsent(
                key, k -> new BlockDiskCache(cacheDir, maxSizeBytes, blockSize));
    }

    public int blockSize() {
        return blockSize;
    }

    public byte[] getBlock(String filePath, int blockIndex) {
        File path = cachePath(filePath, blockIndex);
        if (!path.exists()) {
            return null;
        }
        try {
            byte[] data = Files.readAllBytes(path.toPath());
            // touch to update mtime for LRU
            path.setLastModified(System.currentTimeMillis());
            return data;
        } catch (IOException e) {
            LOG.debug("Failed to read cache block: {}", path, e);
            return null;
        }
    }

    public void putBlock(String filePath, int blockIndex, byte[] data) {
        File path = cachePath(filePath, blockIndex);
        if (path.exists()) {
            return;
        }

        File subDir = path.getParentFile();
        subDir.mkdirs();

        File tmpFile =
                new File(
                        path.getParent(),
                        path.getName() + ".tmp." + Thread.currentThread().getId());
        try {
            try (FileOutputStream fos = new FileOutputStream(tmpFile)) {
                fos.write(data);
            }
            if (!tmpFile.renameTo(path)) {
                tmpFile.delete();
                // another thread won the race — don't update currentSize
                return;
            }
        } catch (IOException e) {
            tmpFile.delete();
            LOG.debug("Failed to write cache block: {}", path, e);
            return;
        }

        boolean needEvict = false;
        synchronized (lock) {
            currentSize += data.length;
            needEvict = maxSizeBytes < Long.MAX_VALUE && currentSize > maxSizeBytes;
        }
        if (needEvict) {
            evict();
        }
    }

    private void evict() {
        List<CacheEntry> entries = new ArrayList<>();
        File[] prefixDirs = cacheDir.listFiles();
        if (prefixDirs == null) {
            return;
        }
        for (File prefixDir : prefixDirs) {
            if (!prefixDir.isDirectory()) {
                continue;
            }
            File[] files = prefixDir.listFiles();
            if (files == null) {
                continue;
            }
            for (File file : files) {
                if (file.getName().contains(".tmp.")) {
                    continue;
                }
                entries.add(new CacheEntry(file.lastModified(), file.length(), file));
            }
        }

        entries.sort(Comparator.comparingLong(e -> e.mtime));

        List<CacheEntry> toDelete = new ArrayList<>();
        synchronized (lock) {
            for (CacheEntry entry : entries) {
                if (currentSize <= maxSizeBytes) {
                    break;
                }
                toDelete.add(entry);
                currentSize -= entry.size;
            }
        }
        for (CacheEntry entry : toDelete) {
            if (!entry.file.delete()) {
                synchronized (lock) {
                    currentSize += entry.size;
                }
            }
        }
    }

    private long scanSize() {
        long total = 0;
        try {
            Path root = cacheDir.toPath();
            if (!Files.exists(root)) {
                return 0;
            }
            total = scanDirectory(root);
        } catch (IOException e) {
            LOG.warn("Failed to scan cache directory size", e);
        }
        return total;
    }

    private long scanDirectory(Path root) throws IOException {
        long[] total = {0};
        Files.walkFileTree(
                root,
                new SimpleFileVisitor<Path>() {
                    @Override
                    public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
                        if (!file.getFileName().toString().contains(".tmp.")) {
                            total[0] += attrs.size();
                        }
                        return FileVisitResult.CONTINUE;
                    }
                });
        return total[0];
    }

    private File cachePath(String filePath, int blockIndex) {
        String key = filePath + ":" + blockIndex;
        String hex = sha256Hex(key);
        String prefix = hex.substring(0, 2);
        return new File(new File(cacheDir, prefix), hex);
    }

    private static final ThreadLocal<MessageDigest> SHA256_DIGEST =
            ThreadLocal.withInitial(
                    () -> {
                        try {
                            return MessageDigest.getInstance("SHA-256");
                        } catch (NoSuchAlgorithmException e) {
                            throw new RuntimeException("SHA-256 not available", e);
                        }
                    });

    private static final char[] HEX_CHARS = "0123456789abcdef".toCharArray();

    private static String sha256Hex(String input) {
        MessageDigest md = SHA256_DIGEST.get();
        md.reset();
        byte[] hash = md.digest(input.getBytes(java.nio.charset.StandardCharsets.UTF_8));
        char[] chars = new char[hash.length * 2];
        for (int i = 0; i < hash.length; i++) {
            chars[i * 2] = HEX_CHARS[(hash[i] >> 4) & 0x0f];
            chars[i * 2 + 1] = HEX_CHARS[hash[i] & 0x0f];
        }
        return new String(chars);
    }

    @VisibleForTesting
    long currentSize() {
        synchronized (lock) {
            return currentSize;
        }
    }

    @Override
    public void close() {}

    private static class CacheEntry {
        final long mtime;
        final long size;
        final File file;

        CacheEntry(long mtime, long size, File file) {
            this.mtime = mtime;
            this.size = size;
            this.file = file;
        }
    }

    private static class CacheKey {
        final String cacheDir;
        final long maxSizeBytes;
        final int blockSize;

        CacheKey(String cacheDir, long maxSizeBytes, int blockSize) {
            this.cacheDir = cacheDir;
            this.maxSizeBytes = maxSizeBytes;
            this.blockSize = blockSize;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof CacheKey)) {
                return false;
            }
            CacheKey that = (CacheKey) o;
            return maxSizeBytes == that.maxSizeBytes
                    && blockSize == that.blockSize
                    && Objects.equals(cacheDir, that.cacheDir);
        }

        @Override
        public int hashCode() {
            return Objects.hash(cacheDir, maxSizeBytes, blockSize);
        }
    }
}
