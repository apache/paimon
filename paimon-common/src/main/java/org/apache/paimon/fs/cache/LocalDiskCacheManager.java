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
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/** Block-level local disk cache with LRU eviction. Thread-safe. */
public class LocalDiskCacheManager implements LocalCacheManager {

    private static final Logger LOG = LoggerFactory.getLogger(LocalDiskCacheManager.class);

    private final File cacheDir;
    private final long maxSizeBytes;
    private final int blockSize;
    private final Object lock = new Object();
    private final ConcurrentHashMap<String, Long> fileSizeCache = new ConcurrentHashMap<>();

    // LRU-ordered index: key -> size. Access order so get() moves entry to tail.
    private final LinkedHashMap<String, Long> entryIndex;
    private long currentSize;

    public LocalDiskCacheManager(String cacheDir, long maxSizeBytes, int blockSize) {
        this.cacheDir = new File(cacheDir);
        this.maxSizeBytes = maxSizeBytes;
        this.blockSize = blockSize;
        this.entryIndex = new LinkedHashMap<>(64, 0.75f, true);
        this.cacheDir.mkdirs();
        this.currentSize = scanAndPopulateIndex();
    }

    public int blockSize() {
        return blockSize;
    }

    public byte[] getBlock(String filePath, int blockIndex) {
        File path = cachePath(filePath, blockIndex);
        String cacheKey = path.getPath();
        synchronized (lock) {
            if (!entryIndex.containsKey(cacheKey)) {
                return null;
            }
            // access to update LRU order
            entryIndex.get(cacheKey);
        }
        try {
            return Files.readAllBytes(path.toPath());
        } catch (IOException e) {
            LOG.debug("Failed to read cache block: {}", path, e);
            synchronized (lock) {
                Long size = entryIndex.remove(cacheKey);
                if (size != null) {
                    currentSize -= size;
                }
            }
            return null;
        }
    }

    public void putBlock(String filePath, int blockIndex, byte[] data) {
        File path = cachePath(filePath, blockIndex);
        String cacheKey = path.getPath();

        synchronized (lock) {
            if (entryIndex.containsKey(cacheKey)) {
                return;
            }
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
                return;
            }
        } catch (IOException e) {
            tmpFile.delete();
            LOG.debug("Failed to write cache block: {}", path, e);
            return;
        }

        boolean needEvict = false;
        synchronized (lock) {
            entryIndex.put(cacheKey, (long) data.length);
            currentSize += data.length;
            needEvict = maxSizeBytes < Long.MAX_VALUE && currentSize > maxSizeBytes;
        }
        if (needEvict) {
            evict();
        }
    }

    private void evict() {
        List<Map.Entry<String, Long>> toDelete = new ArrayList<>();
        synchronized (lock) {
            if (currentSize <= maxSizeBytes) {
                return;
            }
            Iterator<Map.Entry<String, Long>> it = entryIndex.entrySet().iterator();
            while (it.hasNext() && currentSize > maxSizeBytes) {
                Map.Entry<String, Long> entry = it.next();
                toDelete.add(entry);
                currentSize -= entry.getValue();
                it.remove();
            }
        }
        for (Map.Entry<String, Long> entry : toDelete) {
            if (!new File(entry.getKey()).delete()) {
                synchronized (lock) {
                    entryIndex.put(entry.getKey(), entry.getValue());
                    currentSize += entry.getValue();
                }
            }
        }
    }

    private long scanAndPopulateIndex() {
        long total = 0;
        try {
            Path root = cacheDir.toPath();
            if (!Files.exists(root)) {
                return 0;
            }
            Files.walkFileTree(
                    root,
                    new SimpleFileVisitor<Path>() {
                        @Override
                        public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
                            String name = file.getFileName().toString();
                            if (!name.contains(".tmp.")) {
                                entryIndex.put(file.toFile().getPath(), attrs.size());
                            }
                            return FileVisitResult.CONTINUE;
                        }
                    });
            for (Long size : entryIndex.values()) {
                total += size;
            }
        } catch (IOException e) {
            LOG.warn("Failed to scan cache directory", e);
        }
        return total;
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
    public long getFileSize(String filePath) {
        Long size = fileSizeCache.get(filePath);
        return size != null ? size : -1;
    }

    @Override
    public void putFileSize(String filePath, long size) {
        fileSizeCache.put(filePath, size);
    }
}
