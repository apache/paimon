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

package org.apache.paimon.io;

import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.StringUtils;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link LRUCache}. */
public class LRUCacheTest {

    @TempDir private java.nio.file.Path tempDir;
    private LRUCache cache;
    private LocalFileIO fileIO;

    @BeforeEach
    public void setUp() {
        fileIO = LocalFileIO.create();
        cache = new LRUCache(fileIO);
    }

    @Test
    public void testPutAndGet() {
        Path path = new Path(tempDir.toString(), "test/path");
        Path cachePath = new Path(tempDir.toString(), "cache/path");

        // Put a node into the cache
        cache.put(path, cachePath, false);

        // Get the node from the cache
        LRUCache.Node node = cache.get(path, true);
        assertThat(node).isNotNull();
        assertThat(node.cachePath()).isEqualTo(cachePath);
        assertThat(node.onRemote()).isFalse();
    }

    @Test
    public void testInvalidate() {
        Path path = new Path(tempDir.toString(), "test/path");
        Path cachePath = new Path(tempDir.toString(), "cache/path");

        // Put a node into the cache
        cache.put(path, cachePath, false);

        // Invalidate the node
        LRUCache.Node node = cache.invalidate(path);
        assertThat(node).isNotNull();
        assertThat(node.cachePath()).isEqualTo(cachePath);

        // Try to get the node again, should return null
        LRUCache.Node node2 = cache.get(path, true);
        assertThat(node2).isNull();
    }

    @Test
    public void testFlush() throws IOException {
        // Create a mock target FileIO
        LocalFileIO targetFileIO = LocalFileIO.create();

        // Create multiple cache files
        Path cacheFile1 = writeCacheFile().getLeft();
        Path cacheFile2 = writeCacheFile().getLeft();
        Path cacheFile3 = writeCacheFile().getLeft();

        // Create remote files
        Path remoteFile1 = new Path(tempDir.toString(), "remote/file1");
        Path remoteFile2 = new Path(tempDir.toString(), "remote/file2");
        Path remoteFile3 = new Path(tempDir.toString(), "remote/file3");

        // Add nodes to cache with onRemote = false
        cache.put(remoteFile1, cacheFile1, false);
        cache.put(remoteFile2, cacheFile2, false);
        cache.put(remoteFile3, cacheFile3, false);

        // Verify all nodes are not on remote
        assertThat(targetFileIO.exists(remoteFile1)).isFalse();
        assertThat(targetFileIO.exists(remoteFile2)).isFalse();
        assertThat(targetFileIO.exists(remoteFile3)).isFalse();

        // Flush the cache using thread pool
        cache.flush(targetFileIO);

        // Verify all nodes are now on remote
        assertThat(cache.get(remoteFile1, true).onRemote()).isTrue();
        assertThat(cache.get(remoteFile2, true).onRemote()).isTrue();
        assertThat(cache.get(remoteFile3, true).onRemote()).isTrue();

        // Verify remote files exist
        assertThat(targetFileIO.exists(remoteFile1)).isTrue();
        assertThat(targetFileIO.exists(remoteFile2)).isTrue();
        assertThat(targetFileIO.exists(remoteFile3)).isTrue();
    }

    @Test
    public void testExpire() throws IOException {
        // Create cache files first since onRemote=true requires real files
        Pair<Path, Long> cacheFile1 = writeCacheFile();
        Pair<Path, Long> cacheFile2 = writeCacheFile();
        Pair<Path, Long> cacheFile3 = writeCacheFile();

        // remote file paths
        Path path1 = new Path(tempDir.toString(), "test/path1");
        Path path2 = new Path(tempDir.toString(), "test/path2");
        Path path3 = new Path(tempDir.toString(), "test/path3");

        // Add to the cache
        cache.put(path1, cacheFile1.getLeft(), true);
        cache.put(path2, cacheFile2.getLeft(), true);
        cache.put(path3, cacheFile3.getLeft(), true);

        // Test expire with 0 size, should remove all nodes and files
        cache.expire(0);

        // All nodes should be removed
        assertThat(cache.get(path1, true)).isNull();
        assertThat(cache.get(path2, true)).isNull();
        assertThat(cache.get(path3, true)).isNull();

        // All cache files should also be deleted
        assertThat(fileIO.exists(cacheFile1.getLeft())).isFalse();
        assertThat(fileIO.exists(cacheFile2.getLeft())).isFalse();
        assertThat(fileIO.exists(cacheFile3.getLeft())).isFalse();

        // Re-create cache files for next test
        cacheFile1 = writeCacheFile();
        cacheFile2 = writeCacheFile();
        cacheFile3 = writeCacheFile();

        cache.put(path1, cacheFile1.getLeft(), true);
        cache.put(path2, cacheFile2.getLeft(), true);
        cache.put(path3, cacheFile3.getLeft(), true);

        // In this case, file1 will be expired
        long boundarySize =
                cacheFile1.getRight() + cacheFile2.getRight() + cacheFile3.getRight() - 1;
        cache.expire(boundarySize);

        assertThat(cache.get(path1, true)).isNull();
        assertThat(cache.get(path2, true)).isNotNull();
        assertThat(cache.get(path3, true)).isNotNull();

        assertThat(fileIO.exists(cacheFile1.getLeft())).isFalse();
        assertThat(fileIO.exists(cacheFile2.getLeft())).isTrue();
        assertThat(fileIO.exists(cacheFile3.getLeft())).isTrue();

        // Re-create cache files for next test
        cacheFile1 = writeCacheFile();
        cacheFile2 = writeCacheFile();
        cacheFile3 = writeCacheFile();

        cache.put(path1, cacheFile1.getLeft(), true);
        cache.put(path2, cacheFile2.getLeft(), true);
        cache.put(path3, cacheFile3.getLeft(), true);

        // Test with large size, should keep all nodes
        long largeSize = cacheFile1.getRight() + cacheFile2.getRight() + cacheFile3.getRight() + 1;
        cache.expire(largeSize);

        // All nodes should remain
        assertThat(cache.get(path1, true)).isNotNull();
        assertThat(cache.get(path2, true)).isNotNull();
        assertThat(cache.get(path3, true)).isNotNull();

        // Cache files should still exist
        assertThat(fileIO.exists(cacheFile1.getLeft())).isTrue();
        assertThat(fileIO.exists(cacheFile2.getLeft())).isTrue();
        assertThat(fileIO.exists(cacheFile3.getLeft())).isTrue();
    }

    @Test
    public void testSize() throws IOException {
        // Size should be 0 initially
        assertThat(cache.size()).isEqualTo(0);

        // Create cache file
        Pair<Path, Long> cacheFile = writeCacheFile();

        Path path = new Path(tempDir.toString(), "test/path");

        // Put a node into the cache
        cache.put(path, cacheFile.getLeft(), false);

        // Size should equal the cache file size
        assertThat(cache.size()).isEqualTo(cacheFile.getRight());
    }

    @Test
    public void testLRUNodesOrder() throws IOException {
        // Put initial nodes
        Pair<Path, Long> cacheFile1 = writeCacheFile();
        Pair<Path, Long> cacheFile2 = writeCacheFile();
        Pair<Path, Long> cacheFile3 = writeCacheFile();

        Path path1 = new Path(tempDir.toString(), "test/path1");
        Path path2 = new Path(tempDir.toString(), "test/path2");
        Path path3 = new Path(tempDir.toString(), "test/path3");

        cache.put(path1, cacheFile1.getLeft(), false);
        cache.put(path2, cacheFile2.getLeft(), false);
        cache.put(path3, cacheFile3.getLeft(), false);

        // Check initial LRU order (most recent first): path3, path2, path1
        List<LRUCache.Node> initialOrder = new ArrayList<>();
        for (LRUCache.Node node : cache.nodesInOrder()) {
            initialOrder.add(node);
        }
        assertThat(initialOrder).hasSize(3);
        assertThat(initialOrder.get(0).path()).isEqualTo(path3);
        assertThat(initialOrder.get(1).path()).isEqualTo(path2);
        assertThat(initialOrder.get(2).path()).isEqualTo(path1);

        // Access the first node to move it to the front
        cache.get(path1, true);

        // Check LRU order after accessing path1: path1, path3, path2
        List<LRUCache.Node> orderAfterAccess1 = new ArrayList<>();
        for (LRUCache.Node node : cache.nodesInOrder()) {
            orderAfterAccess1.add(node);
        }
        assertThat(orderAfterAccess1).hasSize(3);
        assertThat(orderAfterAccess1.get(0).path()).isEqualTo(path1);
        assertThat(orderAfterAccess1.get(1).path()).isEqualTo(path3);
        assertThat(orderAfterAccess1.get(2).path()).isEqualTo(path2);

        // Access the second node
        cache.get(path2, true);

        // Check LRU order after accessing path2: path2, path1, path3
        List<LRUCache.Node> orderAfterAccess2 = new ArrayList<>();
        for (LRUCache.Node node : cache.nodesInOrder()) {
            orderAfterAccess2.add(node);
        }
        assertThat(orderAfterAccess2).hasSize(3);
        assertThat(orderAfterAccess2.get(0).path()).isEqualTo(path2);
        assertThat(orderAfterAccess2.get(1).path()).isEqualTo(path1);
        assertThat(orderAfterAccess2.get(2).path()).isEqualTo(path3);

        // Add another node (create a fourth file)
        Pair<Path, Long> cacheFile4 = writeCacheFile();
        Path path4 = new Path(tempDir.toString(), "test/path4");
        cache.put(path4, cacheFile4.getLeft(), false);

        // Check LRU order after adding path4: path4, path2, path1, path3
        List<LRUCache.Node> orderAfterAdding4 = new ArrayList<>();
        for (LRUCache.Node node : cache.nodesInOrder()) {
            orderAfterAdding4.add(node);
        }
        assertThat(orderAfterAdding4).hasSize(4);
        assertThat(orderAfterAdding4.get(0).path()).isEqualTo(path4);
        assertThat(orderAfterAdding4.get(1).path()).isEqualTo(path2);
        assertThat(orderAfterAdding4.get(2).path()).isEqualTo(path1);
        assertThat(orderAfterAdding4.get(3).path()).isEqualTo(path3);

        // Test LRU order - expire to keep only 3 nodes
        // Should remove the least recently used (path3)
        cache.expire(cacheFile4.getRight() + cacheFile2.getRight() + cacheFile1.getRight());

        // Check LRU order after expiration: path4, path2, path1
        List<LRUCache.Node> orderAfterExpiration = new ArrayList<>();
        for (LRUCache.Node node : cache.nodesInOrder()) {
            orderAfterExpiration.add(node);
        }
        assertThat(orderAfterExpiration).hasSize(3);
        assertThat(orderAfterExpiration.get(0).path()).isEqualTo(path4);
        assertThat(orderAfterExpiration.get(1).path()).isEqualTo(path2);
        assertThat(orderAfterExpiration.get(2).path()).isEqualTo(path1);
    }

    private Pair<Path, Long> writeCacheFile() throws IOException {
        Path cachePath = new Path(tempDir.toString(), "cache/" + UUID.randomUUID());
        try (PositionOutputStream out = fileIO.newOutputStream(cachePath, false)) {
            out.write(
                    StringUtils.getRandomString(ThreadLocalRandom.current(), 10, 1000).getBytes());
        }
        return Pair.of(cachePath, fileIO.getFileSize(cachePath));
    }
}
