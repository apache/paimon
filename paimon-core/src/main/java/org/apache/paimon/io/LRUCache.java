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

import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.utils.IOUtils;
import org.apache.paimon.utils.ThreadPoolUtils;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.io.IOException;
import java.io.Serializable;
import java.io.UncheckedIOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

/** LRU cache implementation for local file caching. */
public class LRUCache implements Serializable {

    private static final long serialVersionUID = 1L;

    private final Map<Path, Node> fileMap = new ConcurrentHashMap<>();
    private final LRUList lru = new LRUList();
    private final ExecutorService executor =
            ThreadPoolUtils.createCachedThreadPool(
                    Runtime.getRuntime().availableProcessors(), "LRU-CACHE-FLUSH");

    private final LocalFileIO fileIO;

    public LRUCache(LocalFileIO fileIO) {
        this.fileIO = fileIO;
    }

    public synchronized void put(Path path, Path cachePath, boolean onRemote) {
        Node node = new Node(path, cachePath);
        if (onRemote) {
            long fileSize;
            try {
                fileSize = fileIO.getFileSize(cachePath);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            node.fileSize = fileSize;
            node.onRemote = onRemote;
        }

        fileMap.put(path, node);
        // always put new value because data file won't be overwritten
        lru.addToHead(node);
    }

    @Nullable
    public synchronized Node get(Path path, boolean refreshLru) {
        Node node = fileMap.get(path);
        if (node != null && refreshLru) {
            lru.moveToHead(node);
        }
        return node;
    }

    public synchronized Node invalidate(Path path) {
        Node node = fileMap.get(path);
        if (node == null) {
            return null;
        }

        try {
            fileIO.delete(node.cachePath, false);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        fileMap.remove(path);
        lru.remove(node);
        return node;
    }

    public synchronized void expire(long remainSize) {
        long cacheSize = size();
        while (cacheSize > remainSize) {
            if (lru.isEmpty()) {
                return;
            }
            Node node = lru.tailNode();
            invalidate(node.path);

            cacheSize -= node.fileSize;
        }
    }

    public synchronized long size() {
        long size = 0;
        for (Node node : fileMap.values()) {
            node.setSizeIfNeeded(fileIO);
            size += node.fileSize;
        }
        return size;
    }

    public synchronized void flush(FileIO targetFileIO) throws IOException {
        List<Map.Entry<Path, Node>> toFlush =
                fileMap.entrySet().stream()
                        .filter(entry -> !entry.getValue().onRemote())
                        .collect(Collectors.toList());

        ThreadPoolUtils.randomlyOnlyExecute(
                executor,
                entry -> {
                    Node node = entry.getValue();
                    try {
                        IOUtils.copyBytes(
                                fileIO.newInputStream(node.cachePath),
                                targetFileIO.newOutputStream(entry.getKey(), false));
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                    node.setOnRemote(true);
                },
                toFlush);
    }

    public synchronized void flushFile(Path path, FileIO targetFileIO) throws IOException {
        Node node = get(path, true);
        if (node != null && !node.onRemote()) {
            IOUtils.copyBytes(
                    fileIO.newInputStream(node.cachePath),
                    targetFileIO.newOutputStream(path, false));
            node.setOnRemote(true);
        }
    }

    @VisibleForTesting
    synchronized Iterable<Node> nodesInOrder() {
        return () ->
                new Iterator<Node>() {
                    private Node current = lru.head.next;

                    @Override
                    public boolean hasNext() {
                        return current != lru.tail;
                    }

                    @Override
                    public Node next() {
                        if (!hasNext()) {
                            throw new NoSuchElementException();
                        }
                        Node node = current;
                        current = current.next;
                        return node;
                    }
                };
    }

    /** Node class for LRU cache. */
    public static class Node implements Serializable {

        private static final long serialVersionUID = 1L;

        private final Path path;
        private final Path cachePath;

        private long fileSize = -1;
        private boolean onRemote = false;

        private Node prev, next;

        public Node(Path path, Path cachePath) {
            this.path = path;
            this.cachePath = cachePath;
        }

        public void setSizeIfNeeded(FileIO fileIO) {
            if (fileSize < 0) {
                try {
                    fileSize = fileIO.getFileSize(cachePath);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }

        public Path cachePath() {
            return cachePath;
        }

        public Path path() {
            return path;
        }

        public boolean onRemote() {
            return onRemote;
        }

        public void setOnRemote(boolean onRemote) {
            this.onRemote = onRemote;
        }
    }

    /** LRU list implementation. */
    private static class LRUList implements Serializable {

        private static final long serialVersionUID = 1L;

        private final Node head = new Node(null, null);
        private final Node tail = new Node(null, null);

        LRUList() {
            head.next = tail;
            tail.prev = head;
        }

        void addToHead(Node newNode) {
            newNode.next = head.next;
            newNode.prev = head;
            head.next.prev = newNode;
            head.next = newNode;
        }

        void moveToHead(Node oldNode) {
            if (oldNode.prev == head) {
                return;
            }
            remove(oldNode);
            addToHead(oldNode);
        }

        void remove(Node oldNode) {
            oldNode.prev.next = oldNode.next;
            oldNode.next.prev = oldNode.prev;
        }

        boolean isEmpty() {
            return head.next == tail;
        }

        Node tailNode() {
            return tail.prev;
        }
    }
}
