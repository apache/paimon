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

package org.apache.paimon.table.source;

import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataFileMetaSerializer;
import org.apache.paimon.io.DataInputDeserializer;
import org.apache.paimon.io.DataInputViewStreamWrapper;
import org.apache.paimon.io.DataOutputSerializer;

import org.apache.paimon.shade.guava30.com.google.common.base.FinalizablePhantomReference;
import org.apache.paimon.shade.guava30.com.google.common.base.FinalizableReferenceQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.io.RandomAccessFile;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.AbstractList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Driver-local, explicitly owned storage for replaying split metadata without retaining POJOs.
 *
 * <p>Append is single-threaded. After sealing, immutable slices support independent concurrent
 * readers. The owner must outlive every traversal and Task serialization; closing invalidates all
 * slices. Serialized splits contain the metadata itself, never these local file paths.
 */
public final class DataFileMetaStore implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(DataFileMetaStore.class);

    private static final int BUFFER_SIZE = 64 * 1024;

    private final long maxDiskBytes;
    private Path dataPath;
    private Path indexPath;
    private BufferedOutputStream dataOutput;
    private DataOutputStream indexOutput;
    private RandomAccessFile dataInput;
    private RandomAccessFile indexInput;
    private DataFileMetaSerializer writer = new DataFileMetaSerializer();
    private DataOutputSerializer encoded = new DataOutputSerializer(1024);
    private int count;
    private long dataBytes;
    private volatile boolean sealed;
    private volatile boolean closed;
    private volatile CleanupReference cleanupReference;

    public DataFileMetaStore(Path directory, long maxDiskBytes) throws IOException {
        if (maxDiskBytes < Long.BYTES) {
            throw new IllegalArgumentException("Metadata store limit must include an end offset.");
        }
        this.maxDiskBytes = maxDiskBytes;
        try {
            dataPath = Files.createTempFile(directory, "paimon-metadata-", ".data");
            indexPath = Files.createTempFile(directory, "paimon-metadata-", ".index");
            dataOutput = new BufferedOutputStream(Files.newOutputStream(dataPath), BUFFER_SIZE);
            indexOutput =
                    new DataOutputStream(
                            new BufferedOutputStream(
                                    Files.newOutputStream(indexPath), BUFFER_SIZE));
        } catch (IOException | RuntimeException | Error failure) {
            abort(failure);
            throw failure;
        }
    }

    public void append(DataFileMeta file) throws IOException {
        ensureOpen();
        if (sealed) {
            throw new IllegalStateException("Metadata store is sealed.");
        }
        try {
            if (count == Integer.MAX_VALUE) {
                throw new IOException("Metadata store exceeds the maximum List size.");
            }
            encoded.clear();
            writer.serialize(file, encoded);
            long nextDataBytes = Math.addExact(dataBytes, encoded.length());
            // Include this record's offset and the final end offset, even before sealing.
            long nextIndexBytes = ((long) count + 2) * Long.BYTES;
            if (nextIndexBytes > maxDiskBytes || nextDataBytes > maxDiskBytes - nextIndexBytes) {
                throw new IOException("Metadata store disk limit exceeded: " + maxDiskBytes);
            }
            dataOutput.write(encoded.getSharedBuffer(), 0, encoded.length());
            indexOutput.writeLong(dataBytes);
            dataBytes = nextDataBytes;
            count++;
        } catch (IOException | RuntimeException | Error failure) {
            abort(failure);
            throw failure;
        }
    }

    public void seal() throws IOException {
        ensureOpen();
        if (sealed) {
            return;
        }
        try {
            indexOutput.writeLong(dataBytes);
            dataOutput.close();
            indexOutput.close();
            dataInput = new RandomAccessFile(dataPath.toFile(), "r");
            indexInput = new RandomAccessFile(indexPath.toFile(), "r");
            dataOutput = null;
            indexOutput = null;
            writer = null;
            encoded = null;
            sealed = true;
        } catch (IOException | RuntimeException | Error failure) {
            abort(failure);
            throw failure;
        }
    }

    public List<DataFileMeta> slice(int fromInclusive, int toExclusive) {
        ensureReadable();
        checkRange(fromInclusive, toExclusive, count);
        return new Slice(this, fromInclusive, toExclusive - fromInclusive);
    }

    /** Encoded data and index bytes, including the reserved final offset, not filesystem blocks. */
    public long serializedBytes() {
        return dataBytes + ((long) count + 1) * Long.BYTES;
    }

    void requireFresh() {
        ensureOpen();
        if (sealed || count != 0) {
            throw new IllegalStateException("Metadata output store must be fresh and writable.");
        }
    }

    /**
     * Enables backup cleanup for a sealed plan whose lifetime follows its reachable slices.
     * Explicit owners should still close promptly. Collection and backup cleanup have no timing
     * guarantee; this is not a substitute for a disk budget or planning-failure cleanup.
     */
    public synchronized DataFileMetaStore withAutomaticCleanup() {
        ensureReadable();
        if (cleanupReference == null) {
            cleanupReference =
                    new CleanupReference(this, dataInput, indexInput, dataPath, indexPath);
        }
        return this;
    }

    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("Metadata store is closed.");
        }
    }

    private void ensureReadable() {
        ensureOpen();
        if (!sealed) {
            throw new IllegalStateException("Metadata store must be sealed before reading.");
        }
    }

    private long offset(int record) throws IOException {
        ByteBuffer offset = ByteBuffer.allocate(Long.BYTES);
        readFully(indexInput, offset.array(), (long) record * Long.BYTES);
        return offset.getLong(0);
    }

    private void checkOffsets(long from, long to) throws IOException {
        if (from < 0 || to <= from || to > dataBytes) {
            throw new IOException("Invalid metadata store offsets: " + from + ", " + to);
        }
    }

    private DataFileMeta get(int record) {
        ensureReadable();
        try {
            long from = offset(record);
            long to = offset(record + 1);
            checkOffsets(from, to);
            if (to - from > Integer.MAX_VALUE) {
                throw new IOException("Metadata record exceeds the maximum array size.");
            }
            byte[] bytes = new byte[(int) (to - from)];
            readFully(dataInput, bytes, from);
            return new DataFileMetaSerializer().deserialize(new DataInputDeserializer(bytes));
        } catch (IOException failure) {
            throw new UncheckedIOException(failure);
        } finally {
            keepAlive();
        }
    }

    private Iterator<DataFileMeta> iterator(int fromRecord, int size) {
        ensureReadable();
        if (size == 0) {
            return Collections.<DataFileMeta>emptyList().iterator();
        }
        try {
            long from = offset(fromRecord);
            long to = offset(fromRecord + size);
            checkOffsets(from, to);
            DataInputViewStreamWrapper input =
                    new DataInputViewStreamWrapper(
                            new BufferedInputStream(rangeInput(from, to), BUFFER_SIZE));
            return new Iterator<DataFileMeta>() {
                private final DataFileMetaSerializer reader = new DataFileMetaSerializer();
                private int remaining = size;

                @Override
                public boolean hasNext() {
                    ensureReadable();
                    return remaining > 0;
                }

                @Override
                public DataFileMeta next() {
                    if (!hasNext()) {
                        throw new NoSuchElementException();
                    }
                    try {
                        checkInterrupted();
                        DataFileMeta value = reader.deserialize(input);
                        checkInterrupted();
                        remaining--;
                        return value;
                    } catch (IOException failure) {
                        throw new UncheckedIOException(failure);
                    } finally {
                        keepAlive();
                    }
                }
            };
        } catch (IOException failure) {
            throw new UncheckedIOException(failure);
        }
    }

    private InputStream rangeInput(long from, long to) {
        // Iterators retain only their logical position and buffer, never an extra file handle.
        return new InputStream() {
            private long position = from;

            @Override
            public int read() throws IOException {
                byte[] one = new byte[1];
                return read(one, 0, 1) < 0 ? -1 : one[0] & 0xff;
            }

            @Override
            public int read(byte[] bytes, int offset, int length) throws IOException {
                ensureReadable();
                if (offset < 0 || length < 0 || offset > bytes.length - length) {
                    throw new IndexOutOfBoundsException();
                }
                if (length == 0) {
                    return 0;
                }
                if (position == to) {
                    return -1;
                }
                int requested = (int) Math.min(length, to - position);
                int read = readAt(dataInput, bytes, offset, requested, position);
                if (read <= 0) {
                    throw new EOFException("Incomplete metadata store data.");
                }
                position += read;
                return read;
            }
        };
    }

    private void readFully(RandomAccessFile input, byte[] bytes, long position) throws IOException {
        int offset = 0;
        while (offset < bytes.length) {
            int read = readAt(input, bytes, offset, bytes.length - offset, position + offset);
            if (read <= 0) {
                throw new EOFException("Incomplete metadata store record or index.");
            }
            offset += read;
        }
    }

    private synchronized int readAt(
            RandomAccessFile input, byte[] bytes, int offset, int length, long position)
            throws IOException {
        ensureReadable();
        checkInterrupted();
        // RAF reads cannot close shared handles on interruption. Serialize seeks with reads and
        // owner close; bound each read so cancellation is checked between chunks.
        input.seek(position);
        int read = input.read(bytes, offset, Math.min(length, BUFFER_SIZE));
        checkInterrupted();
        return read;
    }

    private static void checkInterrupted() throws InterruptedIOException {
        if (Thread.currentThread().isInterrupted()) {
            throw new InterruptedIOException("Metadata replay interrupted.");
        }
    }

    private void abort(Throwable failure) {
        try {
            close();
        } catch (IOException cleanupFailure) {
            failure.addSuppressed(cleanupFailure);
        }
    }

    private void keepAlive() {
        if (cleanupReference != null) {
            // JDK 8 has no reachabilityFence. Keep the owner alive until its last file I/O.
            synchronized (this) {
                // Acquiring the owner's monitor is the reachability barrier.
            }
        }
    }

    @Override
    public synchronized void close() throws IOException {
        closed = true;
        writer = null;
        encoded = null;
        if (cleanupReference != null) {
            cleanupReference.close();
            return;
        }
        closeResources(
                new Closeable[] {dataOutput, indexOutput, dataInput, indexInput},
                new Path[] {dataPath, indexPath});
    }

    private static void closeResources(Closeable[] resources, Path[] paths) throws IOException {
        IOException failure = null;
        for (Closeable resource : resources) {
            try {
                if (resource != null) {
                    resource.close();
                }
            } catch (IOException cleanupFailure) {
                if (failure == null) {
                    failure = cleanupFailure;
                } else {
                    failure.addSuppressed(cleanupFailure);
                }
            }
        }
        for (Path path : paths) {
            try {
                if (path != null) {
                    Files.deleteIfExists(path);
                }
            } catch (IOException cleanupFailure) {
                if (failure == null) {
                    failure = cleanupFailure;
                } else {
                    failure.addSuppressed(cleanupFailure);
                }
            }
        }
        if (failure != null) {
            throw failure;
        }
    }

    private static final class CleanupHolder {
        private static final Set<CleanupReference> REFERENCES = ConcurrentHashMap.newKeySet();
        private static final FinalizableReferenceQueue QUEUE = new FinalizableReferenceQueue();
    }

    /** Only independent resources may be retained here, never a store, slice or reader. */
    private static final class CleanupReference
            extends FinalizablePhantomReference<DataFileMetaStore> implements Closeable {
        private final Closeable[] resources;
        private final Path[] paths;
        private boolean cleaned;

        private CleanupReference(
                DataFileMetaStore owner,
                RandomAccessFile data,
                RandomAccessFile index,
                Path dataPath,
                Path indexPath) {
            super(owner, CleanupHolder.QUEUE);
            resources = new Closeable[] {data, index};
            paths = new Path[] {dataPath, indexPath};
            CleanupHolder.REFERENCES.add(this);
        }

        @Override
        public synchronized void close() throws IOException {
            if (!cleaned) {
                closeResources(resources, paths);
                cleaned = true;
                CleanupHolder.REFERENCES.remove(this);
                clear();
            }
        }

        @Override
        public void finalizeReferent() {
            try {
                close();
            } catch (IOException failure) {
                LOG.warn("Could not clean up unreachable metadata replay files.", failure);
            } finally {
                CleanupHolder.REFERENCES.remove(this);
                clear();
            }
        }
    }

    static List<DataFileMeta> retainOrCopy(List<DataFileMeta> files) {
        if (files instanceof Slice) {
            ((Slice) files).store.ensureReadable();
            return files;
        }
        return new java.util.ArrayList<>(files);
    }

    private static void checkRange(int from, int to, int size) {
        if (from < 0 || to < from || to > size) {
            throw new IndexOutOfBoundsException("Range " + from + ":" + to + ", size " + size);
        }
    }

    private static final class Slice extends AbstractList<DataFileMeta> {
        private final DataFileMetaStore store;
        private final int start;
        private final int size;

        private Slice(DataFileMetaStore store, int start, int size) {
            this.store = store;
            this.start = start;
            this.size = size;
        }

        @Override
        public int size() {
            return size;
        }

        @Override
        public DataFileMeta get(int index) {
            if (index < 0 || index >= size) {
                throw new IndexOutOfBoundsException("Index " + index + ", size " + size);
            }
            return store.get(start + index);
        }

        @Override
        public Iterator<DataFileMeta> iterator() {
            return store.iterator(start, size);
        }

        @Override
        public List<DataFileMeta> subList(int fromIndex, int toIndex) {
            checkRange(fromIndex, toIndex, size);
            return store.slice(start + fromIndex, start + toIndex);
        }
    }
}
