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

package org.apache.paimon.index;

import org.apache.paimon.Snapshot;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.io.cache.CacheManager;
import org.apache.paimon.lookup.sort.SortLookupStoreReader;
import org.apache.paimon.lookup.sort.SortLookupStoreWriter;
import org.apache.paimon.memory.MemorySlice;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.sst.BlockIterator;
import org.apache.paimon.sst.SstFileReader;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Comparator;
import java.util.Map;

import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.paimon.utils.Preconditions.checkState;

/**
 * Immutable bucket-local SST segments of {@link DeleteOrderingGuard}s.
 *
 * <p>A segment starts with magic, format version, and table schema id, followed by the native SST
 * blocks and footer. Keys consist of primary-key fields and an ordinal; values contain internal
 * sequence number, retract kind, input level, and projected ordering fields. Native SST blocks
 * provide checksums and a block index. Reads stream the segment for merge processing, with no
 * per-event guard lookup or ingestion-time rewrite of previous segments.
 *
 * <p>The path factory defines the bucket scope. Callers must commit returned index references
 * atomically with the corresponding data changes. Finishing a writer only produces an uncommitted
 * file; it does not publish ordering protection. No bound is retired by this storage layer.
 * Instances and their serializers are not thread-safe.
 */
public class DeleteOrderingGuardFile extends IndexFile {

    public static final String DELETE_ORDERING_GUARD = "DELETE_ORDERING_GUARD";
    public static final String SNAPSHOT_MAX_SEQUENCE_NUMBER =
            "delete-ordering-guards.max-sequence-number";

    /** Conservative hint, retained through resets along with the internal sequence watermark. */
    public static boolean mayContainGuards(@Nullable Snapshot snapshot) {
        return snapshot != null
                && snapshot.properties() != null
                && snapshot.properties().containsKey(SNAPSHOT_MAX_SEQUENCE_NUMBER);
    }

    private static final int MAGIC = 0x50444F47;
    private static final int VERSION = 1;

    private final DeleteOrderingGuardSerializer serializer;
    private final CacheManager cacheManager;
    private final int blockSize;
    private final Comparator<MemorySlice> keyComparator;

    public DeleteOrderingGuardFile(
            FileIO fileIO,
            IndexPathFactory pathFactory,
            DeleteOrderingGuardSerializer serializer,
            CacheManager cacheManager,
            int blockSize) {
        super(fileIO, pathFactory);
        checkArgument(blockSize > 0, "Guard SST block size must be positive.");
        this.serializer = serializer;
        this.cacheManager = cacheManager;
        this.blockSize = blockSize;
        this.keyComparator = serializer.keyComparator();
    }

    public Writer createWriter() {
        return new Writer();
    }

    /** Reads the source schema before constructing its metadata serializer. */
    public static long readSchemaId(FileIO fileIO, Path path) throws IOException {
        try (DataInputStream header = new DataInputStream(fileIO.newInputStream(path))) {
            if (header.readInt() != MAGIC) {
                throw new IOException("Invalid delete ordering guard segment magic: " + path);
            }
            int version = header.readInt();
            if (version != VERSION) {
                throw new IOException("Unsupported delete ordering guard version: " + version);
            }
            return header.readLong();
        }
    }

    public RecordReader<DeleteOrderingGuard> read(IndexFileMeta file) throws IOException {
        checkArgument(
                DELETE_ORDERING_GUARD.equals(file.indexType()),
                "Not a delete ordering guard segment: %s.",
                file.indexType());
        Path path = path(file);
        SeekableInputStream input = fileIO.newInputStream(path);
        try {
            DataInputStream header = new DataInputStream(input);
            if (header.readInt() != MAGIC) {
                throw new IOException("Invalid delete ordering guard segment magic: " + path);
            }
            int version = header.readInt();
            if (version != VERSION) {
                throw new IOException("Unsupported delete ordering guard version: " + version);
            }
            long schemaId = header.readLong();
            if (schemaId != serializer.schemaId()) {
                throw new IOException(
                        "Delete ordering guard schema mismatch: expected "
                                + serializer.schemaId()
                                + ", found "
                                + schemaId);
            }
            return new Reader(
                    new SortLookupStoreReader(
                            keyComparator, path, file.fileSize(), input, cacheManager),
                    file.rowCount());
        } catch (IOException | RuntimeException | Error failure) {
            try {
                input.close();
            } catch (Throwable closeFailure) {
                if (closeFailure != failure) {
                    failure.addSuppressed(closeFailure);
                }
            }
            throw failure;
        }
    }

    /**
     * Streams one segment in primary-key order, opening the output on the first guard. Close aborts
     * unfinished output; {@link #finish()} must be called explicitly to retain a segment.
     */
    public class Writer implements Closeable {

        @Nullable private Path path;
        @Nullable private PositionOutputStream output;
        @Nullable private SortLookupStoreWriter writer;
        @Nullable private byte[] previousKey;
        @Nullable private IndexFileMeta result;
        private long rowCount;
        private boolean ownsFile;
        private boolean finished;
        private boolean aborted;

        public void write(DeleteOrderingGuard guard) throws IOException {
            checkState(
                    !finished && !aborted, "Guard segment writer is already finished or aborted.");
            try {
                byte[] key = serializer.serializeKey(guard, rowCount);
                checkArgument(
                        previousKey == null
                                || keyComparator.compare(
                                                MemorySlice.wrap(previousKey),
                                                MemorySlice.wrap(key))
                                        < 0,
                        "Delete ordering guards must be sorted by primary key.");
                byte[] value = serializer.serializeValue(guard);
                if (writer == null) {
                    path = pathFactory.newPath();
                    output = fileIO.newOutputStream(path, false);
                    ownsFile = true;
                    DataOutputStream header = new DataOutputStream(output);
                    header.writeInt(MAGIC);
                    header.writeInt(VERSION);
                    header.writeLong(serializer.schemaId());
                    writer = new SortLookupStoreWriter(output, blockSize, null, null);
                }
                writer.put(key, value);
                rowCount++;
                previousKey = key;
            } catch (IOException | RuntimeException | Error failure) {
                abortOnFailure(failure);
                throw failure;
            }
        }

        /** Returns null for an empty writer; empty compactions create no segment. */
        @Nullable
        public IndexFileMeta finish() throws IOException {
            checkState(!aborted, "Guard segment writer was aborted.");
            if (finished) {
                return result;
            }
            try {
                if (writer != null) {
                    writer.close();
                    result =
                            new IndexFileMeta(
                                    DELETE_ORDERING_GUARD,
                                    path.getName(),
                                    fileIO.getFileSize(path),
                                    rowCount,
                                    null,
                                    isExternalPath() ? path.toString() : null,
                                    null);
                }
                finished = true;
                return result;
            } catch (IOException | RuntimeException | Error failure) {
                abortOnFailure(failure);
                throw failure;
            }
        }

        /** Deletes this writer's uncommitted output, including an already finished segment. */
        public void abort() throws IOException {
            if (aborted) {
                return;
            }
            aborted = true;
            try {
                if (output != null && !finished) {
                    output.close();
                }
            } finally {
                if (ownsFile) {
                    fileIO.deleteQuietly(path);
                }
            }
        }

        private void abortOnFailure(Throwable failure) {
            try {
                abort();
            } catch (Throwable closeFailure) {
                if (closeFailure != failure) {
                    failure.addSuppressed(closeFailure);
                }
            }
        }

        @Override
        public void close() throws IOException {
            if (!finished) {
                abort();
            }
        }
    }

    private class Reader implements RecordReader<DeleteOrderingGuard> {

        private final SortLookupStoreReader reader;
        private final SstFileReader.SstFileIterator iterator;
        private final long expectedRowCount;
        private long readRowCount;
        private boolean closed;

        private Reader(SortLookupStoreReader reader, long expectedRowCount) {
            this.reader = reader;
            this.iterator = reader.createIterator();
            this.expectedRowCount = expectedRowCount;
        }

        @Nullable
        @Override
        public RecordIterator<DeleteOrderingGuard> readBatch() throws IOException {
            checkState(!closed, "Guard segment reader is already closed.");
            BlockIterator batch = iterator.readBatch();
            if (batch == null) {
                if (readRowCount != expectedRowCount) {
                    throw new IOException(
                            "Delete ordering guard row count mismatch: expected "
                                    + expectedRowCount
                                    + ", found "
                                    + readRowCount);
                }
                return null;
            }
            return new RecordIterator<DeleteOrderingGuard>() {
                @Nullable
                @Override
                public DeleteOrderingGuard next() throws IOException {
                    if (!batch.hasNext()) {
                        return null;
                    }
                    if (++readRowCount > expectedRowCount) {
                        throw new IOException("Delete ordering guard segment contains extra rows.");
                    }
                    Map.Entry<MemorySlice, MemorySlice> entry = batch.next();
                    return serializer.deserialize(entry.getKey(), entry.getValue());
                }

                @Override
                public void releaseBatch() {}
            };
        }

        @Override
        public void close() throws IOException {
            if (!closed) {
                closed = true;
                reader.close();
            }
        }
    }
}
