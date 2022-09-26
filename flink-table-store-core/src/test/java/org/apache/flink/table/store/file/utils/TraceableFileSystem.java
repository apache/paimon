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

package org.apache.flink.table.store.file.utils;

import org.apache.flink.core.fs.BlockLocation;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.FileSystemKind;
import org.apache.flink.core.fs.Path;
import org.apache.flink.util.function.SupplierWithException;

import javax.annotation.concurrent.GuardedBy;

import java.io.IOException;
import java.net.URI;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** A file system that tracks the number of concurrently open input streams, output streams. */
public class TraceableFileSystem extends FileSystem {

    private final FileSystem originalFs;

    /** The lock that synchronizes connection bookkeeping. */
    private final ReentrantLock lock;

    /** The set of currently open output streams. */
    @GuardedBy("lock")
    private final HashSet<OutStream> openOutputStreams;

    /** The set of currently open input streams. */
    @GuardedBy("lock")
    private final HashSet<InStream> openInputStreams;

    public TraceableFileSystem(FileSystem originalFs) {

        this.originalFs = checkNotNull(originalFs, "originalFs");
        this.lock = new ReentrantLock(true);
        this.openOutputStreams = new HashSet<>();
        this.openInputStreams = new HashSet<>();
    }

    @Override
    public FSDataOutputStream create(Path f, WriteMode overwriteMode) throws IOException {
        return createOutputStream(f, () -> originalFs.create(f, overwriteMode));
    }

    @Override
    @Deprecated
    @SuppressWarnings("deprecation")
    public FSDataOutputStream create(
            Path f, boolean overwrite, int bufferSize, short replication, long blockSize)
            throws IOException {

        return createOutputStream(
                f, () -> originalFs.create(f, overwrite, bufferSize, replication, blockSize));
    }

    @Override
    public FSDataInputStream open(Path f, int bufferSize) throws IOException {
        return createInputStream(f, () -> originalFs.open(f, bufferSize));
    }

    @Override
    public FSDataInputStream open(Path f) throws IOException {
        return createInputStream(f, () -> originalFs.open(f));
    }

    private FSDataOutputStream createOutputStream(
            Path f, SupplierWithException<FSDataOutputStream, IOException> streamOpener)
            throws IOException {

        final SupplierWithException<OutStream, IOException> wrappedStreamOpener =
                () -> new OutStream(f, streamOpener.get(), this);

        return createStream(wrappedStreamOpener, openOutputStreams);
    }

    private FSDataInputStream createInputStream(
            Path f, SupplierWithException<FSDataInputStream, IOException> streamOpener)
            throws IOException {

        final SupplierWithException<InStream, IOException> wrappedStreamOpener =
                () -> new InStream(f, streamOpener.get(), this);

        return createStream(wrappedStreamOpener, openInputStreams);
    }

    @Override
    public FileSystemKind getKind() {
        return originalFs.getKind();
    }

    @Override
    public boolean isDistributedFS() {
        return originalFs.isDistributedFS();
    }

    @Override
    public Path getWorkingDirectory() {
        return originalFs.getWorkingDirectory();
    }

    @Override
    public Path getHomeDirectory() {
        return originalFs.getHomeDirectory();
    }

    @Override
    public URI getUri() {
        return originalFs.getUri();
    }

    @Override
    public FileStatus getFileStatus(Path f) throws IOException {
        return originalFs.getFileStatus(f);
    }

    @Override
    public BlockLocation[] getFileBlockLocations(FileStatus file, long start, long len)
            throws IOException {
        return originalFs.getFileBlockLocations(file, start, len);
    }

    @Override
    public FileStatus[] listStatus(Path f) throws IOException {
        return originalFs.listStatus(f);
    }

    @Override
    public boolean delete(Path f, boolean recursive) throws IOException {
        return originalFs.delete(f, recursive);
    }

    @Override
    public boolean mkdirs(Path f) throws IOException {
        return originalFs.mkdirs(f);
    }

    @Override
    public boolean rename(Path src, Path dst) throws IOException {
        return originalFs.rename(src, dst);
    }

    @Override
    public boolean exists(Path f) throws IOException {
        return originalFs.exists(f);
    }

    @Override
    @Deprecated
    @SuppressWarnings("deprecation")
    public long getDefaultBlockSize() {
        return originalFs.getDefaultBlockSize();
    }

    private <T> T createStream(
            final SupplierWithException<T, IOException> streamOpener, final HashSet<T> openStreams)
            throws IOException {
        // open the stream outside the lock.
        final T out = streamOpener.get();

        // add the stream to the set, need to re-acquire the lock
        lock.lock();
        try {
            openStreams.add(out);
        } finally {
            lock.unlock();
        }

        return out;
    }

    void unregisterOutputStream(OutStream stream) {
        lock.lock();
        try {
            openOutputStreams.remove(stream);
        } finally {
            lock.unlock();
        }
    }

    void unregisterInputStream(InStream stream) {
        lock.lock();
        try {
            openInputStreams.remove(stream);
        } finally {
            lock.unlock();
        }
    }

    private static final class OutStream extends FSDataOutputStream {

        private final Path file;

        private final FSDataOutputStream originalStream;

        private final TraceableFileSystem fs;

        /** Flag tracking whether the stream was already closed, for proper inactivity tracking. */
        private final AtomicBoolean closed = new AtomicBoolean();

        OutStream(Path file, FSDataOutputStream originalStream, TraceableFileSystem fs) {
            this.file = file;
            this.originalStream = checkNotNull(originalStream);
            this.fs = checkNotNull(fs);
        }

        private Path file() {
            return file;
        }

        @Override
        public void write(int b) throws IOException {
            originalStream.write(b);
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            originalStream.write(b, off, len);
        }

        @Override
        public long getPos() throws IOException {
            return originalStream.getPos();
        }

        @Override
        public void flush() throws IOException {
            originalStream.flush();
        }

        @Override
        public void sync() throws IOException {
            originalStream.sync();
        }

        @Override
        public void close() throws IOException {
            if (closed.compareAndSet(false, true)) {
                try {
                    originalStream.close();
                } finally {
                    fs.unregisterOutputStream(this);
                }
            }
        }
    }

    private static final class InStream extends FSDataInputStream {

        private final Path file;

        private final FSDataInputStream originalStream;

        private final TraceableFileSystem fs;

        /** Flag tracking whether the stream was already closed, for proper inactivity tracking. */
        private final AtomicBoolean closed = new AtomicBoolean();

        InStream(Path file, FSDataInputStream originalStream, TraceableFileSystem fs) {
            this.originalStream = checkNotNull(originalStream);
            this.fs = checkNotNull(fs);
            this.file = file;
        }

        public Path file() {
            return file;
        }

        @Override
        public int read() throws IOException {
            return originalStream.read();
        }

        @Override
        public int read(byte[] b) throws IOException {
            return originalStream.read(b);
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            return originalStream.read(b, off, len);
        }

        @Override
        public long skip(long n) throws IOException {
            return originalStream.skip(n);
        }

        @Override
        public int available() throws IOException {
            return originalStream.available();
        }

        @Override
        public void mark(int readlimit) {
            originalStream.mark(readlimit);
        }

        @Override
        public void reset() throws IOException {
            originalStream.reset();
        }

        @Override
        public boolean markSupported() {
            return originalStream.markSupported();
        }

        @Override
        public void seek(long desired) throws IOException {
            originalStream.seek(desired);
        }

        @Override
        public long getPos() throws IOException {
            return originalStream.getPos();
        }

        @Override
        public void close() throws IOException {
            if (closed.compareAndSet(false, true)) {
                try {
                    originalStream.close();
                } finally {
                    fs.unregisterInputStream(this);
                }
            }
        }
    }

    public List<FSDataInputStream> openInputStreams(Predicate<Path> filter) {
        return openInputStreams.stream()
                .filter(s -> filter.test(s.file))
                .collect(Collectors.toList());
    }

    public List<FSDataOutputStream> openOutputStreams(Predicate<Path> filter) {
        return openOutputStreams.stream()
                .filter(s -> filter.test(s.file))
                .collect(Collectors.toList());
    }
}
