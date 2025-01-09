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

package org.apache.paimon.utils;

import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.FileIOLoader;
import org.apache.paimon.fs.FileStatus;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.PositionOutputStreamWrapper;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.fs.SeekableInputStreamWrapper;
import org.apache.paimon.fs.local.LocalFileIO;

import javax.annotation.concurrent.GuardedBy;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.apache.paimon.utils.Preconditions.checkNotNull;

/** A file system that tracks the number of concurrently open input streams, output streams. */
public class TraceableFileIO implements FileIO {

    public static final String SCHEME = "traceable";

    private final LocalFileIO originalFs = new LocalFileIO();

    /** The lock that synchronizes connection bookkeeping. */
    private static final ReentrantLock LOCK = new ReentrantLock(true);

    /** The set of currently open output streams. */
    @GuardedBy("lock")
    private static final HashSet<OutStream> OPEN_OUTPUT_STREAMS = new HashSet<>();

    /** The set of currently open input streams. */
    @GuardedBy("lock")
    private static final HashSet<InStream> OPEN_INPUT_STREAMS = new HashSet<>();

    @Override
    public PositionOutputStream newOutputStream(Path f, boolean overwrite) throws IOException {
        return createOutputStream(f, () -> originalFs.newOutputStream(f, overwrite));
    }

    @Override
    public boolean isObjectStore() {
        return originalFs.isObjectStore();
    }

    @Override
    public void configure(CatalogContext context) {
        originalFs.configure(context);
    }

    @Override
    public SeekableInputStream newInputStream(Path f) throws IOException {
        return createInputStream(f, () -> originalFs.newInputStream(f));
    }

    private PositionOutputStream createOutputStream(
            Path f, SupplierWithIOException<PositionOutputStream> streamOpener) throws IOException {

        final SupplierWithIOException<OutStream> wrappedStreamOpener =
                () -> new OutStream(ThreadUtils.currentStackString(), f, streamOpener.get(), this);

        return createStream(wrappedStreamOpener, OPEN_OUTPUT_STREAMS);
    }

    private SeekableInputStream createInputStream(
            Path f, SupplierWithIOException<SeekableInputStream> streamOpener) throws IOException {

        final SupplierWithIOException<InStream> wrappedStreamOpener =
                () -> new InStream(ThreadUtils.currentStackString(), f, streamOpener.get(), this);

        return createStream(wrappedStreamOpener, OPEN_INPUT_STREAMS);
    }

    @Override
    public FileStatus getFileStatus(Path f) throws IOException {
        return originalFs.getFileStatus(f);
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

    private <T> T createStream(
            final SupplierWithIOException<T> streamOpener, final HashSet<T> openStreams)
            throws IOException {
        // open the stream outside the lock.
        final T out = streamOpener.get();

        // add the stream to the set, need to re-acquire the lock
        LOCK.lock();
        try {
            openStreams.add(out);
        } finally {
            LOCK.unlock();
        }

        return out;
    }

    void unregisterOutputStream(OutStream stream) {
        LOCK.lock();
        try {
            OPEN_OUTPUT_STREAMS.remove(stream);
        } finally {
            LOCK.unlock();
        }
    }

    void unregisterInputStream(InStream stream) {
        LOCK.lock();
        try {
            OPEN_INPUT_STREAMS.remove(stream);
        } finally {
            LOCK.unlock();
        }
    }

    private static final class OutStream extends PositionOutputStreamWrapper {

        private final String stack;
        private final Path file;
        private final TraceableFileIO fs;

        /** Flag tracking whether the stream was already closed, for proper inactivity tracking. */
        private final AtomicBoolean closed = new AtomicBoolean();

        OutStream(
                String stack, Path file, PositionOutputStream originalStream, TraceableFileIO fs) {
            super(originalStream);
            this.stack = stack;
            this.file = file;
            this.fs = checkNotNull(fs);
        }

        @Override
        public void close() throws IOException {
            if (closed.compareAndSet(false, true)) {
                try {
                    out.close();
                } finally {
                    fs.unregisterOutputStream(this);
                }
            }
        }

        @Override
        public String toString() {
            return "OutStream{" + "file=" + file + ", " + "stack=" + stack + '}';
        }
    }

    private static final class InStream extends SeekableInputStreamWrapper {

        private final Path file;
        private final String stack;
        private final TraceableFileIO fs;

        /** Flag tracking whether the stream was already closed, for proper inactivity tracking. */
        private final AtomicBoolean closed = new AtomicBoolean();

        InStream(String stack, Path file, SeekableInputStream originalStream, TraceableFileIO fs) {
            super(originalStream);
            this.stack = stack;
            this.fs = checkNotNull(fs);
            this.file = file;
        }

        public Path file() {
            return file;
        }

        @Override
        public void close() throws IOException {
            if (closed.compareAndSet(false, true)) {
                try {
                    in.close();
                } finally {
                    fs.unregisterInputStream(this);
                }
            }
        }

        @Override
        public String toString() {
            return "InStream{" + "file=" + file + ", " + "stack=" + stack + '}';
        }
    }

    public static List<SeekableInputStream> openInputStreams(Predicate<Path> filter) {
        LOCK.lock();
        try {
            return OPEN_INPUT_STREAMS.stream()
                    .filter(s -> filter.test(s.file))
                    .collect(Collectors.toList());
        } finally {
            LOCK.unlock();
        }
    }

    public static List<PositionOutputStream> openOutputStreams(Predicate<Path> filter) {
        LOCK.lock();
        try {
            return OPEN_OUTPUT_STREAMS.stream()
                    .filter(s -> filter.test(s.file))
                    .collect(Collectors.toList());
        } finally {
            LOCK.unlock();
        }
    }

    /** Loader for {@link TraceableFileIO}. */
    public static class Loader implements FileIOLoader {

        @Override
        public String getScheme() {
            return SCHEME;
        }

        @Override
        public FileIO load(Path path) {
            return new TraceableFileIO();
        }
    }
}
