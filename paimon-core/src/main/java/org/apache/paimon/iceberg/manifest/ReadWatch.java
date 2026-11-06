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

package org.apache.paimon.iceberg.manifest;

import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.FileStatus;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.SeekableInputStream;

import javax.annotation.Nullable;

import java.io.IOException;

/**
 * Passes every call on to the file system and notes whether any of them failed, or a read ended
 * early, so that a decoder error can be told from a storage error. A failure that surfaces from
 * closing a stream, from asking for its position or from a call on the file system is a storage
 * failure as much as one that surfaces from a read.
 */
final class ReadWatch implements FileIO {

    /** A call on the storage. */
    @FunctionalInterface
    private interface StorageCall<T> {
        T run() throws IOException;
    }

    private final FileIO delegate;
    private final long fileSize;
    private volatile Exception failure;
    private volatile boolean endedShort;

    ReadWatch(FileIO delegate, long fileSize) {
        this.delegate = delegate;
        this.fileSize = fileSize;
    }

    /** Runs a call on the storage, and notes what it throws before it passes it on. */
    private <T> T watched(StorageCall<T> call) throws IOException {
        try {
            return call.run();
        } catch (IOException | RuntimeException e) {
            note(e);
            throw e;
        }
    }

    private void note(Exception e) {
        if (failure == null) {
            failure = e;
        }
    }

    /** Fails when a call failed, or a read ended before the end of the file. */
    void throwIfStorageFailed(Path path, @Nullable Throwable cause) throws IOException {
        Exception failed = failure;
        if (failed instanceof IOException) {
            throw (IOException) failed;
        }
        if (failed != null) {
            throw new IOException("A read of " + path + " failed", failed);
        }
        if (endedShort) {
            throw new IOException("A read of " + path + " ended before the end of the file", cause);
        }
    }

    @Override
    public SeekableInputStream newInputStream(Path path) throws IOException {
        SeekableInputStream in = watched(() -> delegate.newInputStream(path));
        return new SeekableInputStream() {
            @Override
            public void seek(long desired) throws IOException {
                watched(
                        () -> {
                            in.seek(desired);
                            return null;
                        });
            }

            @Override
            public long getPos() throws IOException {
                return watched(in::getPos);
            }

            @Override
            public int read() throws IOException {
                try {
                    int b = in.read();
                    if (b < 0 && in.getPos() < fileSize) {
                        endedShort = true;
                    }
                    return b;
                } catch (IOException | RuntimeException e) {
                    note(e);
                    throw e;
                }
            }

            @Override
            public int read(byte[] b, int off, int len) throws IOException {
                try {
                    int n = in.read(b, off, len);
                    if (n < 0 && in.getPos() < fileSize) {
                        endedShort = true;
                    }
                    return n;
                } catch (IOException | RuntimeException e) {
                    note(e);
                    throw e;
                }
            }

            @Override
            public void close() throws IOException {
                watched(
                        () -> {
                            in.close();
                            return null;
                        });
            }
        };
    }

    @Override
    public boolean isObjectStore() {
        return delegate.isObjectStore();
    }

    @Override
    public void configure(CatalogContext context) {
        delegate.configure(context);
    }

    @Override
    public PositionOutputStream newOutputStream(Path path, boolean overwrite) throws IOException {
        return watched(() -> delegate.newOutputStream(path, overwrite));
    }

    @Override
    public FileStatus getFileStatus(Path path) throws IOException {
        return watched(() -> delegate.getFileStatus(path));
    }

    @Override
    public FileStatus[] listStatus(Path path) throws IOException {
        return watched(() -> delegate.listStatus(path));
    }

    @Override
    public boolean exists(Path path) throws IOException {
        return watched(() -> delegate.exists(path));
    }

    @Override
    public boolean delete(Path path, boolean recursive) throws IOException {
        return watched(() -> delegate.delete(path, recursive));
    }

    @Override
    public boolean mkdirs(Path path) throws IOException {
        return watched(() -> delegate.mkdirs(path));
    }

    @Override
    public boolean rename(Path src, Path dst) throws IOException {
        return watched(() -> delegate.rename(src, dst));
    }
}
