/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.store.file.utils;

import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FSDataInputStreamWrapper;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FSDataOutputStreamWrapper;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.FileSystemFactory;
import org.apache.flink.core.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A {@link TestAtomicRenameFileSystem} which may fail when reading and writing. Mainly used to
 * check if components deal with failures correctly.
 */
public class FailingAtomicRenameFileSystem extends TestAtomicRenameFileSystem {

    public static final String SCHEME = "fail";

    private final String threadName;
    private final AtomicInteger failCounter = new AtomicInteger();
    private int failPossibility;

    public FailingAtomicRenameFileSystem(String threadName) {
        this.threadName = threadName;
    }

    public static FailingAtomicRenameFileSystem get() {
        try {
            return (FailingAtomicRenameFileSystem) new Path(getFailingPath("/")).getFileSystem();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static String getFailingPath(String path) {
        // set authority as thread name so that different testing threads use different instances
        // for more information see FileSystem#getUnguardedFileSystem for the caching strategy
        return SCHEME + "://" + Thread.currentThread().getName() + path;
    }

    public void reset(int maxFails, int failPossibility) {
        failCounter.set(maxFails);
        this.failPossibility = failPossibility;
    }

    @Override
    public FSDataInputStream open(Path f, int bufferSize) throws IOException {
        return new FailingFSDataInputStreamWrapper(super.open(f, bufferSize));
    }

    @Override
    public FSDataInputStream open(Path f) throws IOException {
        return new FailingFSDataInputStreamWrapper(super.open(f));
    }

    @Override
    public FSDataOutputStream create(Path filePath, FileSystem.WriteMode overwrite)
            throws IOException {
        return new FailingFSDataOutputStreamWrapper(super.create(filePath, overwrite));
    }

    @Override
    public URI getUri() {
        return URI.create(SCHEME + "://" + threadName + "/");
    }

    /** {@link FileSystemFactory} for {@link FailingAtomicRenameFileSystem}. */
    public static final class FailingAtomicRenameFileSystemFactory implements FileSystemFactory {

        @Override
        public String getScheme() {
            return SCHEME;
        }

        @Override
        public FileSystem create(URI uri) throws IOException {
            return new FailingAtomicRenameFileSystem(uri.getAuthority());
        }
    }

    /** Specific {@link IOException} produced by {@link FailingAtomicRenameFileSystem}. */
    public static final class ArtificialException extends IOException {

        public ArtificialException() {
            super("Artificial exception");
        }
    }

    private class FailingFSDataInputStreamWrapper extends FSDataInputStreamWrapper {

        public FailingFSDataInputStreamWrapper(FSDataInputStream inputStream) {
            super(inputStream);
        }

        @Override
        public int read() throws IOException {
            if (ThreadLocalRandom.current().nextInt(failPossibility) == 0
                    && failCounter.getAndDecrement() > 0) {
                throw new ArtificialException();
            }
            return super.read();
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            if (ThreadLocalRandom.current().nextInt(failPossibility) == 0
                    && failCounter.getAndDecrement() > 0) {
                throw new ArtificialException();
            }
            return super.read(b, off, len);
        }
    }

    private class FailingFSDataOutputStreamWrapper extends FSDataOutputStreamWrapper {

        public FailingFSDataOutputStreamWrapper(FSDataOutputStream outputStream) {
            super(outputStream);
        }

        @Override
        public void write(int b) throws IOException {
            if (ThreadLocalRandom.current().nextInt(failPossibility) == 0
                    && failCounter.getAndDecrement() > 0) {
                throw new ArtificialException();
            }
            super.write(b);
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            if (ThreadLocalRandom.current().nextInt(failPossibility) == 0
                    && failCounter.getAndDecrement() > 0) {
                throw new ArtificialException();
            }
            super.write(b, off, len);
        }
    }
}
