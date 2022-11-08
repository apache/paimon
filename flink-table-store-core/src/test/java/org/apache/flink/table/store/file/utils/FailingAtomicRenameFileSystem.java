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

import org.apache.flink.core.fs.BlockLocation;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FSDataInputStreamWrapper;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FSDataOutputStreamWrapper;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.FileSystemFactory;
import org.apache.flink.core.fs.LocatedFileStatus;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.fs.local.LocalBlockLocation;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.function.RunnableWithException;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A {@link TestAtomicRenameFileSystem} which may fail when reading and writing. Mainly used to
 * check if components deal with failures correctly.
 */
public class FailingAtomicRenameFileSystem extends TestAtomicRenameFileSystem {

    public static final String SCHEME = "fail";

    private final String name;
    private final AtomicInteger failCounter = new AtomicInteger();
    private int failPossibility;

    private FailingAtomicRenameFileSystem(String name) {
        this.name = name;
    }

    public static String getFailingPath(String name, String path) {
        // set authority as given name so that different tests use different instances
        // for more information see FileSystem#getUnguardedFileSystem for the caching strategy
        return SCHEME + "://" + name + path;
    }

    public static void reset(String name, int maxFails, int failPossibility) {
        try {
            ((FailingAtomicRenameFileSystem) new Path(getFailingPath(name, "/")).getFileSystem())
                    .reset(maxFails, failPossibility);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void reset(int maxFails, int failPossibility) {
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
        return URI.create(SCHEME + "://" + name + "/");
    }

    @Override
    public FileStatus getFileStatus(Path path) throws IOException {
        File file = this.pathToFile(path);
        if (file.exists()) {
            return new FailingLocalFileStatus(file, path);
        } else {
            throw new FileNotFoundException(
                    "File "
                            + path
                            + " does not exist or the user running Flink ('"
                            + System.getProperty("user.name")
                            + "') has insufficient permissions to access it.");
        }
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
        public int read(byte[] b, int off, int len) throws IOException {
            // only fail when reading more than 1 byte so that we won't fail too often
            if (b.length > 1
                    && ThreadLocalRandom.current().nextInt(failPossibility) == 0
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
        public void write(byte[] b, int off, int len) throws IOException {
            // only fail when writing more than 1 byte so that we won't fail too often
            if (b.length > 1
                    && ThreadLocalRandom.current().nextInt(failPossibility) == 0
                    && failCounter.getAndDecrement() > 0) {
                throw new ArtificialException();
            }
            super.write(b, off, len);
        }
    }

    private static class FailingLocalFileStatus implements LocatedFileStatus {

        private final File file;
        private final Path path;
        private final long len;

        private FailingLocalFileStatus(File file, Path path) {
            this.file = file;
            this.path = path;
            this.len = file.length();
        }

        @Override
        public BlockLocation[] getBlockLocations() {
            return new BlockLocation[] {new LocalBlockLocation(len)};
        }

        @Override
        public long getLen() {
            return len;
        }

        @Override
        public long getBlockSize() {
            return len;
        }

        @Override
        public short getReplication() {
            return 1;
        }

        @Override
        public long getModificationTime() {
            return file.lastModified();
        }

        @Override
        public long getAccessTime() {
            return 0;
        }

        @Override
        public boolean isDir() {
            return file.isDirectory();
        }

        @Override
        public Path getPath() {
            return path;
        }

        @Override
        public String toString() {
            return "FailingLocalFileStatus{file=" + this.file + ", path=" + this.path + '}';
        }
    }

    public static <T> T retryArtificialException(Callable<T> callable) throws Exception {
        while (true) {
            try {
                return callable.call();
            } catch (Throwable t) {
                if (!ExceptionUtils.findThrowable(t, ArtificialException.class).isPresent()) {
                    throw t;
                }
            }
        }
    }

    public static void retryArtificialException(RunnableWithException runnable) throws Exception {
        while (true) {
            try {
                runnable.run();
                break;
            } catch (Throwable t) {
                if (!ExceptionUtils.findThrowable(t, ArtificialException.class).isPresent()) {
                    throw t;
                }
            }
        }
    }
}
