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

import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.FileIOLoader;
import org.apache.paimon.fs.FileStatus;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.PositionOutputStreamWrapper;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.fs.SeekableInputStreamWrapper;
import org.apache.paimon.fs.local.LocalFileIO;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A {@link TraceableFileIO} which may fail when reading and writing. Mainly used to check if
 * components deal with failures correctly.
 */
public class FailingFileIO extends TraceableFileIO {

    public static final String SCHEME = "fail";

    private static final Map<String, AtomicInteger> FAIL_COUNTERS = new ConcurrentHashMap<>();
    private static final Map<String, Integer> FAIL_POSSIBILITIES = new ConcurrentHashMap<>();

    public static String getFailingPath(String name, String path) {
        // set authority as given name so that different tests use different instances
        // for more information see FileSystem#getUnguardedFileSystem for the caching strategy
        return SCHEME + "://" + name + path;
    }

    public static void reset(String name, int maxFails, int failPossibility) {
        FAIL_COUNTERS.put(name, new AtomicInteger(maxFails));
        FAIL_POSSIBILITIES.put(name, failPossibility);
    }

    private String name(Path path) {
        return path.toUri().getAuthority();
    }

    @Override
    public SeekableInputStream newInputStream(Path f) throws IOException {
        return new FailingSeekableInputStreamWrapper(name(f), super.newInputStream(f));
    }

    @Override
    public PositionOutputStream newOutputStream(Path filePath, boolean overwrite)
            throws IOException {
        return new FailingPositionOutputStreamWrapper(
                name(filePath), super.newOutputStream(filePath, overwrite));
    }

    @Override
    public FileStatus getFileStatus(Path path) throws IOException {
        File file = new LocalFileIO().toFile(path);
        if (file.exists()) {
            return new FailingLocalFileStatus(file, path);
        } else {
            throw new FileNotFoundException(
                    "File "
                            + path
                            + " does not exist or the user running Paimon ('"
                            + System.getProperty("user.name")
                            + "') has insufficient permissions to access it.");
        }
    }

    /** Specific {@link IOException} produced by {@link FailingFileIO}. */
    public static final class ArtificialException extends IOException {

        public ArtificialException() {
            super("Artificial exception");
        }
    }

    private static class FailingSeekableInputStreamWrapper extends SeekableInputStreamWrapper {

        private final String name;

        public FailingSeekableInputStreamWrapper(String name, SeekableInputStream inputStream) {
            super(inputStream);
            this.name = name;
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            // only fail when reading more than 1 byte so that we won't fail too often
            if (b.length > 1
                    && ThreadLocalRandom.current().nextInt(FAIL_POSSIBILITIES.get(name)) == 0
                    && FAIL_COUNTERS.get(name).getAndDecrement() > 0) {
                throw new ArtificialException();
            }
            return super.read(b, off, len);
        }
    }

    private static class FailingPositionOutputStreamWrapper extends PositionOutputStreamWrapper {

        private final String name;

        public FailingPositionOutputStreamWrapper(String name, PositionOutputStream outputStream) {
            super(outputStream);
            this.name = name;
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            // only fail when writing more than 1 byte so that we won't fail too often
            if (b.length > 1
                    && ThreadLocalRandom.current().nextInt(FAIL_POSSIBILITIES.get(name)) == 0
                    && FAIL_COUNTERS.get(name).getAndDecrement() > 0) {
                throw new ArtificialException();
            }
            super.write(b, off, len);
        }
    }

    private static class FailingLocalFileStatus implements FileStatus {

        private final File file;
        private final Path path;
        private final long len;

        private FailingLocalFileStatus(File file, Path path) {
            this.file = file;
            this.path = path;
            this.len = file.length();
        }

        @Override
        public long getLen() {
            return len;
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
        public long getModificationTime() {
            return file.lastModified();
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

    public static void retryArtificialException(Runnable runnable) throws Exception {
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

    /** Loader for {@link FailingFileIO}. */
    public static class Loader implements FileIOLoader {

        @Override
        public String getScheme() {
            return SCHEME;
        }

        @Override
        public FileIO load(Path path) {
            return new FailingFileIO();
        }
    }
}
