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

package org.apache.paimon.fs;

import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.options.Options;

import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.AccessDeniedException;
import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.StandardCopyOption;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

import static org.apache.paimon.utils.Preconditions.checkState;
import static org.assertj.core.api.Assertions.assertThat;

/** Test static methods and methods with default implementations of {@link FileIO}. */
public class FileIOTest {

    @TempDir java.nio.file.Path tempDir;

    @Test
    public void testRequireOptions() throws IOException {
        Options options = new Options();

        try {
            FileIO.get(
                    new Path("require-options://" + tempDir.toString()),
                    CatalogContext.create(options));
            Assertions.fail();
        } catch (UnsupportedSchemeException e) {
            assertThat(e.getSuppressed()[0])
                    .hasMessageContaining("Missing required options are:\n\nRequire1\nreQuire2");
        }

        options.set("Re-quire1", "dummy");
        options.set("reQuire2", "dummy");
        FileIO fileIO =
                FileIO.get(
                        new Path("require-options://" + tempDir.toString()),
                        CatalogContext.create(options));
        assertThat(fileIO).isInstanceOf(RequireOptionsFileIOLoader.MyFileIO.class);
    }

    @Test
    public void testCopy() throws Exception {
        Path srcFile = new Path(tempDir.resolve("src.txt").toUri());
        Path dstFile = new Path(tempDir.resolve("dst.txt").toUri());

        FileIO fileIO = new DummyFileIO();
        fileIO.writeFileUtf8(srcFile, "foobar");

        assertThat(fileIO.copyFile(srcFile, dstFile)).isTrue();
        assertThat(fileIO.readFileUtf8(dstFile)).isEqualTo("foobar");
        fileIO.deleteQuietly(dstFile);

        assertThat(fileIO.copyFile(srcFile, dstFile)).isTrue();
        assertThat(fileIO.readFileUtf8(dstFile)).isEqualTo("foobar");
        fileIO.deleteQuietly(dstFile);

        fileIO.deleteQuietly(srcFile);
        srcFile = new Path(this.getClass().getClassLoader().getResource("test-data.orc").toURI());

        fileIO.copyFile(srcFile, dstFile);
        assertThat(FileUtils.contentEquals(new File(srcFile.toUri()), new File(dstFile.toUri())))
                .isFalse();
        fileIO.deleteQuietly(dstFile);

        fileIO.copyFile(srcFile, dstFile);
        assertThat(FileUtils.contentEquals(new File(srcFile.toUri()), new File(dstFile.toUri())))
                .isTrue();
        fileIO.deleteQuietly(dstFile);
    }

    public static void testOverwriteFileUtf8(Path file, FileIO fileIO) throws InterruptedException {
        AtomicReference<Exception> exception = new AtomicReference<>();
        final int max = 10;

        Thread writeThread =
                new Thread(
                        () -> {
                            for (int i = 0; i <= max; i++) {
                                try {
                                    fileIO.overwriteFileUtf8(file, "" + i);
                                    Thread.sleep(100);
                                } catch (Exception e) {
                                    exception.set(e);
                                    return;
                                }
                            }
                        });

        Thread readThread =
                new Thread(
                        () -> {
                            while (true) {
                                try {
                                    Optional<String> ret = fileIO.readOverwrittenFileUtf8(file);
                                    if (!ret.isPresent()) {
                                        continue;
                                    }

                                    int value = Integer.parseInt(ret.get());
                                    if (value == max) {
                                        return;
                                    }
                                } catch (Exception e) {
                                    exception.set(e);
                                    return;
                                }
                            }
                        });

        writeThread.start();
        readThread.start();

        writeThread.join();
        readThread.join();

        assertThat(exception.get()).isNull();
    }

    /** A {@link FileIO} on local filesystem to test the default copy implementation. */
    private static class DummyFileIO implements FileIO {
        private static final ReentrantLock RENAME_LOCK = new ReentrantLock();

        @Override
        public boolean isObjectStore() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void configure(CatalogContext context) {
            throw new UnsupportedOperationException();
        }

        @Override
        public SeekableInputStream newInputStream(Path path) throws FileNotFoundException {
            return new LocalFileIO.LocalSeekableInputStream(toFile(path));
        }

        @Override
        public PositionOutputStream newOutputStream(Path path, boolean overwrite)
                throws IOException {
            if (exists(path) && !overwrite) {
                throw new FileAlreadyExistsException("File already exists: " + path);
            }

            Path parent = path.getParent();
            if (parent != null && !mkdirs(parent)) {
                throw new IOException("Mkdirs failed to create " + parent);
            }

            return new LocalFileIO.LocalPositionOutputStream(toFile(path));
        }

        @Override
        public FileStatus getFileStatus(Path path) {
            throw new UnsupportedOperationException();
        }

        @Override
        public FileStatus[] listStatus(Path path) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean exists(Path path) {
            return toFile(path).exists();
        }

        @Override
        public boolean delete(Path path, boolean recursive) throws IOException {
            File file = toFile(path);
            if (file.isFile()) {
                return file.delete();
            } else if ((!recursive) && file.isDirectory()) {
                File[] containedFiles = file.listFiles();
                if (containedFiles == null) {
                    throw new IOException(
                            "Directory " + file + " does not exist or an I/O error occurred");
                } else if (containedFiles.length != 0) {
                    throw new IOException("Directory " + file + " is not empty");
                }
            }

            return delete(file);
        }

        private boolean delete(final File f) {
            if (f.isDirectory()) {
                final File[] files = f.listFiles();
                if (files != null) {
                    for (File file : files) {
                        final boolean del = delete(file);
                        if (!del) {
                            return false;
                        }
                    }
                }
            } else {
                return f.delete();
            }

            // Now directory is empty
            return f.delete();
        }

        @Override
        public boolean mkdirs(Path path) throws IOException {
            return mkdirsInternal(toFile(path));
        }

        private boolean mkdirsInternal(File file) throws IOException {
            if (file.isDirectory()) {
                return true;
            } else if (file.exists() && !file.isDirectory()) {
                // Important: The 'exists()' check above must come before the 'isDirectory()' check
                // to
                //            be safe when multiple parallel instances try to create the directory

                // exists and is not a directory -> is a regular file
                throw new FileAlreadyExistsException(file.getAbsolutePath());
            } else {
                File parent = file.getParentFile();
                return (parent == null || mkdirsInternal(parent))
                        && (file.mkdir() || file.isDirectory());
            }
        }

        @Override
        public boolean rename(Path src, Path dst) throws IOException {
            File srcFile = toFile(src);
            File dstFile = toFile(dst);
            File dstParent = dstFile.getParentFile();
            dstParent.mkdirs();
            try {
                RENAME_LOCK.lock();
                if (dstFile.exists()) {
                    return false;
                }
                Files.move(srcFile.toPath(), dstFile.toPath(), StandardCopyOption.ATOMIC_MOVE);
                return true;
            } catch (NoSuchFileException
                    | AccessDeniedException
                    | DirectoryNotEmptyException
                    | SecurityException e) {
                return false;
            } finally {
                RENAME_LOCK.unlock();
            }
        }

        private File toFile(Path path) {
            // remove scheme
            String localPath = path.toUri().getPath();
            checkState(localPath != null, "Cannot convert a null path to File");

            if (localPath.length() == 0) {
                return new File(".");
            }

            return new File(localPath);
        }
    }
}
