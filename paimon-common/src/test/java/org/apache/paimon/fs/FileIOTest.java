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
import org.apache.paimon.utils.Pair;

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
        fileIO.tryToWriteAtomic(srcFile, "foobar");

        fileIO.copyFile(srcFile, dstFile, true);
        assertThat(fileIO.readFileUtf8(dstFile)).isEqualTo("foobar");
        fileIO.deleteQuietly(dstFile);

        fileIO.copyFile(srcFile, dstFile, true);
        assertThat(fileIO.readFileUtf8(dstFile)).isEqualTo("foobar");
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

    @Test
    public void testListFiles() throws Exception {
        Path fileA = new Path(tempDir.resolve("a").toUri());
        Path dirB = new Path(tempDir.resolve("b").toUri());
        Path fileBC = new Path(tempDir.resolve("b/c").toUri());

        FileIO fileIO = new LocalFileIO();
        fileIO.writeFile(fileA, "fileA", false);
        fileIO.mkdirs(dirB);
        fileIO.writeFile(fileBC, "fileBC", false);

        {
            // if listing non-recursively, file "a" is the only file in the top level directory
            FileStatus[] statuses = fileIO.listFiles(new Path(tempDir.toUri()), false);
            assertThat(statuses.length).isEqualTo(1);
            assertThat(statuses[0].getPath()).isEqualTo(fileA);
        }

        {
            // if listing recursively, file "a" and "b/c" should be listed, directory "b" should be
            // omitted
            FileStatus[] statuses = fileIO.listFiles(new Path(tempDir.toUri()), true);
            assertThat(statuses.length).isEqualTo(2);
            assertThat(statuses[0].getPath()).isEqualTo(fileA);
            assertThat(statuses[1].getPath()).isEqualTo(fileBC);
        }
    }

    @Test
    public void testListFilesPaged() throws Exception {
        FileIO fileIO = new LocalFileIO();
        // 10 files starting with "a"
        for (int i = 0; i < 10; i++) {
            Path p = new Path(tempDir.resolve(String.format("a-%02d", i)).toUri());
            fileIO.writeFile(p, p.toString(), false);
        }
        // 10 files starting with "b"
        for (int i = 0; i < 10; i++) {
            Path p = new Path(tempDir.resolve(String.format("b-%02d", i)).toUri());
            fileIO.writeFile(p, p.toString(), false);
        }
        // 10 files under directory "c"
        fileIO.mkdirs(new Path(tempDir.resolve("c").toUri()));
        for (int i = 0; i < 10; i++) {
            Path p = new Path(tempDir.resolve(String.format("c/c-%02d", i)).toUri());
            fileIO.writeFile(p, p.toString(), false);
        }

        {
            // first 5 files should be "a-00" to "a-04"
            Pair<FileStatus[], String> page =
                    fileIO.listFilesPaged(new Path(tempDir.toUri()), true, 5, null);
            assertThat(page.getLeft().length).isEqualTo(5);
            assertThat(page.getLeft()[0].getPath().getName()).isEqualTo("a-00");
            assertThat(page.getLeft()[4].getPath().getName()).isEqualTo("a-04");
            assertThat(page.getRight()).isNotNull();
            // the next 10 files should be "a-05" to "b-04"
            page = fileIO.listFilesPaged(new Path(tempDir.toUri()), true, 10, page.getRight());
            assertThat(page.getLeft().length).isEqualTo(10);
            assertThat(page.getLeft()[0].getPath().getName()).isEqualTo("a-05");
            assertThat(page.getLeft()[9].getPath().getName()).isEqualTo("b-04");
            assertThat(page.getRight()).isNotNull();
            // next 10 files should recurse to "c/c-04"
            page = fileIO.listFilesPaged(new Path(tempDir.toUri()), true, 10, page.getRight());
            assertThat(page.getLeft().length).isEqualTo(10);
            assertThat(page.getLeft()[9].getPath().getParent().getName()).isEqualTo("c");
            assertThat(page.getLeft()[9].getPath().getName()).isEqualTo("c-04");
            assertThat(page.getRight()).isNotNull();
        }

        {
            // list all files non-recursively should return "a-00" through "b-09" and no more
            Pair<FileStatus[], String> page =
                    fileIO.listFilesPaged(new Path(tempDir.toUri()), false, 9999, null);
            assertThat(page.getLeft().length).isEqualTo(20);
            assertThat(page.getLeft()[0].getPath().getName()).isEqualTo("a-00");
            assertThat(page.getLeft()[19].getPath().getName()).isEqualTo("b-09");
            assertThat(page.getRight()).isNull();
        }
    }

    /** A {@link FileIO} on local filesystem to test various default implementations. */
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
        public FileStatus getFileStatus(Path path) throws IOException {
            return new LocalFileIO().getFileStatus(path);
        }

        @Override
        public FileStatus[] listStatus(Path path) throws IOException {
            return new LocalFileIO().listStatus(path);
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
