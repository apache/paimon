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

package org.apache.paimon.fs.local;

import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.FileStatus;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.fs.VectoredReadable;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.AccessDeniedException;
import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

import static org.apache.paimon.fs.local.LocalFileIOLoader.SCHEME;
import static org.apache.paimon.utils.Preconditions.checkState;

/** {@link FileIO} for local file. */
public class LocalFileIO implements FileIO {

    private static final long serialVersionUID = 1L;

    // the lock to ensure atomic renaming
    private static final ReentrantLock RENAME_LOCK = new ReentrantLock();

    public static LocalFileIO create() {
        return new LocalFileIO();
    }

    @Override
    public boolean isObjectStore() {
        return false;
    }

    @Override
    public void configure(CatalogContext context) {}

    @Override
    public SeekableInputStream newInputStream(Path path) throws IOException {
        return new LocalSeekableInputStream(toFile(path));
    }

    @Override
    public PositionOutputStream newOutputStream(Path path, boolean overwrite) throws IOException {
        if (exists(path) && !overwrite) {
            throw new FileAlreadyExistsException("File already exists: " + path);
        }

        Path parent = path.getParent();
        if (parent != null && !mkdirs(parent)) {
            throw new IOException("Mkdirs failed to create " + parent);
        }

        return new LocalPositionOutputStream(toFile(path));
    }

    @Override
    public FileStatus getFileStatus(Path path) throws IOException {
        final File file = toFile(path);
        if (file.exists()) {
            return new LocalFileStatus(file, SCHEME);
        } else {
            throw new FileNotFoundException(
                    "File "
                            + file
                            + " does not exist or the user running "
                            + "Paimon ('"
                            + System.getProperty("user.name")
                            + "') has insufficient permissions to access it.");
        }
    }

    @Override
    public FileStatus[] listStatus(Path path) throws IOException {
        final File file = toFile(path);
        FileStatus[] results = new FileStatus[0];

        if (!file.exists()) {
            return results;
        }

        if (file.isFile()) {
            results = new FileStatus[] {new LocalFileStatus(file, SCHEME)};
        } else {
            String[] names = file.list();
            if (names != null) {
                List<FileStatus> fileList = new ArrayList<>(names.length);
                for (String name : names) {
                    try {
                        fileList.add(getFileStatus(new Path(path, name)));
                    } catch (FileNotFoundException ignore) {
                        // ignore the files not found since the dir list may have changed since the
                        // names[] list was generated.
                    }
                }
                results = fileList.toArray(new FileStatus[0]);
            }
        }

        return results;
    }

    @Override
    public boolean exists(Path path) throws IOException {
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
            // Important: The 'exists()' check above must come before the 'isDirectory()' check to
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

    /**
     * Converts the given Path to a File for this file system. If the path is empty, we will return
     * <tt>new File(".")</tt> instead of <tt>new File("")</tt>, since the latter returns
     * <tt>false</tt> for <tt>isDirectory</tt> judgement.
     */
    public File toFile(Path path) {
        // remove scheme
        String localPath = path.toUri().getPath();
        checkState(localPath != null, "Cannot convert a null path to File");

        if (localPath.length() == 0) {
            return new File(".");
        }

        return new File(localPath);
    }

    /** Local {@link SeekableInputStream}. */
    public static class LocalSeekableInputStream extends SeekableInputStream
            implements VectoredReadable {

        private final FileInputStream in;
        private final FileChannel channel;

        public LocalSeekableInputStream(File file) throws FileNotFoundException {
            this.in = new FileInputStream(file);
            this.channel = in.getChannel();
        }

        @Override
        public void seek(long desired) throws IOException {
            if (desired != getPos()) {
                this.channel.position(desired);
            }
        }

        @Override
        public long getPos() throws IOException {
            return channel.position();
        }

        @Override
        public int read() throws IOException {
            return in.read();
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            return in.read(b, off, len);
        }

        @Override
        public void close() throws IOException {
            in.close();
        }

        @Override
        public int pread(long position, byte[] b, int off, int len) throws IOException {
            if (len == 0) {
                return 0;
            }

            return channel.read(ByteBuffer.wrap(b, off, len), position);
        }
    }

    /** Local {@link PositionOutputStream}. */
    public static class LocalPositionOutputStream extends PositionOutputStream {

        private final FileOutputStream out;

        public LocalPositionOutputStream(File file) throws FileNotFoundException {
            this.out = new FileOutputStream(file);
        }

        @Override
        public long getPos() throws IOException {
            return out.getChannel().position();
        }

        @Override
        public void write(int b) throws IOException {
            out.write(b);
        }

        @Override
        public void write(byte[] b) throws IOException {
            out.write(b);
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            out.write(b, off, len);
        }

        @Override
        public void flush() throws IOException {
            out.flush();
        }

        @Override
        public void close() throws IOException {
            out.close();
        }
    }

    private static class LocalFileStatus implements FileStatus {

        private final File file;
        private final long length;
        private final String scheme;

        private LocalFileStatus(File file, String scheme) {
            this.file = file;
            this.length = file.length();
            this.scheme = scheme;
        }

        @Override
        public long getLen() {
            return length;
        }

        @Override
        public boolean isDir() {
            return file.isDirectory();
        }

        @Override
        public Path getPath() {
            return new Path(scheme + ":" + file.toURI().getPath());
        }

        @Override
        public long getModificationTime() {
            return file.lastModified();
        }
    }
}
