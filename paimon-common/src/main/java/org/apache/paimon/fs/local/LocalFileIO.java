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
import org.apache.paimon.fs.FileRange;
import org.apache.paimon.fs.FileStatus;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.fs.VectoredReadable;

import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.CompletionHandler;
import java.nio.channels.FileChannel;
import java.nio.file.AccessDeniedException;
import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.IntFunction;

import static org.apache.paimon.fs.FileRange.validateAndSortRanges;
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

        private final File file;
        private final FileInputStream in;
        private final FileChannel channel;

        private AsynchronousFileChannel asyncChannel = null;

        public LocalSeekableInputStream(File file) throws FileNotFoundException {
            this.file = file;
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
            if (asyncChannel != null) {
                asyncChannel.close();
            }
        }

        AsynchronousFileChannel getAsyncChannel() throws IOException {
            if (asyncChannel == null) {
                synchronized (this) {
                    asyncChannel =
                            AsynchronousFileChannel.open(file.toPath(), StandardOpenOption.READ);
                }
            }
            return asyncChannel;
        }

        @Override
        public void readVectored(List<? extends FileRange> ranges, IntFunction<ByteBuffer> allocate)
                throws IOException {
            // Validate, but do not pass in a file length as it may change.
            List<? extends FileRange> sortedRanges =
                    validateAndSortRanges(ranges, Optional.empty());
            // Set up all futures, so that we can use them if things fail
            for (FileRange range : sortedRanges) {
                range.setData(new CompletableFuture<>());
            }
            try {
                AsynchronousFileChannel channel = getAsyncChannel();
                ByteBuffer[] buffers = new ByteBuffer[sortedRanges.size()];
                AsyncHandler asyncHandler = new AsyncHandler(channel, sortedRanges, buffers);
                for (int i = 0; i < sortedRanges.size(); ++i) {
                    FileRange range = sortedRanges.get(i);
                    buffers[i] = allocate.apply(range.getLength());
                    channel.read(buffers[i], range.getOffset(), i, asyncHandler);
                }
            } catch (IOException ioe) {
                LOG.debug("Exception occurred during vectored read ", ioe);
                for (FileRange range : sortedRanges) {
                    range.getData().completeExceptionally(ioe);
                }
            }
        }
    }

    /**
     * A CompletionHandler that implements readFully and translates back into the form of
     * CompletionHandler that our users expect.
     */
    static class AsyncHandler implements CompletionHandler<Integer, Integer> {
        private final AsynchronousFileChannel channel;
        private final List<? extends FileRange> ranges;
        private final ByteBuffer[] buffers;

        AsyncHandler(
                AsynchronousFileChannel channel,
                List<? extends FileRange> ranges,
                ByteBuffer[] buffers) {
            this.channel = channel;
            this.ranges = ranges;
            this.buffers = buffers;
        }

        @Override
        public void completed(Integer result, Integer r) {
            FileRange range = ranges.get(r);
            ByteBuffer buffer = buffers[r];
            if (result == -1) {
                failed(new EOFException("Read past End of File"), r);
            } else {
                if (buffer.remaining() > 0) {
                    // issue a read for the rest of the buffer
                    // QQ: What if this fails? It has the same handler.
                    channel.read(buffer, range.getOffset() + buffer.position(), r, this);
                } else {
                    // QQ: Why  is this required? I think because we don't want the
                    // user to read data beyond limit.
                    buffer.flip();
                    range.getData().complete(buffer);
                }
            }
        }

        @Override
        public void failed(Throwable exc, Integer r) {
            LOG.debug("Failed while reading range {} ", r, exc);
            ranges.get(r).getData().completeExceptionally(exc);
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
