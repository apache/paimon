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

package org.apache.paimon.fs.hadoop;

import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.FileStatus;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.hadoop.SerializableConfiguration;
import org.apache.paimon.utils.FunctionWithException;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.ReflectionUtils;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Options;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

/** Hadoop {@link FileIO}. */
public class HadoopFileIO implements FileIO {

    private static final long serialVersionUID = 1L;

    protected SerializableConfiguration hadoopConf;

    protected transient volatile Map<Pair<String, String>, FileSystem> fsMap;

    @VisibleForTesting
    public void setFileSystem(Path path, FileSystem fs) throws IOException {
        org.apache.hadoop.fs.Path hadoopPath = path(path);
        getFileSystem(hadoopPath, p -> fs);
    }

    @Override
    public boolean isObjectStore() {
        return false;
    }

    @Override
    public void configure(CatalogContext context) {
        this.hadoopConf = new SerializableConfiguration(context.hadoopConf());
    }

    @Override
    public SeekableInputStream newInputStream(Path path) throws IOException {
        org.apache.hadoop.fs.Path hadoopPath = path(path);
        return new HadoopSeekableInputStream(getFileSystem(hadoopPath).open(hadoopPath));
    }

    @Override
    public PositionOutputStream newOutputStream(Path path, boolean overwrite) throws IOException {
        org.apache.hadoop.fs.Path hadoopPath = path(path);
        return new HadoopPositionOutputStream(
                getFileSystem(hadoopPath).create(hadoopPath, overwrite));
    }

    @Override
    public FileStatus getFileStatus(Path path) throws IOException {
        org.apache.hadoop.fs.Path hadoopPath = path(path);
        return new HadoopFileStatus(getFileSystem(hadoopPath).getFileStatus(hadoopPath));
    }

    @Override
    public FileStatus[] listStatus(Path path) throws IOException {
        org.apache.hadoop.fs.Path hadoopPath = path(path);
        FileStatus[] statuses = new FileStatus[0];
        org.apache.hadoop.fs.FileStatus[] hadoopStatuses =
                getFileSystem(hadoopPath).listStatus(hadoopPath);
        if (hadoopStatuses != null) {
            statuses = new FileStatus[hadoopStatuses.length];
            for (int i = 0; i < hadoopStatuses.length; i++) {
                statuses[i] = new HadoopFileStatus(hadoopStatuses[i]);
            }
        }
        return statuses;
    }

    @Override
    public boolean exists(Path path) throws IOException {
        org.apache.hadoop.fs.Path hadoopPath = path(path);
        return getFileSystem(hadoopPath).exists(hadoopPath);
    }

    @Override
    public boolean delete(Path path, boolean recursive) throws IOException {
        org.apache.hadoop.fs.Path hadoopPath = path(path);
        return getFileSystem(hadoopPath).delete(hadoopPath, recursive);
    }

    @Override
    public boolean mkdirs(Path path) throws IOException {
        org.apache.hadoop.fs.Path hadoopPath = path(path);
        return getFileSystem(hadoopPath).mkdirs(hadoopPath);
    }

    @Override
    public boolean rename(Path src, Path dst) throws IOException {
        org.apache.hadoop.fs.Path hadoopSrc = path(src);
        org.apache.hadoop.fs.Path hadoopDst = path(dst);
        return getFileSystem(hadoopSrc).rename(hadoopSrc, hadoopDst);
    }

    @Override
    public void overwriteFileUtf8(Path path, String content) throws IOException {
        boolean success = tryAtomicOverwriteViaRename(path, content);
        if (!success) {
            FileIO.super.overwriteFileUtf8(path, content);
        }
    }

    private org.apache.hadoop.fs.Path path(Path path) {
        return new org.apache.hadoop.fs.Path(path.toUri());
    }

    private FileSystem getFileSystem(org.apache.hadoop.fs.Path path) throws IOException {
        return getFileSystem(path, this::createFileSystem);
    }

    private FileSystem getFileSystem(
            org.apache.hadoop.fs.Path path,
            FunctionWithException<org.apache.hadoop.fs.Path, FileSystem, IOException> creator)
            throws IOException {
        if (fsMap == null) {
            synchronized (this) {
                if (fsMap == null) {
                    fsMap = new ConcurrentHashMap<>();
                }
            }
        }

        Map<Pair<String, String>, FileSystem> map = fsMap;

        URI uri = path.toUri();
        String scheme = uri.getScheme();
        String authority = uri.getAuthority();
        Pair<String, String> key = Pair.of(scheme, authority);
        FileSystem fs = map.get(key);
        if (fs == null) {
            fs = creator.apply(path);
            map.put(key, fs);
        }
        return fs;
    }

    protected FileSystem createFileSystem(org.apache.hadoop.fs.Path path) throws IOException {
        return path.getFileSystem(hadoopConf.get());
    }

    private static class HadoopSeekableInputStream extends SeekableInputStream {

        /**
         * Minimum amount of bytes to skip forward before we issue a seek instead of discarding
         * read.
         *
         * <p>The current value is just a magic number. In the long run, this value could become
         * configurable, but for now it is a conservative, relatively small value that should bring
         * safe improvements for small skips (e.g. in reading meta data), that would hurt the most
         * with frequent seeks.
         *
         * <p>The optimal value depends on the DFS implementation and configuration plus the
         * underlying filesystem. For now, this number is chosen "big enough" to provide
         * improvements for smaller seeks, and "small enough" to avoid disadvantages over real
         * seeks. While the minimum should be the page size, a true optimum per system would be the
         * amounts of bytes the can be consumed sequentially within the seektime. Unfortunately,
         * seektime is not constant and devices, OS, and DFS potentially also use read buffers and
         * read-ahead.
         */
        private static final int MIN_SKIP_BYTES = 1024 * 1024;

        private final FSDataInputStream in;

        private HadoopSeekableInputStream(FSDataInputStream in) {
            this.in = in;
        }

        @Override
        public void seek(long seekPos) throws IOException {
            // We do some optimizations to avoid that some implementations of distributed FS perform
            // expensive seeks when they are actually not needed.
            long delta = seekPos - getPos();

            if (delta > 0L && delta <= MIN_SKIP_BYTES) {
                // Instead of a small forward seek, we skip over the gap
                skipFully(delta);
            } else if (delta != 0L) {
                // For larger gaps and backward seeks, we do a real seek
                forceSeek(seekPos);
            } // Do nothing if delta is zero.
        }

        @Override
        public long getPos() throws IOException {
            return in.getPos();
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

        /**
         * Positions the stream to the given location. In contrast to {@link #seek(long)}, this
         * method will always issue a "seek" command to the dfs and may not replace it by {@link
         * #skip(long)} for small seeks.
         *
         * <p>Notice that the underlying DFS implementation can still decide to do skip instead of
         * seek.
         *
         * @param seekPos the position to seek to.
         */
        public void forceSeek(long seekPos) throws IOException {
            in.seek(seekPos);
        }

        /**
         * Skips over a given amount of bytes in the stream.
         *
         * @param bytes the number of bytes to skip.
         */
        public void skipFully(long bytes) throws IOException {
            while (bytes > 0) {
                bytes -= in.skip(bytes);
            }
        }
    }

    private static class HadoopPositionOutputStream extends PositionOutputStream {

        private final FSDataOutputStream out;

        private HadoopPositionOutputStream(FSDataOutputStream out) {
            this.out = out;
        }

        @Override
        public long getPos() throws IOException {
            return out.getPos();
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
            out.hflush();
        }

        @Override
        public void close() throws IOException {
            out.close();
        }
    }

    private static class HadoopFileStatus implements FileStatus {

        private final org.apache.hadoop.fs.FileStatus status;

        private HadoopFileStatus(org.apache.hadoop.fs.FileStatus status) {
            this.status = status;
        }

        @Override
        public long getLen() {
            return status.getLen();
        }

        @Override
        public boolean isDir() {
            return status.isDirectory();
        }

        @Override
        public Path getPath() {
            return new Path(status.getPath().toUri());
        }

        @Override
        public long getModificationTime() {
            return status.getModificationTime();
        }
    }

    // ============================== extra methods ===================================

    private transient volatile AtomicReference<Method> renameMethodRef;

    public boolean tryAtomicOverwriteViaRename(Path dst, String content) throws IOException {
        org.apache.hadoop.fs.Path hadoopDst = path(dst);
        FileSystem fs = getFileSystem(hadoopDst);

        if (renameMethodRef == null) {
            synchronized (this) {
                if (renameMethodRef == null) {
                    Method method;
                    // Implementation in FileSystem is incorrect, not atomic, Object Storage like S3
                    // and OSS not override it
                    // DistributedFileSystem and ViewFileSystem override the rename method to public
                    // and implement correct renaming
                    try {
                        method = ReflectionUtils.getMethod(fs.getClass(), "rename", 3);
                    } catch (NoSuchMethodException e) {
                        method = null;
                    }
                    renameMethodRef = new AtomicReference<>(method);
                }
            }
        }

        Method renameMethod = renameMethodRef.get();
        if (renameMethod == null) {
            return false;
        }

        boolean renameDone = false;

        // write tempPath
        Path tempPath = dst.createTempPath();
        org.apache.hadoop.fs.Path hadoopTemp = path(tempPath);
        try {
            try (PositionOutputStream out = newOutputStream(tempPath, false)) {
                OutputStreamWriter writer = new OutputStreamWriter(out, StandardCharsets.UTF_8);
                writer.write(content);
                writer.flush();
            }

            renameMethod.invoke(
                    fs, hadoopTemp, hadoopDst, new Options.Rename[] {Options.Rename.OVERWRITE});
            renameDone = true;
            // TODO: this is a workaround of HADOOP-16255 - remove this when HADOOP-16255 is
            // resolved
            tryRemoveCrcFile(hadoopTemp);
            return true;
        } catch (InvocationTargetException | IllegalAccessException e) {
            throw new IOException(e);
        } finally {
            if (!renameDone) {
                deleteQuietly(tempPath);
            }
        }
    }

    /** @throws IOException if a fatal exception occurs. Will try to ignore most exceptions. */
    @SuppressWarnings("CatchMayIgnoreException")
    private void tryRemoveCrcFile(org.apache.hadoop.fs.Path path) throws IOException {
        try {
            final org.apache.hadoop.fs.Path checksumFile =
                    new org.apache.hadoop.fs.Path(
                            path.getParent(), String.format(".%s.crc", path.getName()));

            FileSystem fs = getFileSystem(checksumFile);
            if (fs.exists(checksumFile)) {
                // checksum file exists, deleting it
                fs.delete(checksumFile, true); // recursive=true
            }
        } catch (Throwable t) {
            if (t instanceof VirtualMachineError
                    || t instanceof ThreadDeath
                    || t instanceof LinkageError) {
                throw t;
            }
            // else, ignore - we are removing crc file as "best-effort"
        }
    }
}
