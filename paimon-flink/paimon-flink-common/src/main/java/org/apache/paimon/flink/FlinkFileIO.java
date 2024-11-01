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

package org.apache.paimon.flink;

import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.FileStatus;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.SeekableInputStream;

import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.core.fs.FileSystemKind;

import java.io.IOException;
import java.io.UncheckedIOException;

/** Flink {@link FileIO} to use {@link FileSystem}. */
public class FlinkFileIO implements FileIO {

    private static final long serialVersionUID = 1L;

    private final org.apache.flink.core.fs.Path path;

    public FlinkFileIO(Path path) {
        this.path = path(path);
    }

    @Override
    public boolean isObjectStore() {
        try {
            return path.getFileSystem().getKind() != FileSystemKind.FILE_SYSTEM;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void configure(CatalogContext context) {}

    @Override
    public SeekableInputStream newInputStream(Path path) throws IOException {
        org.apache.flink.core.fs.Path flinkPath = path(path);
        return new FlinkSeekableInputStream(getFileSystem(flinkPath).open(flinkPath));
    }

    @Override
    public PositionOutputStream newOutputStream(Path path, boolean overwrite) throws IOException {
        org.apache.flink.core.fs.Path flinkPath = path(path);
        return new FlinkPositionOutputStream(
                getFileSystem(flinkPath)
                        .create(
                                flinkPath,
                                overwrite ? WriteMode.OVERWRITE : WriteMode.NO_OVERWRITE));
    }

    @Override
    public FileStatus getFileStatus(Path path) throws IOException {
        org.apache.flink.core.fs.Path flinkPath = path(path);
        return new FlinkFileStatus(getFileSystem(flinkPath).getFileStatus(flinkPath));
    }

    @Override
    public FileStatus[] listStatus(Path path) throws IOException {
        org.apache.flink.core.fs.Path flinkPath = path(path);
        FileStatus[] statuses = new FileStatus[0];
        org.apache.flink.core.fs.FileStatus[] flinkStatuses =
                getFileSystem(flinkPath).listStatus(flinkPath);
        if (flinkStatuses != null) {
            statuses = new FileStatus[flinkStatuses.length];
            for (int i = 0; i < flinkStatuses.length; i++) {
                statuses[i] = new FlinkFileStatus(flinkStatuses[i]);
            }
        }
        return statuses;
    }

    @Override
    public boolean exists(Path path) throws IOException {
        org.apache.flink.core.fs.Path flinkPath = path(path);
        return getFileSystem(flinkPath).exists(flinkPath);
    }

    @Override
    public boolean delete(Path path, boolean recursive) throws IOException {
        org.apache.flink.core.fs.Path flinkPath = path(path);
        return getFileSystem(flinkPath).delete(flinkPath, recursive);
    }

    @Override
    public boolean mkdirs(Path path) throws IOException {
        org.apache.flink.core.fs.Path flinkPath = path(path);
        return getFileSystem(flinkPath).mkdirs(flinkPath);
    }

    @Override
    public boolean rename(Path src, Path dst) throws IOException {
        org.apache.flink.core.fs.Path flinkSrc = path(src);
        org.apache.flink.core.fs.Path flinkDst = path(dst);
        return getFileSystem(flinkSrc).rename(flinkSrc, flinkDst);
    }

    private org.apache.flink.core.fs.Path path(Path path) {
        return new org.apache.flink.core.fs.Path(path.toUri());
    }

    protected FileSystem getFileSystem(org.apache.flink.core.fs.Path path) throws IOException {
        return path.getFileSystem();
    }

    private static class FlinkSeekableInputStream extends SeekableInputStream {

        private final FSDataInputStream in;

        private FlinkSeekableInputStream(FSDataInputStream in) {
            this.in = in;
        }

        @Override
        public void seek(long seekPos) throws IOException {
            in.seek(seekPos);
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
    }

    private static class FlinkPositionOutputStream extends PositionOutputStream {

        private final FSDataOutputStream out;

        private FlinkPositionOutputStream(FSDataOutputStream out) {
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
            out.flush();
        }

        @Override
        public void close() throws IOException {
            out.close();
        }
    }

    private static class FlinkFileStatus implements FileStatus {

        private final org.apache.flink.core.fs.FileStatus status;

        private FlinkFileStatus(org.apache.flink.core.fs.FileStatus status) {
            this.status = status;
        }

        @Override
        public long getLen() {
            return status.getLen();
        }

        @Override
        public boolean isDir() {
            return status.isDir();
        }

        @Override
        public Path getPath() {
            return new Path(status.getPath().toUri());
        }

        @Override
        public long getModificationTime() {
            return status.getModificationTime();
        }

        @Override
        public long getAccessTime() {
            return status.getAccessTime();
        }
    }
}
