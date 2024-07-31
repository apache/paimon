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

package org.apache.paimon.format.fs;

import org.apache.paimon.cache.BlockCacheConfig;
import org.apache.paimon.cache.CachedSeekableInputStream;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.SeekableInputStream;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;

import java.io.IOException;
import java.net.URI;

/** A read only {@link FileSystem} that wraps an {@link FileIO}. */
public class HadoopReadOnlyFileSystem extends FileSystem {

    private final FileIO fileIO;
    private final BlockCacheConfig blockCacheConfig;

    public HadoopReadOnlyFileSystem(FileIO fileIO, BlockCacheConfig blockCacheConfig) {
        this.fileIO = fileIO;
        this.blockCacheConfig = blockCacheConfig;
    }

    @Override
    public URI getUri() {
        throw new UnsupportedOperationException();
    }

    @Override
    public FSDataInputStream open(Path path) throws IOException {
        SeekableInputStream inputStream = fileIO.newInputStream(toPaimonPath(path));
        if (blockCacheConfig.enabled) {
            inputStream =
                    new CachedSeekableInputStream(
                            inputStream,
                            toPaimonPath(path),
                            fileIO.getFileSize(toPaimonPath(path)),
                            blockCacheConfig);
        }
        return new FSDataInputStream(new FSDataWrappedInputStream(inputStream));
    }

    @Override
    public FSDataInputStream open(Path path, int bufferSize) throws IOException {
        return open(path);
    }

    @Override
    public FileStatus getFileStatus(Path path) throws IOException {
        return toHadoopStatus(fileIO.getFileStatus(toPaimonPath(path)));
    }

    private static org.apache.paimon.fs.Path toPaimonPath(Path path) {
        return new org.apache.paimon.fs.Path(path.toUri());
    }

    private static Path toHadoopPath(org.apache.paimon.fs.Path path) {
        return new Path(path.toUri());
    }

    private static FileStatus toHadoopStatus(org.apache.paimon.fs.FileStatus status) {
        return new FileStatus(
                status.getLen(),
                status.isDir(),
                0,
                0,
                0,
                0,
                null,
                null,
                null,
                toHadoopPath(status.getPath()));
    }

    // --------------------- unsupported methods ----------------------------

    @Override
    public FSDataOutputStream create(
            Path f,
            FsPermission permission,
            boolean overwrite,
            int bufferSize,
            short replication,
            long blockSize,
            Progressable progress)
            throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public FSDataOutputStream append(Path f, int bufferSize, Progressable progress)
            throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean rename(Path src, Path dst) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean delete(Path path, boolean recursive) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public FileStatus[] listStatus(Path path) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setWorkingDirectory(Path path) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Path getWorkingDirectory() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean mkdirs(Path path, FsPermission fsPermission) {
        throw new UnsupportedOperationException();
    }
}
