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
import org.apache.paimon.data.BlobDescriptor;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** Test-only {@link FileIO} which checks the portable input domain of selected operations. */
public final class StrictContractFileIO implements FileIO {

    private static final long serialVersionUID = 1L;

    private final FileIO delegate;

    public StrictContractFileIO(FileIO delegate) {
        this.delegate = delegate;
    }

    @Override
    public boolean isObjectStore() {
        return delegate.isObjectStore();
    }

    @Override
    public void configure(CatalogContext context) {
        delegate.configure(context);
    }

    @Override
    public void setRuntimeContext(Map<String, String> options) {
        delegate.setRuntimeContext(options);
    }

    @Override
    public SeekableInputStream newInputStream(Path path) throws IOException {
        return delegate.newInputStream(path);
    }

    @Override
    public PositionOutputStream newOutputStream(Path path, boolean overwrite) throws IOException {
        return delegate.newOutputStream(path, overwrite);
    }

    @Override
    public TwoPhaseOutputStream newTwoPhaseOutputStream(Path path, boolean overwrite)
            throws IOException {
        return new ForwardingTwoPhaseOutputStream(
                delegate.newTwoPhaseOutputStream(path, overwrite));
    }

    @Override
    public FileStatus getFileStatus(Path path) throws IOException {
        return delegate.getFileStatus(path);
    }

    @Override
    public FileStatus[] listStatus(Path path) throws IOException {
        requireDirectory(path, "listStatus");
        return delegate.listStatus(path);
    }

    @Override
    public FileStatus[] listFiles(Path path, boolean recursive) throws IOException {
        requireDirectory(path, "listFiles");
        return delegate.listFiles(path, recursive);
    }

    @Override
    public RemoteIterator<FileStatus> listFilesIterative(Path path, boolean recursive)
            throws IOException {
        requireDirectory(path, "listFilesIterative");
        return delegate.listFilesIterative(path, recursive);
    }

    @Override
    public FileStatus[] listDirectories(Path path) throws IOException {
        requireDirectory(path, "listDirectories");
        return delegate.listDirectories(path);
    }

    @Override
    public boolean exists(Path path) throws IOException {
        return delegate.exists(path);
    }

    @Override
    public boolean delete(Path path, boolean recursive) throws IOException {
        return delegate.delete(path, recursive);
    }

    @Override
    public boolean mkdirs(Path path) throws IOException {
        return delegate.mkdirs(path);
    }

    @Override
    public boolean rename(Path src, Path dst) throws IOException {
        requireDistinctPaths(src, dst);
        requireExisting(src, "rename source");
        requireMissing(dst, "rename destination");
        Path parent = dst.getParent();
        if (parent == null) {
            throw violation("rename destination has no parent: " + dst);
        }
        requireDirectory(parent, "rename destination parent");
        return delegate.rename(src, dst);
    }

    @Override
    public Optional<Path> archive(Path path, StorageType type) throws IOException {
        return delegate.archive(path, type);
    }

    @Override
    public void restoreArchive(Path path, Duration duration) throws IOException {
        delegate.restoreArchive(path, duration);
    }

    @Override
    public Optional<Path> unarchive(Path path, StorageType type) throws IOException {
        return delegate.unarchive(path, type);
    }

    @Override
    public String createBlobPresignedUrl(
            Path tableRoot, BlobDescriptor descriptor, Duration validity) throws IOException {
        return delegate.createBlobPresignedUrl(tableRoot, descriptor, validity);
    }

    @Override
    public void close() throws IOException {
        delegate.close();
    }

    @Override
    public void deleteQuietly(Path file) {
        delegate.deleteQuietly(file);
    }

    @Override
    public void deleteFilesQuietly(List<Path> files) {
        delegate.deleteFilesQuietly(files);
    }

    @Override
    public void deleteDirectoryQuietly(Path directory) {
        delegate.deleteDirectoryQuietly(directory);
    }

    @Override
    public long getFileSize(Path path) throws IOException {
        return delegate.getFileSize(path);
    }

    @Override
    public boolean isDir(Path path) throws IOException {
        return delegate.isDir(path);
    }

    @Override
    public void checkOrMkdirs(Path path) throws IOException {
        delegate.checkOrMkdirs(path);
    }

    @Override
    public String readFileUtf8(Path path) throws IOException {
        return delegate.readFileUtf8(path);
    }

    @Override
    public boolean tryToWriteAtomic(Path path, String content) throws IOException {
        return delegate.tryToWriteAtomic(path, content);
    }

    @Override
    public void writeFile(Path path, String content, boolean overwrite) throws IOException {
        delegate.writeFile(path, content, overwrite);
    }

    @Override
    public void overwriteFileUtf8(Path path, String content) throws IOException {
        delegate.overwriteFileUtf8(path, content);
    }

    @Override
    public void overwriteHintFile(Path path, String content) throws IOException {
        delegate.overwriteHintFile(path, content);
    }

    @Override
    public void copyFile(Path sourcePath, Path targetPath, boolean overwrite) throws IOException {
        delegate.copyFile(sourcePath, targetPath, overwrite);
    }

    @Override
    public void copyFiles(Path sourceDirectory, Path targetDirectory, boolean overwrite)
            throws IOException {
        requireDirectory(sourceDirectory, "copyFiles source");
        delegate.copyFiles(sourceDirectory, targetDirectory, overwrite);
    }

    @Override
    public Optional<String> readOverwrittenFileUtf8(Path path) throws IOException {
        return delegate.readOverwrittenFileUtf8(path);
    }

    private void requireExisting(Path path, String operation) throws IOException {
        if (!delegate.exists(path)) {
            throw violation(operation + " does not exist: " + path);
        }
    }

    private void requireMissing(Path path, String operation) throws IOException {
        if (delegate.exists(path)) {
            throw violation(operation + " already exists: " + path);
        }
    }

    private void requireDirectory(Path path, String operation) throws IOException {
        final FileStatus status;
        try {
            status = delegate.getFileStatus(path);
        } catch (IOException e) {
            throw violation(operation + " requires an existing directory: " + path, e);
        }
        if (!status.isDir()) {
            throw violation(operation + " requires a directory: " + path);
        }
    }

    private static void requireDistinctPaths(Path src, Path dst) {
        if (src.equals(dst)) {
            throw violation("rename source and destination are the same path: " + src);
        }
    }

    private static AssertionError violation(String message) {
        return new AssertionError(message);
    }

    private static AssertionError violation(String message, Exception cause) {
        return new AssertionError(message, cause);
    }

    private static FileIO unwrap(FileIO fileIO) {
        return fileIO instanceof StrictContractFileIO
                ? ((StrictContractFileIO) fileIO).delegate
                : fileIO;
    }

    private static final class ForwardingTwoPhaseOutputStream extends TwoPhaseOutputStream {

        private final TwoPhaseOutputStream delegate;

        private ForwardingTwoPhaseOutputStream(TwoPhaseOutputStream delegate) {
            this.delegate = delegate;
        }

        @Override
        public void write(int b) throws IOException {
            delegate.write(b);
        }

        @Override
        public void write(byte[] b) throws IOException {
            delegate.write(b);
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            delegate.write(b, off, len);
        }

        @Override
        public void flush() throws IOException {
            delegate.flush();
        }

        @Override
        public long getPos() throws IOException {
            return delegate.getPos();
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }

        @Override
        public Committer closeForCommit() throws IOException {
            return new UnwrappingCommitter(delegate.closeForCommit());
        }
    }

    private static final class UnwrappingCommitter implements TwoPhaseOutputStream.Committer {

        private static final long serialVersionUID = 1L;

        private final TwoPhaseOutputStream.Committer delegate;

        private UnwrappingCommitter(TwoPhaseOutputStream.Committer delegate) {
            this.delegate = delegate;
        }

        @Override
        public void commit(FileIO fileIO) throws IOException {
            delegate.commit(unwrap(fileIO));
        }

        @Override
        public void discard(FileIO fileIO) throws IOException {
            delegate.discard(unwrap(fileIO));
        }

        @Override
        public Path targetPath() {
            return delegate.targetPath();
        }

        @Override
        public void clean(FileIO fileIO) throws IOException {
            delegate.clean(unwrap(fileIO));
        }
    }
}
