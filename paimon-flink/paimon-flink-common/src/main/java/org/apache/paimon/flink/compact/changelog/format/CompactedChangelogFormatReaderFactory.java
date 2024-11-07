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

package org.apache.paimon.flink.compact.changelog.format;

import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fileindex.FileIndexResult;
import org.apache.paimon.format.FormatReaderFactory;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.FileStatus;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.reader.RecordReader;

import java.io.EOFException;
import java.io.IOException;

/**
 * {@link FormatReaderFactory} for compacted changelog.
 *
 * <p><b>File Name Protocol</b>
 *
 * <p>There are two kinds of file name. In the following description, <code>bid1</code> and <code>
 * bid2</code> are bucket id, <code>off</code> is offset, <code>len1</code> and <code>len2</code>
 * are lengths.
 *
 * <ul>
 *   <li><code>bucket-bid1/compacted-changelog-xxx$bid1-len1</code>: This is the real file name. If
 *       this file name is recorded in manifest file meta, reader should read the bytes of this file
 *       starting from offset <code>0</code> with length <code>len1</code>.
 *   <li><code>bucket-bid2/compacted-changelog-xxx$bid1-len1-off-len2</code>: This is the fake file
 *       name. Reader should read the bytes of file <code>
 *       bucket-bid1/compacted-changelog-xxx$bid1-len1</code> starting from offset <code>off</code>
 *       with length <code>len2</code>.
 * </ul>
 */
public class CompactedChangelogFormatReaderFactory implements FormatReaderFactory {

    private final FormatReaderFactory wrapped;

    public CompactedChangelogFormatReaderFactory(FormatReaderFactory wrapped) {
        this.wrapped = wrapped;
    }

    @Override
    public RecordReader<InternalRow> createReader(Context context) throws IOException {
        OffsetReadOnlyFileIO fileIO = new OffsetReadOnlyFileIO(context.fileIO());
        long length = decodePath(context.filePath()).length;

        return wrapped.createReader(
                new Context() {

                    @Override
                    public FileIO fileIO() {
                        return fileIO;
                    }

                    @Override
                    public Path filePath() {
                        return context.filePath();
                    }

                    @Override
                    public long fileSize() {
                        return length;
                    }

                    @Override
                    public FileIndexResult fileIndex() {
                        return context.fileIndex();
                    }
                });
    }

    private static DecodeResult decodePath(Path path) {
        String[] nameAndFormat = path.getName().split("\\.");
        String[] names = nameAndFormat[0].split("\\$");
        String[] split = names[1].split("-");
        if (split.length == 2) {
            return new DecodeResult(path, 0, Long.parseLong(split[1]));
        } else {
            Path realPath =
                    new Path(
                            path.getParent().getParent(),
                            "bucket-"
                                    + split[0]
                                    + "/"
                                    + names[0]
                                    + "$"
                                    + split[0]
                                    + "-"
                                    + split[1]
                                    + "."
                                    + nameAndFormat[1]);
            return new DecodeResult(realPath, Long.parseLong(split[2]), Long.parseLong(split[3]));
        }
    }

    private static class DecodeResult {

        private final Path path;
        private final long offset;
        private final long length;

        private DecodeResult(Path path, long offset, long length) {
            this.path = path;
            this.offset = offset;
            this.length = length;
        }
    }

    private static class OffsetReadOnlyFileIO implements FileIO {

        private final FileIO wrapped;

        private OffsetReadOnlyFileIO(FileIO wrapped) {
            this.wrapped = wrapped;
        }

        @Override
        public boolean isObjectStore() {
            return wrapped.isObjectStore();
        }

        @Override
        public void configure(CatalogContext context) {
            wrapped.configure(context);
        }

        @Override
        public SeekableInputStream newInputStream(Path path) throws IOException {
            DecodeResult result = decodePath(path);
            return new OffsetSeekableInputStream(
                    wrapped.newInputStream(result.path), result.offset, result.length);
        }

        @Override
        public PositionOutputStream newOutputStream(Path path, boolean overwrite)
                throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public FileStatus getFileStatus(Path path) throws IOException {
            DecodeResult result = decodePath(path);
            FileStatus status = wrapped.getFileStatus(result.path);

            return new FileStatus() {

                @Override
                public long getLen() {
                    return result.length;
                }

                @Override
                public boolean isDir() {
                    return status.isDir();
                }

                @Override
                public Path getPath() {
                    return path;
                }

                @Override
                public long getModificationTime() {
                    return status.getModificationTime();
                }
            };
        }

        @Override
        public FileStatus[] listStatus(Path path) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean exists(Path path) throws IOException {
            return wrapped.exists(decodePath(path).path);
        }

        @Override
        public boolean delete(Path path, boolean recursive) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean mkdirs(Path path) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean rename(Path src, Path dst) throws IOException {
            throw new UnsupportedOperationException();
        }
    }

    private static class OffsetSeekableInputStream extends SeekableInputStream {

        private final SeekableInputStream wrapped;
        private final long offset;
        private final long length;

        private OffsetSeekableInputStream(SeekableInputStream wrapped, long offset, long length)
                throws IOException {
            this.wrapped = wrapped;
            this.offset = offset;
            this.length = length;
            wrapped.seek(offset);
        }

        @Override
        public void seek(long desired) throws IOException {
            wrapped.seek(offset + desired);
        }

        @Override
        public long getPos() throws IOException {
            return wrapped.getPos() - offset;
        }

        @Override
        public int read() throws IOException {
            if (getPos() >= length) {
                throw new EOFException();
            }
            return wrapped.read();
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            long realLen = Math.min(len, length - getPos());
            return wrapped.read(b, off, (int) realLen);
        }

        @Override
        public void close() throws IOException {
            wrapped.close();
        }
    }
}
