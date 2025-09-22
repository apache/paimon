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
import org.apache.paimon.format.FormatReaderFactory;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.FileStatus;
import org.apache.paimon.fs.OffsetSeekableInputStream;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.reader.FileRecordReader;
import org.apache.paimon.utils.CompactedChangelogPathResolver;
import org.apache.paimon.utils.RoaringBitmap32;

import java.io.IOException;

/**
 * {@link FormatReaderFactory} for compacted changelog.
 *
 * <p>Uses {@link org.apache.paimon.utils.CompactedChangelogPathResolver} for file name protocol
 * handling.
 */
public class CompactedChangelogFormatReaderFactory implements FormatReaderFactory {

    private final FormatReaderFactory wrapped;

    public CompactedChangelogFormatReaderFactory(FormatReaderFactory wrapped) {
        this.wrapped = wrapped;
    }

    @Override
    public FileRecordReader<InternalRow> createReader(Context context) throws IOException {
        OffsetReadOnlyFileIO fileIO = new OffsetReadOnlyFileIO(context.fileIO());
        long length = CompactedChangelogPathResolver.decodePath(context.filePath()).getLength();

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
                    public RoaringBitmap32 selection() {
                        return context.selection();
                    }
                });
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
            CompactedChangelogPathResolver.DecodeResult result =
                    CompactedChangelogPathResolver.decodePath(path);
            return new OffsetSeekableInputStream(
                    wrapped.newInputStream(result.getPath()),
                    result.getOffset(),
                    result.getLength());
        }

        @Override
        public PositionOutputStream newOutputStream(Path path, boolean overwrite)
                throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public FileStatus getFileStatus(Path path) throws IOException {
            CompactedChangelogPathResolver.DecodeResult result =
                    CompactedChangelogPathResolver.decodePath(path);
            FileStatus status = wrapped.getFileStatus(result.getPath());

            return new FileStatus() {

                @Override
                public long getLen() {
                    return result.getLength();
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
            return wrapped.exists(CompactedChangelogPathResolver.decodePath(path).getPath());
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
}
