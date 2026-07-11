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

package org.apache.paimon.index.pkvector;

import org.apache.paimon.data.InternalArray;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.index.GlobalIndexMeta;
import org.apache.paimon.index.IndexFile;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.index.IndexPathFactory;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.FileWriterAbortExecutor;
import org.apache.paimon.io.KeyValueVectorSidecarWriter;
import org.apache.paimon.manifest.FileSource;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.paimon.utils.Preconditions.checkState;

/** Creates immutable row-position-addressable raw vector segments for data files. */
public class PkVectorRawSegmentFile extends IndexFile {

    public static final String PK_VECTOR_RAW = "pk-vector-raw";

    public PkVectorRawSegmentFile(FileIO fileIO, IndexPathFactory pathFactory) {
        super(fileIO, pathFactory);
    }

    RawVectorSidecarReader newReader(IndexFileMeta segment) throws IOException {
        return new RawVectorSidecarReader(fileIO, path(segment));
    }

    public KeyValueVectorSidecarWriter newWriter(
            int dimension,
            String indexDefinitionId,
            int vectorFieldId,
            BiConsumer<IndexFileMeta, FileSource> segmentConsumer,
            Consumer<IndexFileMeta> segmentAbortConsumer) {
        Path path = pathFactory.newPath();
        try {
            return new Writer(
                    new RawVectorSidecarWriter(fileIO, path, dimension),
                    indexDefinitionId,
                    vectorFieldId,
                    segmentConsumer,
                    segmentAbortConsumer);
        } catch (IOException e) {
            fileIO.deleteQuietly(path);
            throw new UncheckedIOException(
                    "Failed to create primary-key raw vector segment: " + path, e);
        }
    }

    private class Writer implements KeyValueVectorSidecarWriter {

        private final RawVectorSidecarWriter rawWriter;
        private final String indexDefinitionId;
        private final int vectorFieldId;
        private final BiConsumer<IndexFileMeta, FileSource> segmentConsumer;
        private final Consumer<IndexFileMeta> segmentAbortConsumer;

        @Nullable private IndexFileMeta completedSegment;
        private boolean closed;
        private boolean completed;

        private Writer(
                RawVectorSidecarWriter rawWriter,
                String indexDefinitionId,
                int vectorFieldId,
                BiConsumer<IndexFileMeta, FileSource> segmentConsumer,
                Consumer<IndexFileMeta> segmentAbortConsumer) {
            this.rawWriter = rawWriter;
            this.indexDefinitionId = indexDefinitionId;
            this.vectorFieldId = vectorFieldId;
            this.segmentConsumer = segmentConsumer;
            this.segmentAbortConsumer = segmentAbortConsumer;
        }

        @Override
        public void write(@Nullable InternalArray vector) throws IOException {
            rawWriter.write(vector);
        }

        @Override
        public void close() throws IOException {
            if (!closed) {
                rawWriter.close();
                closed = true;
            }
        }

        @Override
        public void complete(DataFileMeta sourceFile) throws IOException {
            checkState(closed, "Raw vector segment must be closed before completion.");
            checkState(!completed, "Raw vector segment is already completed.");
            checkArgument(
                    rawWriter.rowCount() == sourceFile.rowCount(),
                    "Raw vector segment row count %s does not match source file %s row count %s.",
                    rawWriter.rowCount(),
                    sourceFile.fileName(),
                    sourceFile.rowCount());

            PkVectorRawSegmentMeta metadata =
                    new PkVectorRawSegmentMeta(
                            indexDefinitionId,
                            new PkVectorSourceFile(sourceFile.fileName(), sourceFile.rowCount()));
            Path path = rawWriter.path();
            IndexFileMeta segment =
                    new IndexFileMeta(
                            PK_VECTOR_RAW,
                            path.getName(),
                            fileIO.getFileSize(path),
                            rawWriter.liveVectorCount(),
                            new GlobalIndexMeta(
                                    0,
                                    sourceFile.rowCount(),
                                    vectorFieldId,
                                    null,
                                    metadata.serialize()),
                            pathFactory.isExternalPath() ? path.toString() : null);
            segmentConsumer.accept(segment, sourceFile.fileSource().orElse(FileSource.APPEND));
            completedSegment = segment;
            completed = true;
        }

        @Override
        public void abort() {
            if (completedSegment != null) {
                segmentAbortConsumer.accept(completedSegment);
                completedSegment = null;
            }
            rawWriter.abort();
        }

        @Override
        public Optional<FileWriterAbortExecutor> abortExecutor() {
            return Optional.of(
                    new FileWriterAbortExecutor(fileIO, rawWriter.path()) {
                        @Override
                        public void abort() {
                            Writer.this.abort();
                        }
                    });
        }
    }
}
