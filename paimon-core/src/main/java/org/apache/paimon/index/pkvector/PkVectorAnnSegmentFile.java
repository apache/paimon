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

import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.globalindex.GlobalIndexSingleColumnWriter;
import org.apache.paimon.globalindex.GlobalIndexWriter;
import org.apache.paimon.globalindex.GlobalIndexer;
import org.apache.paimon.globalindex.ResultEntry;
import org.apache.paimon.globalindex.VectorGlobalIndexer;
import org.apache.paimon.globalindex.io.GlobalIndexFileWriter;
import org.apache.paimon.index.GlobalIndexMeta;
import org.apache.paimon.index.IndexFile;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.index.IndexPathFactory;
import org.apache.paimon.index.pk.PrimaryKeyIndexSourceFile;
import org.apache.paimon.index.pk.PrimaryKeyIndexSourceMeta;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.options.Options;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.VectorType;
import org.apache.paimon.utils.IOUtils;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.LongPredicate;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Builds immutable ANN payloads whose index ids are source data-file row positions. */
public class PkVectorAnnSegmentFile extends IndexFile {

    public PkVectorAnnSegmentFile(FileIO fileIO, IndexPathFactory pathFactory) {
        super(fileIO, pathFactory);
    }

    public IndexFileMeta build(
            List<Source> sources,
            DataField vectorField,
            Options indexOptions,
            String metric,
            String indexType)
            throws IOException {
        checkArgument(!sources.isEmpty(), "An ANN segment must reference source files.");
        long totalRowCount = 0;
        List<PrimaryKeyIndexSourceFile> sourceFiles = new ArrayList<>(sources.size());
        for (Source source : sources) {
            totalRowCount = Math.addExact(totalRowCount, source.sourceFile.rowCount());
            sourceFiles.add(source.sourceFile);
        }
        checkArgument(totalRowCount > 0, "An ANN segment must reference at least one source row.");

        GlobalIndexer indexer = GlobalIndexer.create(indexType, vectorField, indexOptions);
        checkArgument(
                indexer instanceof VectorGlobalIndexer,
                "Index algorithm %s does not implement VectorGlobalIndexer.",
                indexType);
        String indexerMetric = normalizeMetric(((VectorGlobalIndexer) indexer).metric());
        checkArgument(
                normalizeMetric(metric).equals(indexerMetric),
                "Configured metric %s does not match index algorithm metric %s.",
                metric,
                indexerMetric);

        SegmentFileWriter fileWriter = new SegmentFileWriter();
        GlobalIndexWriter writer = null;
        boolean success = false;
        try {
            writer = indexer.createWriter(fileWriter);
            checkArgument(
                    writer instanceof GlobalIndexSingleColumnWriter,
                    "Index algorithm %s does not create a single-column writer.",
                    indexType);
            GlobalIndexSingleColumnWriter singleColumnWriter =
                    (GlobalIndexSingleColumnWriter) writer;
            long liveRowCount = 0;
            long fileOffset = 0;
            int dimension = -1;
            for (Source source : sources) {
                PkVectorReader vectors = source.openReader();
                try {
                    checkArgument(
                            vectors.rowCount() == source.sourceFile.rowCount(),
                            "Vector row count %s does not match source file %s row count %s.",
                            vectors.rowCount(),
                            source.sourceFile.fileName(),
                            source.sourceFile.rowCount());
                    if (dimension < 0) {
                        dimension = vectors.dimension();
                    }
                    checkArgument(
                            vectors.dimension() == dimension,
                            "Vector source %s dimension %s does not match dimension %s.",
                            source.sourceFile.fileName(),
                            vectors.dimension(),
                            dimension);
                    if (vectorField.type() instanceof VectorType) {
                        checkArgument(
                                ((VectorType) vectorField.type()).getLength() == dimension,
                                "Vector field dimension %s does not match source vector dimension %s.",
                                ((VectorType) vectorField.type()).getLength(),
                                dimension);
                    }

                    float[] vector = new float[dimension];
                    for (long rowPosition = 0; rowPosition < vectors.rowCount(); rowPosition++) {
                        boolean present = vectors.readNextVector(vector);
                        if (!present || source.excludedPosition.test(rowPosition)) {
                            continue;
                        }
                        singleColumnWriter.write(vector, fileOffset + rowPosition);
                        liveRowCount++;
                    }
                } finally {
                    source.closeReader(vectors);
                }
                fileOffset += source.sourceFile.rowCount();
            }

            List<ResultEntry> results = writer.finish();
            if (liveRowCount == 0 && results.isEmpty()) {
                results = Collections.singletonList(fileWriter.emptyResult());
            }
            checkArgument(
                    results.size() == 1,
                    "ANN segment build must produce exactly one payload file, but produced %s.",
                    results.size());
            ResultEntry result = results.get(0);
            Path payloadPath = fileWriter.path(result.fileName());
            byte[] payloadMetadata = result.meta() == null ? new byte[0] : result.meta();
            IndexFileMeta segment =
                    new IndexFileMeta(
                            indexType,
                            result.fileName(),
                            fileIO.getFileSize(payloadPath),
                            liveRowCount,
                            new GlobalIndexMeta(
                                    0,
                                    totalRowCount - 1,
                                    vectorField.id(),
                                    null,
                                    payloadMetadata,
                                    new PrimaryKeyIndexSourceMeta(sourceFiles).serialize()),
                            pathFactory.isExternalPath() ? payloadPath.toString() : null);
            success = true;
            return segment;
        } finally {
            if (writer instanceof AutoCloseable) {
                IOUtils.closeQuietly((AutoCloseable) writer);
            }
            if (!success) {
                fileWriter.deleteCreatedFiles();
            }
        }
    }

    private static PrimaryKeyIndexSourceFile sourceMetadata(DataFileMeta sourceFile) {
        return new PrimaryKeyIndexSourceFile(sourceFile.fileName(), sourceFile.rowCount());
    }

    private static String normalizeMetric(String metric) {
        return metric.toLowerCase(Locale.ROOT).replace('-', '_');
    }

    private class SegmentFileWriter implements GlobalIndexFileWriter {

        private final Map<String, Path> createdFiles = new HashMap<>();

        @Override
        public String newFileName(String prefix) {
            Path path = pathFactory.newPath();
            createdFiles.put(path.getName(), path);
            return path.getName();
        }

        @Override
        public PositionOutputStream newOutputStream(String fileName) throws IOException {
            return fileIO.newOutputStream(path(fileName), false);
        }

        private Path path(String fileName) {
            Path path = createdFiles.get(fileName);
            checkArgument(path != null, "ANN payload file %s was not allocated.", fileName);
            return path;
        }

        private ResultEntry emptyResult() throws IOException {
            deleteCreatedFiles();
            createdFiles.clear();
            String fileName = newFileName("empty-vector");
            try (PositionOutputStream ignored = newOutputStream(fileName)) {
                // The searcher does not open payloads for segments without live rows.
            }
            return new ResultEntry(fileName, 0, null);
        }

        private void deleteCreatedFiles() {
            for (Path path : createdFiles.values()) {
                fileIO.deleteQuietly(path);
            }
        }
    }

    /** One vector source used while building an ANN segment. */
    public static class Source {

        private final PrimaryKeyIndexSourceFile sourceFile;
        @Nullable private final PkVectorReader vectors;
        @Nullable private final ReaderFactory readerFactory;
        private final LongPredicate excludedPosition;

        public Source(DataFileMeta sourceFile, PkVectorReader vectors) {
            this(sourceFile, vectors, position -> false);
        }

        public Source(
                DataFileMeta sourceFile, PkVectorReader vectors, LongPredicate excludedPosition) {
            this(sourceMetadata(sourceFile), vectors, excludedPosition);
        }

        Source(
                PrimaryKeyIndexSourceFile sourceFile,
                PkVectorReader vectors,
                LongPredicate excludedPosition) {
            this.sourceFile = sourceFile;
            this.vectors = vectors;
            this.readerFactory = null;
            this.excludedPosition = excludedPosition;
        }

        private Source(
                PrimaryKeyIndexSourceFile sourceFile,
                ReaderFactory readerFactory,
                LongPredicate excludedPosition) {
            this.sourceFile = sourceFile;
            this.vectors = null;
            this.readerFactory = readerFactory;
            this.excludedPosition = excludedPosition;
        }

        static Source lazy(PrimaryKeyIndexSourceFile sourceFile, ReaderFactory readerFactory) {
            return new Source(sourceFile, readerFactory, position -> false);
        }

        static Source lazy(
                PrimaryKeyIndexSourceFile sourceFile,
                ReaderFactory readerFactory,
                LongPredicate excludedPosition) {
            return new Source(sourceFile, readerFactory, excludedPosition);
        }

        private PkVectorReader openReader() throws IOException {
            return vectors != null ? vectors : readerFactory.open();
        }

        private void closeReader(PkVectorReader reader) throws IOException {
            if (vectors == null) {
                reader.close();
            }
        }

        @FunctionalInterface
        interface ReaderFactory {
            PkVectorReader open() throws IOException;
        }
    }
}
