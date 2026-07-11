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
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.options.Options;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.VectorType;
import org.apache.paimon.utils.IOUtils;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.LongPredicate;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Builds immutable ANN payloads whose index ids are source data-file row positions. */
public class PkVectorAnnSegmentFile extends IndexFile {

    public static final String PK_VECTOR_ANN = "pk-vector-ann";

    public PkVectorAnnSegmentFile(FileIO fileIO, IndexPathFactory pathFactory) {
        super(fileIO, pathFactory);
    }

    public IndexFileMeta build(
            List<Source> sources,
            PkVectorAnnSegmentMeta.OrdinalLayout ordinalLayout,
            DataField vectorField,
            Options indexOptions,
            String indexDefinitionId,
            String metric,
            String algorithm)
            throws IOException {
        checkArgument(!sources.isEmpty(), "An ANN segment must reference source files.");
        long totalRowCount = 0;
        List<PkVectorSourceFile> sourceFiles = new ArrayList<>(sources.size());
        for (Source source : sources) {
            totalRowCount = Math.addExact(totalRowCount, source.sourceFile.rowCount());
            sourceFiles.add(source.sourceFile);
        }

        GlobalIndexer indexer = GlobalIndexer.create(algorithm, vectorField, indexOptions);
        checkArgument(
                indexer instanceof VectorGlobalIndexer,
                "Index algorithm %s does not implement VectorGlobalIndexer.",
                algorithm);
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
                    algorithm);
            GlobalIndexSingleColumnWriter singleColumnWriter =
                    (GlobalIndexSingleColumnWriter) writer;
            long liveRowCount = 0;
            long fileOffset = 0;
            int dimension = -1;
            for (Source source : sources) {
                RawVectorSidecarReader rawVectors = source.openReader();
                try {
                    checkArgument(
                            rawVectors.rowCount() == source.sourceFile.rowCount(),
                            "Raw vector row count %s does not match source file %s row count %s.",
                            rawVectors.rowCount(),
                            source.sourceFile.fileName(),
                            source.sourceFile.rowCount());
                    if (dimension < 0) {
                        dimension = rawVectors.dimension();
                    }
                    checkArgument(
                            rawVectors.dimension() == dimension,
                            "Raw vector source %s dimension %s does not match dimension %s.",
                            source.sourceFile.fileName(),
                            rawVectors.dimension(),
                            dimension);
                    if (vectorField.type() instanceof VectorType) {
                        checkArgument(
                                ((VectorType) vectorField.type()).getLength() == dimension,
                                "Vector field dimension %s does not match raw vector dimension %s.",
                                ((VectorType) vectorField.type()).getLength(),
                                dimension);
                    }

                    float[] vector = new float[dimension];
                    rawVectors.rewind();
                    for (long rowPosition = 0; rowPosition < rawVectors.rowCount(); rowPosition++) {
                        boolean present = rawVectors.readNextVector(vector);
                        if (!present || source.excludedPosition.test(rowPosition)) {
                            continue;
                        }
                        singleColumnWriter.write(vector, fileOffset + rowPosition);
                        liveRowCount++;
                    }
                } finally {
                    source.closeReader(rawVectors);
                }
                fileOffset += source.sourceFile.rowCount();
            }

            List<ResultEntry> results = writer.finish();
            checkArgument(
                    results.size() == 1,
                    "ANN segment build must produce exactly one payload file, but produced %s.",
                    results.size());
            ResultEntry result = results.get(0);
            Path payloadPath = fileWriter.path(result.fileName());
            byte[] payloadMetadata = result.meta() == null ? new byte[0] : result.meta();
            PkVectorAnnSegmentMeta metadata =
                    new PkVectorAnnSegmentMeta(
                            indexDefinitionId, sourceFiles, ordinalLayout, payloadMetadata);
            IndexFileMeta segment =
                    new IndexFileMeta(
                            PK_VECTOR_ANN,
                            result.fileName(),
                            fileIO.getFileSize(payloadPath),
                            liveRowCount,
                            new GlobalIndexMeta(
                                    0, totalRowCount, vectorField.id(), null, metadata.serialize()),
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

    private static PkVectorSourceFile sourceMetadata(DataFileMeta sourceFile) {
        return new PkVectorSourceFile(sourceFile.fileName(), sourceFile.rowCount());
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

        private void deleteCreatedFiles() {
            for (Path path : createdFiles.values()) {
                fileIO.deleteQuietly(path);
            }
        }
    }

    /** One raw vector source used while building an ANN segment. */
    public static class Source {

        private final PkVectorSourceFile sourceFile;
        @Nullable private final RawVectorSidecarReader rawVectors;
        @Nullable private final ReaderFactory readerFactory;
        private final LongPredicate excludedPosition;

        public Source(DataFileMeta sourceFile, RawVectorSidecarReader rawVectors) {
            this(sourceFile, rawVectors, position -> false);
        }

        public Source(
                DataFileMeta sourceFile,
                RawVectorSidecarReader rawVectors,
                LongPredicate excludedPosition) {
            this(sourceMetadata(sourceFile), rawVectors, excludedPosition);
        }

        Source(
                PkVectorSourceFile sourceFile,
                RawVectorSidecarReader rawVectors,
                LongPredicate excludedPosition) {
            this.sourceFile = sourceFile;
            this.rawVectors = rawVectors;
            this.readerFactory = null;
            this.excludedPosition = excludedPosition;
        }

        private Source(
                PkVectorSourceFile sourceFile,
                ReaderFactory readerFactory,
                LongPredicate excludedPosition) {
            this.sourceFile = sourceFile;
            this.rawVectors = null;
            this.readerFactory = readerFactory;
            this.excludedPosition = excludedPosition;
        }

        static Source lazy(PkVectorSourceFile sourceFile, ReaderFactory readerFactory) {
            return new Source(sourceFile, readerFactory, position -> false);
        }

        private RawVectorSidecarReader openReader() throws IOException {
            return rawVectors != null ? rawVectors : readerFactory.open();
        }

        private void closeReader(RawVectorSidecarReader reader) throws IOException {
            if (rawVectors == null) {
                reader.close();
            }
        }

        @FunctionalInterface
        interface ReaderFactory {
            RawVectorSidecarReader open() throws IOException;
        }
    }
}
