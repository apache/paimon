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

package org.apache.paimon.io;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fileindex.FileIndexOptions;
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.format.FormatWriterFactory;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.stats.SimpleStats;
import org.apache.paimon.stats.SimpleStatsConverter;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.LongCounter;
import org.apache.paimon.utils.Pair;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.apache.paimon.io.DataFilePathFactory.dataFileToFileIndexPath;

/**
 * A {@link StatsCollectingSingleFileWriter} to write data files containing {@link InternalRow}.
 * Also produces {@link DataFileMeta} after writing a file.
 */
public class RowDataFileWriter extends StatsCollectingSingleFileWriter<InternalRow, DataFileMeta> {

    private final long schemaId;
    private final boolean isExternalPath;
    private final SimpleStatsConverter statsArraySerializer;
    private final List<DataFileAuxiliaryWriter> auxiliaryFileWriters;
    private final FileSource fileSource;
    @Nullable private final List<String> writeCols;
    private final RowDataFileSequenceNumberTracker sequenceNumberTracker;

    public RowDataFileWriter(
            FileIO fileIO,
            FileWriterContext context,
            Path path,
            RowType writeSchema,
            long schemaId,
            Supplier<LongCounter> seqNumCounterSupplier,
            FileIndexOptions fileIndexOptions,
            FileSource fileSource,
            boolean asyncFileWrite,
            boolean statsDenseStore,
            boolean isExternalPath,
            @Nullable List<String> writeCols) {
        this(
                fileIO,
                context,
                path,
                writeSchema,
                schemaId,
                seqNumCounterSupplier,
                fileIndexOptions,
                fileSource,
                asyncFileWrite,
                statsDenseStore,
                isExternalPath,
                writeCols,
                null,
                null);
    }

    public RowDataFileWriter(
            FileIO fileIO,
            FileWriterContext context,
            Path path,
            RowType writeSchema,
            long schemaId,
            Supplier<LongCounter> seqNumCounterSupplier,
            FileIndexOptions fileIndexOptions,
            FileSource fileSource,
            boolean asyncFileWrite,
            boolean statsDenseStore,
            boolean isExternalPath,
            @Nullable List<String> writeCols,
            @Nullable FileFormat rowSidecarFormat,
            @Nullable Path rowSidecarPath) {
        super(fileIO, context, path, Function.identity(), writeSchema, asyncFileWrite);
        if ((rowSidecarFormat == null) != (rowSidecarPath == null)) {
            throw new IllegalArgumentException(
                    "Row sidecar format and path should be both null or both non-null.");
        }
        this.schemaId = schemaId;
        this.isExternalPath = isExternalPath;
        this.statsArraySerializer = new SimpleStatsConverter(writeSchema, statsDenseStore);
        List<DataFileAuxiliaryWriter> auxiliaryFileWriters = new ArrayList<>();
        Path fileIndexPath = dataFileToFileIndexPath(path);
        DataFileIndexWriter dataFileIndexWriter =
                DataFileIndexWriter.create(fileIO, fileIndexPath, writeSchema, fileIndexOptions);
        if (dataFileIndexWriter != null) {
            auxiliaryFileWriters.add(
                    new DataFileIndexAuxiliaryWriter(dataFileIndexWriter, fileIO, fileIndexPath));
        }
        if (rowSidecarFormat != null) {
            auxiliaryFileWriters.add(
                    new RowSidecarAuxiliaryWriter(
                            fileIO,
                            rowSidecarFormat.createWriterFactory(writeSchema),
                            rowSidecarPath,
                            context.compression(),
                            asyncFileWrite));
        }
        this.auxiliaryFileWriters = Collections.unmodifiableList(auxiliaryFileWriters);
        this.fileSource = fileSource;
        this.writeCols = writeCols;
        this.sequenceNumberTracker =
                new RowDataFileSequenceNumberTracker(
                        writeSchema, seqNumCounterSupplier, super::recordCount);
    }

    @Override
    public void write(InternalRow row) throws IOException {
        super.write(row);
        for (DataFileAuxiliaryWriter auxiliaryFileWriter : auxiliaryFileWriters) {
            auxiliaryFileWriter.write(row);
        }
        sequenceNumberTracker.update(row);
    }

    @Override
    public void writeBundle(BundleRecords bundle) throws IOException {
        for (InternalRow row : bundle) {
            write(row);
        }
    }

    @Override
    public void close() throws IOException {
        for (DataFileAuxiliaryWriter auxiliaryFileWriter : auxiliaryFileWriters) {
            auxiliaryFileWriter.close();
        }
        super.close();
    }

    @Override
    public void abort() {
        for (DataFileAuxiliaryWriter auxiliaryFileWriter : auxiliaryFileWriters) {
            auxiliaryFileWriter.abort();
        }
        super.abort();
    }

    @Override
    public Optional<FileWriterAbortExecutor> abortExecutor() {
        Optional<FileWriterAbortExecutor> mainAbortExecutor = super.abortExecutor();
        if (auxiliaryFileWriters.isEmpty()) {
            return mainAbortExecutor;
        }

        List<FileWriterAbortExecutor> abortExecutors = new ArrayList<>();
        mainAbortExecutor.ifPresent(abortExecutors::add);
        for (DataFileAuxiliaryWriter auxiliaryFileWriter : auxiliaryFileWriters) {
            auxiliaryFileWriter.abortExecutor().ifPresent(abortExecutors::add);
        }
        if (abortExecutors.isEmpty()) {
            return Optional.empty();
        }
        if (abortExecutors.size() == 1) {
            return Optional.of(abortExecutors.get(0));
        }
        return Optional.of(new CompoundFileWriterAbortExecutor(fileIO, path, abortExecutors));
    }

    @Override
    public DataFileMeta result() throws IOException {
        long fileSize = outputBytes();
        Pair<List<String>, SimpleStats> statsPair =
                statsArraySerializer.toBinary(fieldStats(fileSize));
        List<String> extraFiles = new ArrayList<>();
        byte[] embeddedIndex = null;
        for (DataFileAuxiliaryWriter auxiliaryFileWriter : auxiliaryFileWriters) {
            DataFileAuxiliaryResult auxiliaryResult = auxiliaryFileWriter.result();
            extraFiles.addAll(auxiliaryResult.extraFiles());
            if (auxiliaryResult.embeddedIndexBytes() != null) {
                if (embeddedIndex != null) {
                    throw new IOException("Found more than one embedded index for one data file.");
                }
                embeddedIndex = auxiliaryResult.embeddedIndexBytes();
            }
        }
        String externalPath = isExternalPath ? path.toString() : null;
        return DataFileMeta.forAppend(
                path.getName(),
                fileSize,
                recordCount(),
                statsPair.getRight(),
                sequenceNumberTracker.min(),
                sequenceNumberTracker.max(),
                schemaId,
                extraFiles.isEmpty() ? Collections.emptyList() : extraFiles,
                embeddedIndex,
                fileSource,
                statsPair.getKey(),
                externalPath,
                null,
                writeCols);
    }

    private interface DataFileAuxiliaryWriter {

        void write(InternalRow row) throws IOException;

        void close() throws IOException;

        void abort();

        Optional<FileWriterAbortExecutor> abortExecutor();

        DataFileAuxiliaryResult result() throws IOException;
    }

    private static class DataFileAuxiliaryResult {

        private static final DataFileAuxiliaryResult EMPTY =
                new DataFileAuxiliaryResult(null, Collections.emptyList());

        @Nullable private final byte[] embeddedIndexBytes;
        private final List<String> extraFiles;

        private DataFileAuxiliaryResult(
                @Nullable byte[] embeddedIndexBytes, List<String> extraFiles) {
            this.embeddedIndexBytes = embeddedIndexBytes;
            this.extraFiles = extraFiles;
        }

        @Nullable
        private byte[] embeddedIndexBytes() {
            return embeddedIndexBytes;
        }

        private List<String> extraFiles() {
            return extraFiles;
        }

        private static DataFileAuxiliaryResult embeddedIndex(byte[] embeddedIndexBytes) {
            return new DataFileAuxiliaryResult(embeddedIndexBytes, Collections.emptyList());
        }

        private static DataFileAuxiliaryResult extraFile(String extraFile) {
            return new DataFileAuxiliaryResult(null, Collections.singletonList(extraFile));
        }
    }

    private static class DataFileIndexAuxiliaryWriter implements DataFileAuxiliaryWriter {

        private final DataFileIndexWriter writer;
        private final FileIO fileIO;
        private final Path indexPath;

        private DataFileIndexAuxiliaryWriter(
                DataFileIndexWriter writer, FileIO fileIO, Path indexPath) {
            this.writer = writer;
            this.fileIO = fileIO;
            this.indexPath = indexPath;
        }

        @Override
        public void write(InternalRow row) {
            writer.write(row);
        }

        @Override
        public void close() throws IOException {
            writer.close();
        }

        @Override
        public void abort() {
            fileIO.deleteQuietly(indexPath);
        }

        @Override
        public Optional<FileWriterAbortExecutor> abortExecutor() {
            return Optional.of(new FileWriterAbortExecutor(fileIO, indexPath));
        }

        @Override
        public DataFileAuxiliaryResult result() {
            DataFileIndexWriter.FileIndexResult result = writer.result();
            if (result.independentIndexFile() != null) {
                return DataFileAuxiliaryResult.extraFile(result.independentIndexFile());
            }
            if (result.embeddedIndexBytes() != null) {
                return DataFileAuxiliaryResult.embeddedIndex(result.embeddedIndexBytes());
            }
            return DataFileAuxiliaryResult.EMPTY;
        }
    }

    private static class RowSidecarAuxiliaryWriter
            extends SingleFileWriter<InternalRow, DataFileAuxiliaryResult>
            implements DataFileAuxiliaryWriter {

        private RowSidecarAuxiliaryWriter(
                FileIO fileIO,
                FormatWriterFactory factory,
                Path path,
                String compression,
                boolean asyncWrite) {
            super(fileIO, factory, path, Function.identity(), compression, asyncWrite);
        }

        @Override
        public DataFileAuxiliaryResult result() {
            return DataFileAuxiliaryResult.extraFile(path.getName());
        }
    }

    private static class CompoundFileWriterAbortExecutor extends FileWriterAbortExecutor {

        private final List<FileWriterAbortExecutor> abortExecutors;

        private CompoundFileWriterAbortExecutor(
                FileIO fileIO, Path path, List<FileWriterAbortExecutor> abortExecutors) {
            super(fileIO, path);
            this.abortExecutors = abortExecutors;
        }

        @Override
        public void abort() {
            for (FileWriterAbortExecutor abortExecutor : abortExecutors) {
                abortExecutor.abort();
            }
        }
    }
}
