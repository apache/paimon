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

package org.apache.paimon.format.parquet;

import org.apache.paimon.format.parquet.writer.StreamOutputFile;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.options.Options;
import org.apache.paimon.types.RowType;

import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.internal.column.columnindex.ColumnIndex;
import org.apache.parquet.internal.column.columnindex.OffsetIndex;
import org.apache.parquet.schema.MessageType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Copies compressed Parquet RowGroups from input files into one or more output files. Output files
 * are rolled only at RowGroup boundaries when the accumulated compressed size exceeds {@code
 * targetFileSize}.
 */
public class ParquetRowGroupCopier {

    /** A prepared input file with its RowGroup metadata. */
    public static final class Input {
        private final ParquetInputFile inputFile;
        private final ParquetMetadata metadata;

        public Input(ParquetInputFile inputFile, ParquetMetadata metadata) {
            this.inputFile = inputFile;
            this.metadata = metadata;
        }

        public ParquetInputFile inputFile() {
            return inputFile;
        }

        public ParquetMetadata metadata() {
            return metadata;
        }
    }

    /** A copied RowGroup from a specific input file block. */
    public static final class BlockContribution {
        private final int fileIndex;
        private final int blockIndex;
        private final long rowCount;

        public BlockContribution(int fileIndex, int blockIndex, long rowCount) {
            this.fileIndex = fileIndex;
            this.blockIndex = blockIndex;
            this.rowCount = rowCount;
        }

        public int fileIndex() {
            return fileIndex;
        }

        public int blockIndex() {
            return blockIndex;
        }

        public long rowCount() {
            return rowCount;
        }
    }

    /** Result of copying RowGroups into one output file. */
    public static final class OutputFile {
        private final Path path;
        private final long fileSize;
        private final long rowCount;
        private final List<BlockContribution> blockContributions;

        public OutputFile(
                Path path,
                long fileSize,
                long rowCount,
                List<BlockContribution> blockContributions) {
            this.path = path;
            this.fileSize = fileSize;
            this.rowCount = rowCount;
            this.blockContributions = blockContributions;
        }

        public Path path() {
            return path;
        }

        public long fileSize() {
            return fileSize;
        }

        public long rowCount() {
            return rowCount;
        }

        public List<BlockContribution> blockContributions() {
            return blockContributions;
        }
    }

    private final FileIO fileIO;
    private final MessageType schema;
    private final long targetFileSize;
    private final Supplier<Path> outputPathSupplier;
    private final Options options;
    private final boolean preservePageIndex;

    public ParquetRowGroupCopier(
            FileIO fileIO,
            RowType writeType,
            long targetFileSize,
            Supplier<Path> outputPathSupplier,
            Options options,
            boolean preservePageIndex) {
        this.fileIO = fileIO;
        this.schema = ParquetSchemaConverter.convertToParquetMessageType(writeType);
        this.targetFileSize = targetFileSize;
        this.outputPathSupplier = outputPathSupplier;
        this.options = options;
        this.preservePageIndex = preservePageIndex;
    }

    public List<OutputFile> copy(List<Input> inputs) throws IOException {
        if (inputs.isEmpty()) {
            return Collections.emptyList();
        }

        List<OutputFile> outputs = new ArrayList<>();
        ActiveWriter activeWriter = null;
        try {
            for (int fileIndex = 0; fileIndex < inputs.size(); fileIndex++) {
                Input input = inputs.get(fileIndex);
                List<BlockMetaData> blocks = input.metadata().getBlocks();
                try (ParquetInputStream inputStream = input.inputFile().newStream()) {
                    ParquetFileReader indexReader = null;
                    if (preservePageIndex) {
                        indexReader =
                                new ParquetFileReader(
                                        input.inputFile(),
                                        input.metadata(),
                                        ParquetUtil.getParquetReadOptionsBuilder(options).build(),
                                        inputStream,
                                        null);
                    }
                    try {
                        for (int blockIndex = 0; blockIndex < blocks.size(); blockIndex++) {
                            BlockMetaData block = blocks.get(blockIndex);
                            long blockCompressedSize = block.getCompressedSize();
                            if (activeWriter != null
                                    && activeWriter.compressedSize > 0
                                    && activeWriter.compressedSize + blockCompressedSize
                                            > targetFileSize) {
                                outputs.add(activeWriter.finish());
                                activeWriter = null;
                            }
                            if (activeWriter == null) {
                                activeWriter =
                                        ActiveWriter.start(fileIO, schema, outputPathSupplier);
                            }
                            if (preservePageIndex) {
                                appendRowGroupWithPageIndexes(
                                        activeWriter.writer, inputStream, indexReader, block);
                            } else {
                                activeWriter.writer.appendRowGroup(inputStream, block, true);
                            }
                            activeWriter.compressedSize += blockCompressedSize;
                            activeWriter.rowCount += block.getRowCount();
                            activeWriter.blockContributions.add(
                                    new BlockContribution(
                                            fileIndex, blockIndex, block.getRowCount()));
                        }
                    } finally {
                        if (indexReader != null) {
                            indexReader.detachFileInputStream();
                            indexReader.close();
                        }
                    }
                }
            }
            if (activeWriter != null) {
                outputs.add(activeWriter.finish());
            }
            return outputs;
        } catch (IOException | RuntimeException e) {
            if (activeWriter != null) {
                activeWriter.abortQuietly();
            }
            for (OutputFile output : outputs) {
                fileIO.deleteQuietly(output.path());
            }
            throw e;
        }
    }

    private void appendRowGroupWithPageIndexes(
            ParquetFileWriter writer,
            ParquetInputStream inputStream,
            ParquetFileReader indexReader,
            BlockMetaData block)
            throws IOException {
        Map<ColumnPath, ColumnChunkMetaData> columns = new HashMap<>();
        for (ColumnChunkMetaData column : block.getColumns()) {
            columns.put(column.getPath(), column);
        }

        writer.startBlock(block.getRowCount());
        for (ColumnDescriptor descriptor : schema.getColumns()) {
            ColumnPath path = ColumnPath.get(descriptor.getPath());
            ColumnChunkMetaData column = columns.remove(path);
            if (column == null) {
                throw new IOException(
                        "Missing column " + path.toDotString() + " while copying RowGroup");
            }
            ColumnIndex columnIndex = indexReader.readColumnIndex(column);
            OffsetIndex offsetIndex = indexReader.readOffsetIndex(column);
            writer.appendColumnChunk(
                    descriptor, inputStream, column, null, columnIndex, offsetIndex);
        }
        if (!columns.isEmpty()) {
            throw new IOException("Unexpected columns while copying RowGroup: " + columns.keySet());
        }
        writer.endBlock();
    }

    private static final class ActiveWriter {
        private final FileIO fileIO;
        private final Path path;
        private final PositionOutputStream outputStream;
        private final ParquetFileWriter writer;
        private long compressedSize;
        private long rowCount;
        private final List<BlockContribution> blockContributions = new ArrayList<>();

        private ActiveWriter(
                FileIO fileIO,
                Path path,
                PositionOutputStream outputStream,
                ParquetFileWriter writer) {
            this.fileIO = fileIO;
            this.path = path;
            this.outputStream = outputStream;
            this.writer = writer;
        }

        private static ActiveWriter start(
                FileIO fileIO, MessageType schema, Supplier<Path> outputPathSupplier)
                throws IOException {
            Path path = outputPathSupplier.get();
            PositionOutputStream outputStream = fileIO.newOutputStream(path, false);
            try {
                ParquetFileWriter writer =
                        new ParquetFileWriter(
                                new StreamOutputFile(outputStream),
                                schema,
                                ParquetFileWriter.Mode.CREATE,
                                ParquetWriter.DEFAULT_BLOCK_SIZE,
                                ParquetWriter.MAX_PADDING_SIZE_DEFAULT);
                writer.start();
                return new ActiveWriter(fileIO, path, outputStream, writer);
            } catch (IOException | RuntimeException e) {
                try {
                    outputStream.close();
                } catch (IOException ignored) {
                    // best effort
                }
                fileIO.deleteQuietly(path);
                throw e;
            }
        }

        private OutputFile finish() throws IOException {
            writer.end(Collections.emptyMap());
            outputStream.close();
            long fileSize = fileIO.getFileSize(path);
            return new OutputFile(
                    path, fileSize, rowCount, Collections.unmodifiableList(blockContributions));
        }

        private void abortQuietly() {
            try {
                outputStream.close();
            } catch (IOException ignored) {
                // best effort
            }
            fileIO.deleteQuietly(path);
        }
    }
}
