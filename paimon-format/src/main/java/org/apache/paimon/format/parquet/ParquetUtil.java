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

import org.apache.paimon.format.SimpleStatsExtractor;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.options.Options;
import org.apache.paimon.types.DataField;
import org.apache.paimon.utils.Pair;

import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.conf.PlainParquetConfiguration;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Parquet utilities that support to extract the metadata, assert expected stats, etc. */
public class ParquetUtil {

    /**
     * Extract stats from specified Parquet file path.
     *
     * @param path the path of parquet file to be read
     * @param length the length of parquet file to be read
     * @return result sets as map, key is column name, value is statistics (for example, null count,
     *     minimum value, maximum value)
     */
    public static Pair<Map<String, Statistics<?>>, SimpleStatsExtractor.FileInfo>
            extractColumnStats(FileIO fileIO, Path path, long length, Options options)
                    throws IOException {
        try (ParquetFileReader reader = getParquetReader(fileIO, path, length, options)) {
            ParquetMetadata parquetMetadata = reader.getFooter();
            Map<String, Statistics<?>> resultStats = extractColumnStats(parquetMetadata);
            return Pair.of(resultStats, new SimpleStatsExtractor.FileInfo(reader.getRecordCount()));
        }
    }

    /**
     * Extract column stats from in-memory {@link ParquetMetadata}. This avoids re-reading the file
     * from storage, which is critical for object stores (like OSS/S3) where the file may not be
     * immediately visible after close.
     *
     * @param parquetMetadata the in-memory Parquet metadata (footer)
     * @return result sets as map, key is column name, value is statistics
     */
    public static Map<String, Statistics<?>> extractColumnStats(ParquetMetadata parquetMetadata) {
        return extractColumnStatsFromBlocks(parquetMetadata.getBlocks());
    }

    /** Extract and merge column stats from the given RowGroups. */
    public static Map<String, Statistics<?>> extractColumnStatsFromBlocks(
            List<BlockMetaData> blockMetaDataList) {
        Map<String, Statistics<?>> resultStats = new HashMap<>();
        for (BlockMetaData blockMetaData : blockMetaDataList) {
            List<ColumnChunkMetaData> columnChunkMetaDataList = blockMetaData.getColumns();
            for (ColumnChunkMetaData columnChunkMetaData : columnChunkMetaDataList) {
                Statistics<?> stats = columnChunkMetaData.getStatistics();
                String columnName = columnChunkMetaData.getPath().toDotString();
                Statistics<?> midStats;
                if (!resultStats.containsKey(columnName)) {
                    midStats = stats;
                } else {
                    midStats = resultStats.get(columnName);
                    midStats.mergeStatistics(stats);
                }
                resultStats.put(columnName, midStats);
            }
        }
        return resultStats;
    }

    /**
     * Read the footer of the Parquet file at the given path.
     *
     * @param path the path of parquet file to be read
     * @param length the length of parquet file to be read
     * @param options the configuration
     * @return the parquet footer metadata
     */
    public static ParquetMetadata readFooter(FileIO fileIO, Path path, long length, Options options)
            throws IOException {
        try (ParquetFileReader reader = getParquetReader(fileIO, path, length, options)) {
            return reader.getFooter();
        }
    }

    /**
     * Generate {@link ParquetFileReader} instance to read the Parquet file at the given path.
     *
     * @param path the path of parquet file to be read
     * @param length the length of parquet file to be read
     * @param options the configuration
     * @return parquet reader, used for reading footer, status, etc.
     */
    public static ParquetFileReader getParquetReader(
            FileIO fileIO, Path path, long length, Options options) throws IOException {
        return new ParquetFileReader(
                ParquetInputFile.fromPath(fileIO, path, length),
                getParquetReadOptionsBuilder(options).build(),
                null);
    }

    public static ParquetReadOptions.Builder getParquetReadOptionsBuilder(Options options) {
        PlainParquetConfiguration parquetConfiguration =
                new PlainParquetConfiguration(options.toMap());
        return ParquetReadOptions.builder(parquetConfiguration);
    }

    public static int getRowGroupCount(FileIO fileIO, Path path, long length, Options options)
            throws IOException {
        return readFooter(fileIO, path, length, options).getBlocks().size();
    }

    public static boolean hasDictionaryPage(FileIO fileIO, Path path, long length, Options options)
            throws IOException {
        ParquetMetadata footer = readFooter(fileIO, path, length, options);
        for (BlockMetaData block : footer.getBlocks()) {
            for (ColumnChunkMetaData column : block.getColumns()) {
                if (column.hasDictionaryPage()) {
                    return true;
                }
            }
        }
        return false;
    }

    public static boolean aggregatedRowGroupFootersMatch(
            FileIO fileIO,
            Path[] inputPaths,
            long[] inputLengths,
            Path[] outputPaths,
            long[] outputLengths,
            Options options)
            throws IOException {
        List<BlockMetaData> inputBlocks = readAllBlocks(fileIO, inputPaths, inputLengths, options);
        List<BlockMetaData> outputBlocks =
                readAllBlocks(fileIO, outputPaths, outputLengths, options);
        if (inputBlocks.size() != outputBlocks.size()) {
            return false;
        }
        for (int i = 0; i < inputBlocks.size(); i++) {
            if (!rowGroupFooterMatches(inputBlocks.get(i), outputBlocks.get(i))) {
                return false;
            }
        }
        for (int i = 0; i < outputPaths.length; i++) {
            if (!rowGroupLayoutValid(fileIO, outputPaths[i], outputLengths[i], options)) {
                return false;
            }
        }
        return true;
    }

    private static List<BlockMetaData> readAllBlocks(
            FileIO fileIO, Path[] paths, long[] lengths, Options options) throws IOException {
        List<BlockMetaData> blocks = new java.util.ArrayList<>();
        for (int i = 0; i < paths.length; i++) {
            blocks.addAll(readFooter(fileIO, paths[i], lengths[i], options).getBlocks());
        }
        return blocks;
    }

    private static boolean rowGroupFooterMatches(
            BlockMetaData inputBlock, BlockMetaData outputBlock) {
        if (outputBlock.getRowCount() != inputBlock.getRowCount()) {
            return false;
        }
        if (outputBlock.getColumns().size() != inputBlock.getColumns().size()) {
            return false;
        }
        for (int c = 0; c < inputBlock.getColumns().size(); c++) {
            ColumnChunkMetaData inputColumn = inputBlock.getColumns().get(c);
            ColumnChunkMetaData outputColumn = outputBlock.getColumns().get(c);
            if (!outputColumn.getPath().equals(inputColumn.getPath())
                    || outputColumn.getTotalSize() != inputColumn.getTotalSize()
                    || !outputColumn
                            .getStatistics()
                            .toString()
                            .equals(inputColumn.getStatistics().toString())) {
                return false;
            }
        }
        return true;
    }

    private static boolean rowGroupLayoutValid(
            FileIO fileIO, Path path, long fileSize, Options options) throws IOException {
        long previousBlockStart = -1;
        List<BlockMetaData> blocks = readFooter(fileIO, path, fileSize, options).getBlocks();
        for (int i = 0; i < blocks.size(); i++) {
            BlockMetaData block = blocks.get(i);
            long blockStart = block.getStartingPos();
            if (i == 0 && blockStart != 4L) {
                return false;
            }
            if (blockStart <= previousBlockStart) {
                return false;
            }
            previousBlockStart = blockStart;
            for (ColumnChunkMetaData column : block.getColumns()) {
                if (column.getStartingPos() < blockStart
                        || column.getStartingPos() + column.getTotalSize() > fileSize) {
                    return false;
                }
            }
        }
        return true;
    }

    static void assertStatsClass(
            DataField field, Statistics<?> stats, Class<? extends Statistics<?>> expectedClass) {
        if (!expectedClass.isInstance(stats)) {
            throw new IllegalArgumentException(
                    "Expecting "
                            + expectedClass.getName()
                            + " for field "
                            + field.asSQLString()
                            + " but found "
                            + stats.getClass().getName());
        }
    }
}
