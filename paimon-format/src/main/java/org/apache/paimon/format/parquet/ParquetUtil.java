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
import org.apache.paimon.types.DataField;
import org.apache.paimon.utils.Pair;

import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.column.statistics.Statistics;
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
     * Extract stats from specified Parquet files path.
     *
     * @param path the path of parquet files to be read
     * @return result sets as map, key is column name, value is statistics (for example, null count,
     *     minimum value, maximum value)
     */
    public static Pair<Map<String, Statistics<?>>, SimpleStatsExtractor.FileInfo>
            extractColumnStats(FileIO fileIO, Path path) throws IOException {
        try (ParquetFileReader reader = getParquetReader(fileIO, path)) {
            ParquetMetadata parquetMetadata = reader.getFooter();
            List<BlockMetaData> blockMetaDataList = parquetMetadata.getBlocks();
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
            return Pair.of(resultStats, new SimpleStatsExtractor.FileInfo(reader.getRecordCount()));
        }
    }

    /**
     * Generate {@link ParquetFileReader} instance to read the Parquet files at the given path.
     *
     * @param path the path of parquet files to be read
     * @return parquet reader, used for reading footer, status, etc.
     */
    public static ParquetFileReader getParquetReader(FileIO fileIO, Path path) throws IOException {
        return new ParquetFileReader(
                ParquetInputFile.fromPath(fileIO, path),
                ParquetReadOptions.builder().build(),
                null,
                null);
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
