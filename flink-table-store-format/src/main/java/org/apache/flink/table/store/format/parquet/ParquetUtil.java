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

package org.apache.flink.table.store.format.parquet;

import org.apache.flink.table.types.logical.RowType;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.HadoopInputFile;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** parquet utility, support for extracting metadata and etc. */
public class ParquetUtil {

    /**
     * @param path the path of parquet files to be read
     * @return result sets as map, key is column name, value is statistics (for example, null count,
     *     minimum value, maximum value)
     * @throws IOException
     */
    public static Map<String, Statistics> extractColumnStats(Path path) throws IOException {
        ParquetMetadata parquetMetadata = getParquetReader(path).getFooter();
        List<BlockMetaData> blockMetaDataList = parquetMetadata.getBlocks();
        Map<String, Statistics> resultStats = new HashMap<>();
        for (BlockMetaData blockMetaData : blockMetaDataList) {
            List<ColumnChunkMetaData> columnChunkMetaDataList = blockMetaData.getColumns();
            for (ColumnChunkMetaData columnChunkMetaData : columnChunkMetaDataList) {
                Statistics stats = columnChunkMetaData.getStatistics();
                String columnName = columnChunkMetaData.getPath().toDotString();
                Statistics midStats;
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
     * @param path the path of parquet files to be read
     * @return parquet reader, used for reading footer, status, etc.
     * @throws IOException
     */
    public static ParquetFileReader getParquetReader(Path path) throws IOException {
        HadoopInputFile hadoopInputFile = HadoopInputFile.fromPath(path, new Configuration());
        return ParquetFileReader.open(hadoopInputFile, ParquetReadOptions.builder().build());
    }

    static void assertStatsClass(
            RowType.RowField field, Statistics stats, Class<? extends Statistics> expectedClass) {
        if (!expectedClass.isInstance(stats)) {
            throw new IllegalArgumentException(
                    "Expecting "
                            + expectedClass.getName()
                            + " for field "
                            + field.asSummaryString()
                            + " but found "
                            + stats.getClass().getName());
        }
    }
}
