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

import org.apache.paimon.format.FileSplitBoundary;
import org.apache.paimon.format.FormatMetadataReader;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;

import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Metadata reader for Parquet files that extracts row group boundaries for finer-grained splitting.
 *
 * <p>This reader extracts the byte offset and length for each row group in a Parquet file, enabling
 * the file to be split at row group boundaries rather than file boundaries.
 *
 * @since 0.9.0
 */
public class ParquetMetadataReader implements FormatMetadataReader {

    private static final Logger LOG = LoggerFactory.getLogger(ParquetMetadataReader.class);

    @Override
    public List<FileSplitBoundary> getSplitBoundaries(FileIO fileIO, Path filePath, long fileSize)
            throws IOException {
        List<FileSplitBoundary> boundaries = new ArrayList<>();

        try (ParquetFileReader reader = ParquetUtil.getParquetReader(fileIO, filePath, fileSize)) {
            ParquetMetadata metadata = reader.getFooter();
            List<BlockMetaData> blocks = metadata.getBlocks();

            if (blocks.isEmpty()) {
                LOG.warn("Parquet file {} has no row groups", filePath);
                return boundaries;
            }

            for (BlockMetaData block : blocks) {
                long offset = Long.MAX_VALUE;
                long endOffset = Long.MIN_VALUE;

                // Calculate the byte range for this row group by finding the min start and max end
                // of all column chunks
                List<ColumnChunkMetaData> columns = block.getColumns();
                if (columns.isEmpty()) {
                    LOG.warn("Row group in file {} has no columns, skipping", filePath);
                    continue;
                }

                for (ColumnChunkMetaData column : columns) {
                    long columnStart = column.getStartingPos();
                    long columnEnd = columnStart + column.getTotalSize();
                    offset = Math.min(offset, columnStart);
                    endOffset = Math.max(endOffset, columnEnd);
                }

                long length = endOffset - offset;
                long rowCount = block.getRowCount();

                boundaries.add(new FileSplitBoundary(offset, length, rowCount));
            }

            if (LOG.isDebugEnabled()) {
                LOG.debug(
                        "Extracted {} row group boundaries from Parquet file {}",
                        boundaries.size(),
                        filePath);
            }
        } catch (Exception e) {
            LOG.warn("Failed to extract row group boundaries from Parquet file {}", filePath, e);
            // Return empty list on error - file will be treated as single split
            return new ArrayList<>();
        }

        return boundaries;
    }

    @Override
    public boolean supportsFinerGranularity() {
        return true;
    }
}
