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
import org.apache.paimon.options.Options;

import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** {@link FormatMetadataReader} that returns Parquet row-group boundaries from the footer. */
public class ParquetMetadataReader implements FormatMetadataReader {

    private static final Logger LOG = LoggerFactory.getLogger(ParquetMetadataReader.class);

    private final Options options;

    public ParquetMetadataReader(Options options) {
        this.options = options;
    }

    @Override
    public List<FileSplitBoundary> getSplitBoundaries(FileIO fileIO, Path filePath, long fileSize) {
        try (ParquetFileReader reader =
                ParquetUtil.getParquetReader(fileIO, filePath, fileSize, options)) {
            List<BlockMetaData> blocks = reader.getFooter().getBlocks();
            if (blocks.isEmpty()) {
                return Collections.emptyList();
            }
            List<FileSplitBoundary> boundaries = new ArrayList<>(blocks.size());
            for (BlockMetaData block : blocks) {
                boundaries.add(
                        new FileSplitBoundary(
                                block.getStartingPos(),
                                block.getCompressedSize(),
                                block.getRowCount()));
            }
            return boundaries;
        } catch (IOException | RuntimeException e) {
            LOG.warn(
                    "Failed to read Parquet metadata for {}; falling back to single split.",
                    filePath,
                    e);
            return Collections.emptyList();
        }
    }

    @Override
    public boolean supportsFinerGranularity() {
        return true;
    }
}
