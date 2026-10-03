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

import org.apache.paimon.format.SimpleColStats;
import org.apache.paimon.format.parquet.ParquetRowGroupCopyChecker.Incompatibility;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.options.Options;

import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * One input Parquet file prepared for RowGroup copy. Parquet metadata types stay inside
 * paimon-format so callers are not tied to the relocated Parquet packages.
 */
public final class ParquetRowGroupCopySource {

    private final ParquetInputFile inputFile;
    private final ParquetMetadata metadata;

    private ParquetRowGroupCopySource(ParquetInputFile inputFile, ParquetMetadata metadata) {
        this.inputFile = inputFile;
        this.metadata = metadata;
    }

    public static ParquetRowGroupCopySource read(
            FileIO fileIO, Path path, long fileSize, Options options) throws IOException {
        return new ParquetRowGroupCopySource(
                ParquetInputFile.fromPath(fileIO, path, fileSize),
                ParquetUtil.readFooter(fileIO, path, fileSize, options));
    }

    @Nullable
    public Incompatibility check(ParquetRowGroupCopyChecker checker) {
        return checker.checkFooter(metadata);
    }

    public int blockCount() {
        return metadata.getBlocks().size();
    }

    public ParquetRowGroupCopier.Input copierInput() {
        return new ParquetRowGroupCopier.Input(inputFile, metadata);
    }

    public SimpleColStats[] extractBlockStats(
            ParquetSimpleStatsExtractor extractor, List<Integer> blockIndexes) {
        List<BlockMetaData> blocks = metadata.getBlocks();
        List<BlockMetaData> selected = new ArrayList<>(blockIndexes.size());
        for (Integer blockIndex : blockIndexes) {
            selected.add(blocks.get(blockIndex));
        }
        return extractor.extractFromBlocks(selected);
    }
}
