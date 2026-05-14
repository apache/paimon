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

package org.apache.paimon.table.source;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.format.FileSplitBoundary;
import org.apache.paimon.format.FormatMetadataReader;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.utils.FileStorePathFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/** Decorator for {@link SplitGenerator} that splits large files at row group or stripe boundaries. */
public class FineGrainedSplitGenerator implements SplitGenerator {

    private static final Logger LOG = LoggerFactory.getLogger(FineGrainedSplitGenerator.class);

    private final SplitGenerator baseGenerator;
    private final long splitFileThreshold;
    private final int maxSplitsPerFile;
    private final FileIO fileIO;
    private final FileStorePathFactory pathFactory;
    private final Map<String, FormatMetadataReader> metadataReaders;

    public FineGrainedSplitGenerator(
            SplitGenerator baseGenerator,
            long splitFileThreshold,
            int maxSplitsPerFile,
            FileIO fileIO,
            FileStorePathFactory pathFactory,
            Map<String, FormatMetadataReader> metadataReaders) {
        this.baseGenerator = baseGenerator;
        this.splitFileThreshold = splitFileThreshold;
        this.maxSplitsPerFile = maxSplitsPerFile;
        this.fileIO = fileIO;
        this.pathFactory = pathFactory;
        this.metadataReaders = metadataReaders;
    }

    @Override
    public boolean alwaysRawConvertible() {
        return baseGenerator.alwaysRawConvertible();
    }

    /** Delegates to base generator — fine-grained splitting is batch-only. */
    @Override
    public List<SplitGroup> splitForBatch(List<DataFileMeta> files) {
        return baseGenerator.splitForBatch(files);
    }

    /**
     * Fine-grained batch split with partition and bucket context for path construction.
     * Called by {@link org.apache.paimon.table.source.snapshot.SnapshotReaderImpl}.
     */
    public List<SplitGroup> splitForBatch(
            List<DataFileMeta> files, BinaryRow partition, int bucket) {
        List<SplitGroup> baseGroups = baseGenerator.splitForBatch(files);

        if (metadataReaders.isEmpty()) {
            return baseGroups;
        }

        Path bucketDir = pathFactory.bucketPath(partition, bucket);
        List<SplitGroup> result = new ArrayList<>();

        for (SplitGroup group : baseGroups) {
            List<DataFileMeta> groupFiles = group.files;
            List<DataFileMeta> filesToKeep = new ArrayList<>();

            for (DataFileMeta file : groupFiles) {
                if (!shouldSplitFile(file)) {
                    filesToKeep.add(file);
                    continue;
                }

                List<FileSplitBoundary> boundaries = getSplitBoundaries(file, bucketDir);

                if (boundaries.size() <= 1) {
                    filesToKeep.add(file);
                    continue;
                }

                // Flush any non-split files accumulated so far as one group
                if (!filesToKeep.isEmpty()) {
                    result.add(groupOf(group.rawConvertible, filesToKeep, null));
                    filesToKeep = new ArrayList<>();
                }

                int numSplits = Math.min(boundaries.size(), maxSplitsPerFile);
                for (int i = 0; i < numSplits; i++) {
                    result.add(
                            groupOf(
                                    group.rawConvertible,
                                    Collections.singletonList(file),
                                    boundaries.get(i)));
                }
            }

            if (!filesToKeep.isEmpty()) {
                result.add(groupOf(group.rawConvertible, filesToKeep, null));
            }
        }

        return result;
    }

    @Override
    public List<SplitGroup> splitForStreaming(List<DataFileMeta> files) {
        return baseGenerator.splitForStreaming(files);
    }

    private boolean shouldSplitFile(DataFileMeta file) {
        return file.fileSize() >= splitFileThreshold
                && metadataReaders.containsKey(file.fileFormat().toLowerCase());
    }

    private List<FileSplitBoundary> getSplitBoundaries(DataFileMeta file, Path bucketDir) {
        String format = file.fileFormat().toLowerCase();
        FormatMetadataReader reader = metadataReaders.get(format);

        if (reader == null || !reader.supportsFinerGranularity()) {
            return Collections.emptyList();
        }

        Path filePath = new Path(bucketDir, file.fileName());
        try {
            return reader.getSplitBoundaries(fileIO, filePath, file.fileSize());
        } catch (IOException e) {
            LOG.warn(
                    "Failed to read split boundaries for {} ({}), treating as single split",
                    file.fileName(),
                    format,
                    e);
            return Collections.emptyList();
        }
    }

    private static SplitGroup groupOf(
            boolean rawConvertible, List<DataFileMeta> files, FileSplitBoundary boundary) {
        if (rawConvertible) {
            return boundary != null
                    ? SplitGroup.rawConvertibleGroup(files, boundary)
                    : SplitGroup.rawConvertibleGroup(files);
        }
        return SplitGroup.nonRawConvertibleGroup(files);
    }
}
