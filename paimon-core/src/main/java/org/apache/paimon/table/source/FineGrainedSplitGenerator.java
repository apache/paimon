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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A decorator for {@link SplitGenerator} that enables finer-grained splitting by splitting large
 * files at row group (Parquet) or stripe (ORC) boundaries.
 *
 * <p>This generator wraps a base {@link SplitGenerator} and, when enabled, queries format metadata
 * readers to split large files into multiple splits. The boundaries are stored and can be used
 * later when converting to {@link org.apache.paimon.table.source.RawFile} objects.
 *
 * @since 0.9.0
 */
public class FineGrainedSplitGenerator implements SplitGenerator {

    private static final Logger LOG = LoggerFactory.getLogger(FineGrainedSplitGenerator.class);

    private final SplitGenerator baseGenerator;
    private final boolean enabled;
    private final long splitFileThreshold;
    private final int maxSplitsPerFile;
    private final FileIO fileIO;
    private final FileStorePathFactory pathFactory;
    private final Map<String, FormatMetadataReader> metadataReaders;

    /**
     * Map from file name to list of split boundaries. This is populated when fine-grained splitting
     * is performed and can be retrieved later.
     */
    private final Map<String, List<FileSplitBoundary>> fileBoundaries = new HashMap<>();

    public FineGrainedSplitGenerator(
            SplitGenerator baseGenerator,
            boolean enabled,
            long splitFileThreshold,
            int maxSplitsPerFile,
            FileIO fileIO,
            FileStorePathFactory pathFactory,
            Map<String, FormatMetadataReader> metadataReaders) {
        this.baseGenerator = baseGenerator;
        this.enabled = enabled;
        this.splitFileThreshold = splitFileThreshold;
        this.maxSplitsPerFile = maxSplitsPerFile;
        this.fileIO = fileIO;
        this.pathFactory = pathFactory;
        this.metadataReaders = metadataReaders;
    }

    /**
     * Get the split boundaries for a file. Returns empty list if the file was not split or if
     * boundaries are not available.
     */
    public List<FileSplitBoundary> getFileBoundaries(String fileName) {
        return fileBoundaries.getOrDefault(fileName, new ArrayList<>());
    }

    @Override
    public boolean alwaysRawConvertible() {
        return baseGenerator.alwaysRawConvertible();
    }

    @Override
    public List<SplitGroup> splitForBatch(List<DataFileMeta> files) {
        List<SplitGroup> baseGroups = baseGenerator.splitForBatch(files);

        if (!enabled || metadataReaders.isEmpty()) {
            return baseGroups;
        }

        List<SplitGroup> result = new ArrayList<>();

        for (SplitGroup group : baseGroups) {
            List<DataFileMeta> groupFiles = group.files;
            List<DataFileMeta> filesToSplit = new ArrayList<>();
            List<DataFileMeta> filesToKeep = new ArrayList<>();

            // Separate files that should be split from those that should be kept as-is
            for (DataFileMeta file : groupFiles) {
                if (shouldSplitFile(file)) {
                    filesToSplit.add(file);
                } else {
                    filesToKeep.add(file);
                }
            }

            // Add files that don't need splitting as-is
            if (!filesToKeep.isEmpty()) {
                result.add(
                        group.rawConvertible
                                ? SplitGroup.rawConvertibleGroup(filesToKeep)
                                : SplitGroup.nonRawConvertibleGroup(filesToKeep));
            }

            // Split large files into multiple groups
            for (DataFileMeta file : filesToSplit) {
                List<FileSplitBoundary> boundaries = getSplitBoundaries(file);
                if (boundaries.isEmpty() || boundaries.size() == 1) {
                    // Could not split or only one boundary, keep as single file
                    result.add(
                            group.rawConvertible
                                    ? SplitGroup.rawConvertibleGroup(
                                            Collections.singletonList(file))
                                    : SplitGroup.nonRawConvertibleGroup(
                                            Collections.singletonList(file)));
                } else {
                    // Create one SplitGroup per boundary
                    // Limit the number of splits per file
                    int numSplits = Math.min(boundaries.size(), maxSplitsPerFile);
                    List<FileSplitBoundary> boundariesToUse = boundaries.subList(0, numSplits);
                    // Store boundaries for later use in DataSplit.convertToRawFiles()
                    fileBoundaries.put(file.fileName(), boundariesToUse);
                    // Create multiple SplitGroups - each will become a separate DataSplit
                    // Note: All SplitGroups contain the same file, but boundaries are stored
                    // and will be used when converting to RawFile
                    for (int i = 0; i < numSplits; i++) {
                        result.add(
                                group.rawConvertible
                                        ? SplitGroup.rawConvertibleGroup(
                                                Collections.singletonList(file))
                                        : SplitGroup.nonRawConvertibleGroup(
                                                Collections.singletonList(file)));
                    }
                }
            }
        }

        return result;
    }

    @Override
    public List<SplitGroup> splitForStreaming(List<DataFileMeta> files) {
        // For streaming, don't split files - use base generator as-is
        return baseGenerator.splitForStreaming(files);
    }

    private boolean shouldSplitFile(DataFileMeta file) {
        return file.fileSize() >= splitFileThreshold
                && metadataReaders.containsKey(file.fileFormat().toLowerCase());
    }

    private List<FileSplitBoundary> getSplitBoundaries(DataFileMeta file) {
        String format = file.fileFormat().toLowerCase();
        FormatMetadataReader reader = metadataReaders.get(format);

        if (reader == null || !reader.supportsFinerGranularity()) {
            return new ArrayList<>();
        }

        try {
            // Construct path from external path if available
            // If external path is not available, we cannot construct the full path here
            // as we don't have partition/bucket info. In this case, return empty list
            // and the file will be treated as a single split (fallback behavior).
            Path filePath = file.externalPath().map(Path::new).orElse(null);

            if (filePath == null) {
                // Cannot construct full path without partition/bucket info
                // Fall back to file-level splitting
                LOG.debug(
                        "Cannot construct full path for file {} (no external path), "
                                + "skipping fine-grained splitting",
                        file.fileName());
                return new ArrayList<>();
            }

            List<FileSplitBoundary> boundaries =
                    reader.getSplitBoundaries(fileIO, filePath, file.fileSize());

            if (LOG.isDebugEnabled() && !boundaries.isEmpty()) {
                LOG.debug(
                        "Extracted {} boundaries for file {} (format: {})",
                        boundaries.size(),
                        file.fileName(),
                        format);
            }

            return boundaries;
        } catch (IOException e) {
            LOG.warn(
                    "Failed to extract split boundaries for file {} (format: {}), "
                            + "will treat as single split",
                    file.fileName(),
                    format,
                    e);
            return new ArrayList<>();
        }
    }
}
