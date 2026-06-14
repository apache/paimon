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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.format.FileSplitBoundary;
import org.apache.paimon.format.FormatMetadataReader;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.io.DataFileMeta;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

/**
 * Decorator that splits eligible single-file {@link SplitGroup}s further into format-native
 * sub-splits (e.g. Parquet row groups). Pass-through for any group it cannot or should not split.
 */
public class FineGrainedSplitGenerator implements SplitGenerator {

    private final SplitGenerator delegate;
    private final FileIO fileIO;
    private final Function<DataFileMeta, Path> filePathResolver;
    private final Function<String, FormatMetadataReader> metadataReaderProvider;
    private final long targetSplitSize;
    private final int maxSplits;

    public FineGrainedSplitGenerator(
            SplitGenerator delegate,
            FileIO fileIO,
            Function<DataFileMeta, Path> filePathResolver,
            Function<String, FormatMetadataReader> metadataReaderProvider,
            long targetSplitSize,
            int maxSplits) {
        this.delegate = delegate;
        this.fileIO = fileIO;
        this.filePathResolver = filePathResolver;
        this.metadataReaderProvider = metadataReaderProvider;
        this.targetSplitSize = targetSplitSize;
        this.maxSplits = Math.max(1, maxSplits);
    }

    @Override
    public boolean alwaysRawConvertible() {
        return delegate.alwaysRawConvertible();
    }

    @Override
    public List<SplitGroup> splitForBatch(List<DataFileMeta> files) {
        List<SplitGroup> base = delegate.splitForBatch(files);
        List<SplitGroup> out = new ArrayList<>(base.size());
        for (SplitGroup group : base) {
            out.addAll(maybeExpand(group));
        }
        return out;
    }

    @Override
    public List<SplitGroup> splitForStreaming(List<DataFileMeta> files) {
        return delegate.splitForStreaming(files);
    }

    private List<SplitGroup> maybeExpand(SplitGroup group) {
        if (!group.rawConvertible || group.boundary != null || group.files.size() != 1) {
            return Collections.singletonList(group);
        }
        DataFileMeta file = group.files.get(0);
        if (!CoreOptions.FILE_FORMAT_PARQUET.equalsIgnoreCase(file.fileFormat())) {
            return Collections.singletonList(group);
        }
        if (file.fileSize() <= targetSplitSize) {
            return Collections.singletonList(group);
        }
        FormatMetadataReader reader = metadataReaderProvider.apply(file.fileFormat());
        if (reader == null || !reader.supportsFinerGranularity()) {
            return Collections.singletonList(group);
        }

        List<FileSplitBoundary> rowGroups;
        try {
            rowGroups =
                    reader.getSplitBoundaries(fileIO, filePathResolver.apply(file), file.fileSize());
        } catch (Exception e) {
            return Collections.singletonList(group);
        }
        if (rowGroups.isEmpty() || rowGroups.size() == 1) {
            return Collections.singletonList(group);
        }

        List<FileSplitBoundary> coalesced = coalesce(rowGroups, targetSplitSize, maxSplits);
        List<SplitGroup> out = new ArrayList<>(coalesced.size());
        for (FileSplitBoundary b : coalesced) {
            out.add(SplitGroup.rawConvertibleGroup(group.files, b));
        }
        return out;
    }

    static List<FileSplitBoundary> coalesce(
            List<FileSplitBoundary> input, long targetSize, int maxSplits) {
        List<FileSplitBoundary> out = new ArrayList<>();
        long accOffset = input.get(0).offset();
        long accLength = 0;
        long accRows = 0;
        for (FileSplitBoundary b : input) {
            if (accLength > 0 && accLength + b.length() > targetSize) {
                out.add(new FileSplitBoundary(accOffset, accLength, accRows));
                accOffset = b.offset();
                accLength = 0;
                accRows = 0;
            }
            accLength += b.length();
            accRows += b.rowCount();
        }
        if (accLength > 0) {
            out.add(new FileSplitBoundary(accOffset, accLength, accRows));
        }
        if (out.size() > maxSplits) {
            List<FileSplitBoundary> capped = new ArrayList<>(maxSplits);
            for (int i = 0; i < maxSplits - 1; i++) {
                capped.add(out.get(i));
            }
            FileSplitBoundary first = out.get(maxSplits - 1);
            long tailOffset = first.offset();
            long tailLength = first.length();
            long tailRows = first.rowCount();
            for (int i = maxSplits; i < out.size(); i++) {
                FileSplitBoundary b = out.get(i);
                tailLength += b.length();
                tailRows += b.rowCount();
            }
            capped.add(new FileSplitBoundary(tailOffset, tailLength, tailRows));
            return capped;
        }
        return out;
    }
}
