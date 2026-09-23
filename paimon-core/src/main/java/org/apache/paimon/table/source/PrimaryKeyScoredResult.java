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
import org.apache.paimon.globalindex.IndexedSplit;
import org.apache.paimon.globalindex.ScoreGetter;
import org.apache.paimon.globalindex.ScoredGlobalIndexResult;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.utils.Range;
import org.apache.paimon.utils.RoaringNavigableMap64;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Snapshot-scoped scored search result addressed by physical primary-key row positions. */
public class PrimaryKeyScoredResult implements GlobalIndexSplitResult, ScoredGlobalIndexResult {

    private final long snapshotId;
    private final List<PrimaryKeySearchPosition> positions;
    private final List<IndexedSplit> splits;

    public PrimaryKeyScoredResult(
            long snapshotId,
            List<DataSplit> sourceSplits,
            List<PrimaryKeySearchPosition> positions) {
        this.snapshotId = snapshotId;
        this.positions = Collections.unmodifiableList(new ArrayList<>(positions));
        this.splits =
                Collections.unmodifiableList(
                        materializeSplits(snapshotId, sourceSplits, positions));
    }

    @Override
    public long snapshotId() {
        return snapshotId;
    }

    public List<PrimaryKeySearchPosition> positions() {
        return positions;
    }

    @Override
    public List<IndexedSplit> splits() {
        return splits;
    }

    @Override
    public RoaringNavigableMap64 results() {
        throw new UnsupportedOperationException(
                "Primary-key scored results use physical file positions, not global row ids.");
    }

    @Override
    public ScoreGetter scoreGetter() {
        throw new UnsupportedOperationException(
                "Primary-key scored results attach scores to physical file positions, not global row ids.");
    }

    private static List<IndexedSplit> materializeSplits(
            long snapshotId,
            List<DataSplit> sourceSplits,
            List<PrimaryKeySearchPosition> positions) {
        Map<FileKey, SourceFile> sources = sourceFiles(snapshotId, sourceSplits);
        TreeMap<PrimaryKeySearchPosition, PrimaryKeySearchPosition> orderedPositions =
                new TreeMap<>();
        for (PrimaryKeySearchPosition position : positions) {
            checkArgument(
                    orderedPositions.put(position, position) == null,
                    "Primary-key search result contains duplicate physical position %s.",
                    position);
        }

        Map<FileKey, TreeMap<Integer, Float>> selectedByFile = new LinkedHashMap<>();
        for (PrimaryKeySearchPosition position : orderedPositions.values()) {
            FileKey key = FileKey.from(position);
            SourceFile sourceFile = sources.get(key);
            checkArgument(
                    sourceFile != null,
                    "Primary-key search position references unknown data file %s in bucket %s.",
                    position.dataFileName(),
                    position.bucket());
            checkArgument(
                    position.rowPosition() < sourceFile.dataFile.rowCount()
                            && position.rowPosition() <= Integer.MAX_VALUE,
                    "Primary-key search row position %s is outside data file %s.",
                    position.rowPosition(),
                    position.dataFileName());
            selectedByFile
                    .computeIfAbsent(key, ignored -> new TreeMap<>())
                    .put((int) position.rowPosition(), position.score());
        }

        List<IndexedSplit> result = new ArrayList<>(selectedByFile.size());
        for (Map.Entry<FileKey, TreeMap<Integer, Float>> entry : selectedByFile.entrySet()) {
            SourceFile sourceFile = sources.get(entry.getKey());
            DataSplit source = sourceFile.split;
            DataSplit.Builder builder =
                    DataSplit.builder()
                            .withSnapshot(source.snapshotId())
                            .withPartition(source.partition())
                            .withBucket(source.bucket())
                            .withBucketPath(source.bucketPath())
                            .withTotalBuckets(source.totalBuckets())
                            .withDataFiles(Collections.singletonList(sourceFile.dataFile))
                            .isStreaming(false)
                            .rawConvertible(false);
            if (source.deletionFiles().isPresent()) {
                builder.withDataDeletionFiles(
                        Collections.singletonList(
                                source.deletionFiles().get().get(sourceFile.fileIndex)));
            }
            result.add(
                    new IndexedSplit(
                            builder.build(), ranges(entry.getValue()), scores(entry.getValue())));
        }
        return result;
    }

    private static Map<FileKey, SourceFile> sourceFiles(
            long snapshotId, List<DataSplit> sourceSplits) {
        Map<FileKey, SourceFile> result = new HashMap<>();
        for (DataSplit source : sourceSplits) {
            checkArgument(
                    source.snapshotId() == snapshotId,
                    "Primary-key source split snapshot %s does not match result snapshot %s.",
                    source.snapshotId(),
                    snapshotId);
            for (int i = 0; i < source.dataFiles().size(); i++) {
                DataFileMeta dataFile = source.dataFiles().get(i);
                FileKey key = new FileKey(source.partition(), source.bucket(), dataFile.fileName());
                checkArgument(
                        result.put(key, new SourceFile(source, dataFile, i)) == null,
                        "Data file %s appears more than once in primary-key search sources.",
                        dataFile.fileName());
            }
        }
        return result;
    }

    private static List<Range> ranges(TreeMap<Integer, Float> selected) {
        List<Range> result = new ArrayList<>();
        long from = -1;
        long to = -1;
        for (int position : selected.keySet()) {
            if (from < 0) {
                from = position;
            } else if (position != to + 1) {
                result.add(new Range(from, to));
                from = position;
            }
            to = position;
        }
        if (from >= 0) {
            result.add(new Range(from, to));
        }
        return result;
    }

    private static float[] scores(TreeMap<Integer, Float> selected) {
        float[] result = new float[selected.size()];
        int index = 0;
        for (float score : selected.values()) {
            result[index++] = score;
        }
        return result;
    }

    private static class SourceFile {

        private final DataSplit split;
        private final DataFileMeta dataFile;
        private final int fileIndex;

        private SourceFile(DataSplit split, DataFileMeta dataFile, int fileIndex) {
            this.split = split;
            this.dataFile = dataFile;
            this.fileIndex = fileIndex;
        }
    }

    private static class FileKey {

        private final BinaryRow partition;
        private final int bucket;
        private final String dataFileName;

        private FileKey(BinaryRow partition, int bucket, String dataFileName) {
            this.partition = partition.copy();
            this.bucket = bucket;
            this.dataFileName = dataFileName;
        }

        private static FileKey from(PrimaryKeySearchPosition position) {
            return new FileKey(position.partition(), position.bucket(), position.dataFileName());
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof FileKey)) {
                return false;
            }
            FileKey fileKey = (FileKey) o;
            return bucket == fileKey.bucket
                    && partition.equals(fileKey.partition)
                    && dataFileName.equals(fileKey.dataFileName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(partition, bucket, dataFileName);
        }
    }
}
