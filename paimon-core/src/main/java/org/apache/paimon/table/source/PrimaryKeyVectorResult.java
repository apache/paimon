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
import org.apache.paimon.globalindex.GlobalIndexResult;
import org.apache.paimon.globalindex.IndexedSplit;
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

import static org.apache.paimon.globalindex.VectorSearchMetric.normalize;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Snapshot-scoped vector result addressed by physical primary-key table row positions. */
public class PrimaryKeyVectorResult implements GlobalIndexResult {

    private final PrimaryKeyVectorScan.Plan plan;
    private final List<PrimaryKeyVectorRead.Candidate> candidates;
    private final String metric;

    PrimaryKeyVectorResult(
            PrimaryKeyVectorScan.Plan plan,
            List<PrimaryKeyVectorRead.Candidate> candidates,
            String metric) {
        this.plan = plan;
        this.candidates = Collections.unmodifiableList(new ArrayList<>(candidates));
        this.metric = normalize(metric);
    }

    public long snapshotId() {
        return plan.snapshotId();
    }

    public List<IndexedSplit> splits() {
        Map<FileKey, BucketVectorSearchSplit> sourceSplits = sourceSplits();
        Map<FileKey, TreeMap<Integer, Float>> selectedByFile = new LinkedHashMap<>();
        for (PrimaryKeyVectorRead.Candidate candidate : candidates) {
            FileKey key =
                    new FileKey(
                            candidate.partition(), candidate.bucket(), candidate.dataFileName());
            BucketVectorSearchSplit bucketSplit = sourceSplits.get(key);
            checkArgument(
                    bucketSplit != null,
                    "Primary-key vector candidate references unknown data file %s in bucket %s.",
                    candidate.dataFileName(),
                    candidate.bucket());
            DataFileMeta dataFile = dataFile(bucketSplit.dataSplit(), candidate.dataFileName());
            checkArgument(
                    candidate.rowPosition() >= 0
                            && candidate.rowPosition() < dataFile.rowCount()
                            && candidate.rowPosition() <= Integer.MAX_VALUE,
                    "Primary-key vector row position %s is outside data file %s.",
                    candidate.rowPosition(),
                    candidate.dataFileName());
            TreeMap<Integer, Float> selected =
                    selectedByFile.computeIfAbsent(key, ignored -> new TreeMap<>());
            checkArgument(
                    selected.put((int) candidate.rowPosition(), score(candidate.distance()))
                            == null,
                    "Primary-key vector candidate contains duplicate row position %s for data file %s.",
                    candidate.rowPosition(),
                    candidate.dataFileName());
        }

        List<IndexedSplit> result = new ArrayList<>(selectedByFile.size());
        for (Map.Entry<FileKey, TreeMap<Integer, Float>> entry : selectedByFile.entrySet()) {
            BucketVectorSearchSplit bucketSplit = sourceSplits.get(entry.getKey());
            DataSplit source = bucketSplit.dataSplit();
            int fileIndex = fileIndex(source, entry.getKey().dataFileName);
            DataSplit.Builder builder =
                    DataSplit.builder()
                            .withSnapshot(source.snapshotId())
                            .withPartition(source.partition())
                            .withBucket(source.bucket())
                            .withBucketPath(source.bucketPath())
                            .withTotalBuckets(source.totalBuckets())
                            .withDataFiles(
                                    Collections.singletonList(source.dataFiles().get(fileIndex)))
                            .isStreaming(false)
                            .rawConvertible(false);
            if (source.deletionFiles().isPresent()) {
                builder.withDataDeletionFiles(
                        Collections.singletonList(source.deletionFiles().get().get(fileIndex)));
            }
            result.add(
                    new IndexedSplit(
                            builder.build(), ranges(entry.getValue()), scores(entry.getValue())));
        }
        return Collections.unmodifiableList(result);
    }

    @Override
    public RoaringNavigableMap64 results() {
        throw new UnsupportedOperationException(
                "Primary-key vector results use physical file positions, not global row ids.");
    }

    private Map<FileKey, BucketVectorSearchSplit> sourceSplits() {
        Map<FileKey, BucketVectorSearchSplit> result = new HashMap<>();
        for (VectorSearchSplit searchSplit : plan.splits()) {
            BucketVectorSearchSplit bucketSplit = (BucketVectorSearchSplit) searchSplit;
            for (DataFileMeta dataFile : bucketSplit.dataSplit().dataFiles()) {
                FileKey key =
                        new FileKey(
                                bucketSplit.dataSplit().partition(),
                                bucketSplit.dataSplit().bucket(),
                                dataFile.fileName());
                checkArgument(
                        result.put(key, bucketSplit) == null,
                        "Data file %s appears more than once in primary-key vector plan.",
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

    private static DataFileMeta dataFile(DataSplit split, String dataFileName) {
        return split.dataFiles().get(fileIndex(split, dataFileName));
    }

    private static int fileIndex(DataSplit split, String dataFileName) {
        for (int i = 0; i < split.dataFiles().size(); i++) {
            if (dataFileName.equals(split.dataFiles().get(i).fileName())) {
                return i;
            }
        }
        throw new IllegalArgumentException(
                "Data file " + dataFileName + " does not exist in vector bucket split.");
    }

    private float score(float distance) {
        if ("l2".equals(metric)) {
            return 1F / (1F + distance);
        } else if ("cosine".equals(metric)) {
            return 1F - distance;
        } else if ("inner_product".equals(metric)) {
            return -distance;
        }
        throw new IllegalArgumentException("Unsupported primary-key vector metric: " + metric);
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

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
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
