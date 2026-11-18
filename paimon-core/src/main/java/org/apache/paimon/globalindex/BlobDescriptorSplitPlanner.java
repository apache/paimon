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

package org.apache.paimon.globalindex;

import org.apache.paimon.format.blob.BlobFileFormat;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.utils.Range;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/** Bounded row-range splits for the single-normal-file descriptor scan layout. */
class BlobDescriptorSplitPlanner {

    static List<Split> split(Split input, int maxFiles) {
        if (maxFiles <= 0 || !(input instanceof DataSplit)) {
            return Collections.singletonList(input);
        }
        DataSplit split = (DataSplit) input;
        if (split.isStreaming() || split.deletionFiles().isPresent()) {
            return Collections.singletonList(input);
        }
        DataFileMeta normal = null;
        List<DataFileMeta> blobs = new ArrayList<>();
        for (DataFileMeta file : split.dataFiles()) {
            if (BlobFileFormat.isBlobFile(file.fileName())) {
                blobs.add(file);
            } else if (normal == null) {
                normal = file;
            } else {
                return Collections.singletonList(input);
            }
        }
        if (normal == null || blobs.size() <= maxFiles) {
            return Collections.singletonList(input);
        }
        Range normalRange = normal.nonNullRowIdRange();
        blobs.sort(Comparator.comparingLong(DataFileMeta::nonNullFirstRowId));
        for (int i = 0; i < blobs.size(); i++) {
            Range range = blobs.get(i).nonNullRowIdRange();
            if (range.from < normalRange.from
                    || range.to > normalRange.to
                    || (i > 0 && blobs.get(i - 1).nonNullRowIdRange().to >= range.from)) {
                return Collections.singletonList(input);
            }
        }

        List<Split> result = new ArrayList<>();
        long from = normalRange.from;
        for (int start = 0; start < blobs.size(); start += maxFiles) {
            int end = (int) Math.min((long) start + maxFiles, blobs.size());
            long to = end == blobs.size() ? normalRange.to : blobs.get(end).nonNullFirstRowId() - 1;
            List<DataFileMeta> files = new ArrayList<>();
            files.add(normal);
            files.addAll(blobs.subList(start, end));
            DataSplit child =
                    DataSplit.builder()
                            .withSnapshot(split.snapshotId())
                            .withPartition(split.partition())
                            .withBucket(split.bucket())
                            .withBucketPath(split.bucketPath())
                            .withTotalBuckets(split.totalBuckets())
                            .withDataFiles(files)
                            .rawConvertible(false)
                            .build();
            // Keep gaps, including leading/trailing null BLOB rows, owned by exactly one child.
            result.add(
                    new IndexedSplit(child, Collections.singletonList(new Range(from, to)), null));
            if (end != blobs.size()) {
                from = to + 1;
            }
        }
        return result;
    }
}
