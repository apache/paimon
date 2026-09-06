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

package org.apache.paimon.flink.source;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.DeletionFile;
import org.apache.paimon.table.source.PostponeMergePlan;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.utils.Pair;

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.core.io.InputStatus;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Emits real-bucket markers and writer-grouped postpone splits for merge-on-read. */
final class PostponeMergeInputSource extends AbstractNonCoordinatedSource<Split> {

    private static final long serialVersionUID = 1L;

    private final List<Split> inputs;

    PostponeMergeInputSource(PostponeMergePlan plan) {
        this.inputs = new ArrayList<>();

        Map<Pair<BinaryRow, Integer>, List<DataSplit>> realBuckets = new LinkedHashMap<>();
        for (DataSplit split : plan.realSplits()) {
            realBuckets
                    .computeIfAbsent(
                            Pair.of(split.partition().copy(), split.bucket()),
                            ignored -> new ArrayList<>())
                    .add(split);
        }
        for (List<DataSplit> splits : realBuckets.values()) {
            inputs.add(mergeRealSplits(splits));
        }
        inputs.addAll(plan.postponeSplits());
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public SourceReader<Split, SimpleSourceSplit> createReader(SourceReaderContext readerContext) {
        return new Reader();
    }

    private class Reader extends AbstractNonCoordinatedSourceReader<Split> {

        private boolean emitted;

        @Override
        public InputStatus pollNext(ReaderOutput<Split> output) {
            if (!emitted) {
                for (Split input : inputs) {
                    output.collect(input);
                }
                emitted = true;
            }
            return InputStatus.END_OF_INPUT;
        }
    }

    private static DataSplit mergeRealSplits(List<DataSplit> splits) {
        if (splits.size() == 1) {
            return splits.get(0);
        }

        DataSplit first = splits.get(0);
        List<DataFileMeta> dataFiles = new ArrayList<>();
        List<DeletionFile> deletionFiles = new ArrayList<>();
        boolean hasDeletionFiles = false;
        boolean rawConvertible = true;
        for (DataSplit split : splits) {
            dataFiles.addAll(split.dataFiles());
            if (split.deletionFiles().isPresent()) {
                deletionFiles.addAll(split.deletionFiles().get());
                hasDeletionFiles = true;
            } else {
                deletionFiles.addAll(Collections.nCopies(split.dataFiles().size(), null));
            }
            rawConvertible &= split.rawConvertible();
        }

        DataSplit.Builder builder =
                DataSplit.builder()
                        .withSnapshot(first.snapshotId())
                        .withPartition(first.partition())
                        .withBucket(first.bucket())
                        .withBucketPath(first.bucketPath())
                        .withTotalBuckets(first.totalBuckets())
                        .withDataFiles(dataFiles)
                        .isStreaming(first.isStreaming())
                        .rawConvertible(rawConvertible);
        if (hasDeletionFiles) {
            builder.withDataDeletionFiles(deletionFiles);
        }
        return builder.build();
    }
}
