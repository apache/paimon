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

package org.apache.paimon.flink.lookup;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.operation.MergeFileSplitRead;
import org.apache.paimon.operation.SplitRead;
import org.apache.paimon.reader.EmptyRecordReader;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.splitread.IncrementalDiffSplitRead;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

/** A {@link SplitRead} for streaming incremental diff after compaction. */
public class IncrementalCompactDiffSplitRead extends IncrementalDiffSplitRead {

    public IncrementalCompactDiffSplitRead(MergeFileSplitRead mergeRead) {
        super(mergeRead);
    }

    @Override
    public RecordReader<InternalRow> createReader(DataSplit split) throws IOException {
        if (split.beforeFiles().stream().noneMatch(file -> file.level() == 0)) {
            return new EmptyRecordReader<>();
        }
        return super.createReader(filterLevel0Files(split));
    }

    private DataSplit filterLevel0Files(DataSplit split) {
        List<DataFileMeta> beforeFiles =
                split.beforeFiles().stream()
                        .filter(file -> file.level() > 0)
                        .collect(Collectors.toList());
        List<DataFileMeta> afterFiles =
                split.dataFiles().stream()
                        .filter(file -> file.level() > 0)
                        .collect(Collectors.toList());
        DataSplit.Builder builder =
                new DataSplit.Builder()
                        .withSnapshot(split.snapshotId())
                        .withPartition(split.partition())
                        .withBucket(split.bucket())
                        .withBucketPath(split.bucketPath())
                        .withBeforeFiles(beforeFiles)
                        .withDataFiles(afterFiles)
                        .isStreaming(split.isStreaming())
                        .rawConvertible(split.rawConvertible());

        if (split.beforeDeletionFiles().isPresent()) {
            builder.withBeforeDeletionFiles(split.beforeDeletionFiles().get());
        }
        if (split.deletionFiles().isPresent()) {
            builder.withDataDeletionFiles(split.deletionFiles().get());
        }
        return builder.build();
    }
}
