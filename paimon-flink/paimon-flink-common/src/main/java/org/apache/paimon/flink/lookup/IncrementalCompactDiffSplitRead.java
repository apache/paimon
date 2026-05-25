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
import org.apache.paimon.table.source.IncrementalSplit;
import org.apache.paimon.table.source.Split;
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
    public RecordReader<InternalRow> createReader(Split split) throws IOException {
        IncrementalSplit incrementalSplit = (IncrementalSplit) split;
        if (incrementalSplit.beforeFiles().stream().noneMatch(file -> file.level() == 0)) {
            return new EmptyRecordReader<>();
        }
        return super.createReader(filterLevel0Files(incrementalSplit));
    }

    private IncrementalSplit filterLevel0Files(IncrementalSplit split) {
        List<DataFileMeta> beforeFiles =
                split.beforeFiles().stream()
                        .filter(file -> file.level() > 0)
                        .collect(Collectors.toList());
        List<DataFileMeta> afterFiles =
                split.afterFiles().stream()
                        .filter(file -> file.level() > 0)
                        .collect(Collectors.toList());
        return new IncrementalSplit(
                split.snapshotId(),
                split.partition(),
                split.bucket(),
                split.totalBuckets(),
                beforeFiles,
                null,
                afterFiles,
                null,
                split.isStreaming());
    }
}
