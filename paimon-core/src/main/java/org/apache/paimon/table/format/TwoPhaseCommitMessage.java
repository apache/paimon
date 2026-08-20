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

package org.apache.paimon.table.format;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.fs.TwoPhaseOutputStream;
import org.apache.paimon.partition.PartitionStatistics;
import org.apache.paimon.table.sink.CommitMessage;

import javax.annotation.Nullable;

/**
 * {@link CommitMessage} implementation for format table.
 *
 * <p>Carries the row count and byte size of the one file it commits, counted while writing. The
 * partition is not carried: {@link FormatTableCommit} derives it from the committer's target path,
 * and deriving it once keeps the statistics and the registered partition from ever disagreeing.
 */
public class TwoPhaseCommitMessage implements CommitMessage {

    private static final long serialVersionUID = 1L;

    private final TwoPhaseOutputStream.Committer committer;
    private final long recordCount;
    private final long fileSizeInBytes;

    public TwoPhaseCommitMessage(TwoPhaseOutputStream.Committer committer) {
        this(committer, PartitionStatistics.UNKNOWN, PartitionStatistics.UNKNOWN);
    }

    public TwoPhaseCommitMessage(
            TwoPhaseOutputStream.Committer committer, long recordCount, long fileSizeInBytes) {
        this.committer = committer;
        this.recordCount = recordCount;
        this.fileSizeInBytes = fileSizeInBytes;
    }

    @Override
    public BinaryRow partition() {
        return null;
    }

    @Override
    public int bucket() {
        return 0;
    }

    @Override
    public @Nullable Integer totalBuckets() {
        return 0;
    }

    public TwoPhaseOutputStream.Committer getCommitter() {
        return committer;
    }

    /** Rows in this file, or {@link PartitionStatistics#UNKNOWN} when nobody counted them. */
    public long recordCount() {
        return recordCount;
    }

    /** Bytes in this file, or {@link PartitionStatistics#UNKNOWN} when nobody measured them. */
    public long fileSizeInBytes() {
        return fileSizeInBytes;
    }
}
