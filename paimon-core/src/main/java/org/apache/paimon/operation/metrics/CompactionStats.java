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

package org.apache.paimon.operation.metrics;

import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.io.DataFileMeta;

import java.util.List;

/** Statistics for a compaction. */
public class CompactionStats {
    private final long duration;
    private final long compactedDataFilesBefore;
    private final long compactedDataFilesAfter;
    private final long compactedChangelogs;
    private final long rewriteInputFileSize;
    private final long rewriteOutputFileSize;
    private final long rewriteChangelogFileSize;

    public CompactionStats(
            long compactionDuration,
            List<DataFileMeta> compactBefore,
            List<DataFileMeta> compactAfter,
            List<DataFileMeta> compactChangelog) {
        this.duration = compactionDuration;
        this.compactedDataFilesBefore = compactBefore.size();
        this.compactedDataFilesAfter = compactAfter.size();
        this.compactedChangelogs = compactChangelog.size();
        this.rewriteInputFileSize = rewriteFileSize(compactBefore);
        this.rewriteOutputFileSize = rewriteFileSize(compactAfter);
        this.rewriteChangelogFileSize = rewriteFileSize(compactChangelog);
    }

    @VisibleForTesting
    protected long getDuration() {
        return duration;
    }

    protected long getCompactedDataFilesBefore() {
        return compactedDataFilesBefore;
    }

    protected long getCompactedDataFilesAfter() {
        return compactedDataFilesAfter;
    }

    protected long getCompactedChangelogs() {
        return compactedChangelogs;
    }

    protected long getRewriteInputFileSize() {
        return rewriteInputFileSize;
    }

    protected long getRewriteOutputFileSize() {
        return rewriteOutputFileSize;
    }

    protected long getRewriteChangelogFileSize() {
        return rewriteChangelogFileSize;
    }

    private long rewriteFileSize(List<DataFileMeta> files) {
        return files.stream().mapToLong(DataFileMeta::fileSize).sum();
    }

    @Override
    public String toString() {
        return "CompactionStats{"
                + "duration="
                + duration
                + ", compactedDataFilesBefore="
                + compactedDataFilesBefore
                + ", compactedDataFilesAfter="
                + compactedDataFilesAfter
                + ", compactedChangelogs="
                + compactedChangelogs
                + ", rewriteInputFileSize="
                + rewriteInputFileSize
                + ", rewriteOutputFileSize="
                + rewriteOutputFileSize
                + ", rewriteChangelogFileSize="
                + rewriteChangelogFileSize
                + '}';
    }
}
