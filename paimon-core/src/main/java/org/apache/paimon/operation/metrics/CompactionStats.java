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
    private final List<DataFileMeta> compactBefore;
    private final List<DataFileMeta> compactAfter;
    private final List<DataFileMeta> compactChangelog;

    public CompactionStats(
            long compactionDuration,
            List<DataFileMeta> compactBefore,
            List<DataFileMeta> compactAfter,
            List<DataFileMeta> compactChangelog) {
        this.duration = compactionDuration;
        this.compactBefore = compactBefore;
        this.compactAfter = compactAfter;
        this.compactChangelog = compactChangelog;
    }

    @VisibleForTesting
    protected long getDuration() {
        return duration;
    }

    protected long getCompactedDataFilesBefore() {
        return compactBefore.size();
    }

    protected long getCompactedDataFilesAfter() {
        return compactAfter.size();
    }

    protected long getCompactedChangelogs() {
        return compactChangelog.size();
    }

    protected long getRewriteInputFileSize() {
        return rewriteFileSize(compactBefore);
    }

    protected long getRewriteOutputFileSize() {
        return rewriteFileSize(compactAfter);
    }

    protected long getRewriteChangelogFileSize() {
        return rewriteFileSize(compactChangelog);
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
                + compactBefore.size()
                + ", compactedDataFilesAfter="
                + compactAfter.size()
                + ", compactedChangelogs="
                + compactChangelog.size()
                + ", rewriteInputFileSize="
                + rewriteFileSize(compactBefore)
                + ", rewriteOutputFileSize="
                + rewriteFileSize(compactAfter)
                + ", rewriteChangelogFileSize="
                + rewriteFileSize(compactChangelog)
                + '}';
    }
}
