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

import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.FileStatus;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.TwoPhaseOutputStream;
import org.apache.paimon.metrics.MetricRegistry;
import org.apache.paimon.partition.PartitionUtils;
import org.apache.paimon.stats.Statistics;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.TableCommit;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Commit for Format Table. */
public class FormatTableCommit implements BatchTableCommit {

    private String location;
    private FileIO fileIO;
    protected Map<String, String> staticOverWritePartitions;
    protected boolean overwrite = false;

    public FormatTableCommit(
            String location,
            FileIO fileIO,
            boolean overwrite,
            Map<String, String> staticOverWritePartitions) {
        this.location = location;
        this.fileIO = fileIO;
        this.staticOverWritePartitions = staticOverWritePartitions;
        this.overwrite = overwrite;
    }

    @Override
    public void commit(List<CommitMessage> commitMessages) {
        try {
            List<TwoPhaseOutputStream.Committer> committers = new ArrayList<>();
            for (CommitMessage commitMessage : commitMessages) {
                if (commitMessage instanceof TwoPhaseCommitMessage) {
                    TwoPhaseCommitMessage twoPhaseCommitMessage =
                            (TwoPhaseCommitMessage) commitMessage;
                    committers.add(twoPhaseCommitMessage.getCommitter());
                } else {
                    throw new RuntimeException(
                            "Unsupported commit message type: "
                                    + commitMessage.getClass().getName());
                }
            }
            if (overwrite
                    && staticOverWritePartitions != null
                    && !staticOverWritePartitions.isEmpty()) {
                String child = PartitionUtils.buildPartitionName(staticOverWritePartitions);
                Path partitionPath = new Path(location, child);
                deletePreviousDataFile(partitionPath);
            } else if (overwrite) {
                Set<Path> parents = new HashSet<>();
                for (TwoPhaseOutputStream.Committer c : committers) {
                    Path parent = c.targetFilePath().getParent();
                    if (parent != null) {
                        parents.add(parent);
                    }
                }
                for (Path p : parents) {
                    deletePreviousDataFile(p);
                }
            }
            for (TwoPhaseOutputStream.Committer committer : committers) {
                committer.commit(this.fileIO);
            }
        } catch (Exception e) {
            this.abort(commitMessages);
            throw new RuntimeException(e);
        }
    }

    @Override
    public void abort(List<CommitMessage> commitMessages) {
        try {
            for (CommitMessage commitMessage : commitMessages) {
                if (commitMessage instanceof TwoPhaseCommitMessage) {
                    TwoPhaseCommitMessage twoPhaseCommitMessage =
                            (TwoPhaseCommitMessage) commitMessage;
                    twoPhaseCommitMessage.getCommitter().discard(this.fileIO);
                } else {
                    throw new RuntimeException(
                            "Unsupported commit message type: "
                                    + commitMessage.getClass().getName());
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() throws Exception {}

    private void deletePreviousDataFile(Path partitionPath) throws IOException {
        if (fileIO.exists(partitionPath)) {
            FileStatus[] files = fileIO.listFiles(partitionPath, true);
            for (FileStatus file : files) {
                if (FormatTableScan.isDataFileName(file.getPath().getName())) {
                    try {
                        fileIO.delete(file.getPath(), false);
                    } catch (FileNotFoundException e) {
                        // file already deleted
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        }
    }

    @Override
    public void truncateTable() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void truncatePartitions(List<Map<String, String>> partitionSpecs) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateStatistics(Statistics statistics) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void compactManifests() {
        throw new UnsupportedOperationException();
    }

    @Override
    public TableCommit withMetricRegistry(MetricRegistry registry) {
        throw new UnsupportedOperationException();
    }
}
