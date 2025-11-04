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
import org.apache.paimon.stats.Statistics;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.TableCommit;

import javax.annotation.Nullable;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringJoiner;

import static org.apache.paimon.table.format.FormatBatchWriteBuilder.validateStaticPartition;

/** Commit for Format Table. */
public class FormatTableCommit implements BatchTableCommit {

    private String location;
    private final boolean formatTablePartitionOnlyValueInPath;
    private FileIO fileIO;
    private List<String> partitionKeys;
    protected Map<String, String> staticPartitions;
    protected boolean overwrite = false;

    public FormatTableCommit(
            String location,
            List<String> partitionKeys,
            FileIO fileIO,
            boolean formatTablePartitionOnlyValueInPath,
            boolean overwrite,
            @Nullable Map<String, String> staticPartitions) {
        this.location = location;
        this.fileIO = fileIO;
        this.formatTablePartitionOnlyValueInPath = formatTablePartitionOnlyValueInPath;
        validateStaticPartition(staticPartitions, partitionKeys);
        this.staticPartitions = staticPartitions;
        this.overwrite = overwrite;
        this.partitionKeys = partitionKeys;
    }

    @Override
    public void commit(List<CommitMessage> commitMessages) {
        try {
            List<TwoPhaseOutputStream.Committer> committers = new ArrayList<>();
            for (CommitMessage commitMessage : commitMessages) {
                if (commitMessage instanceof TwoPhaseCommitMessage) {
                    committers.add(((TwoPhaseCommitMessage) commitMessage).getCommitter());
                } else {
                    throw new RuntimeException(
                            "Unsupported commit message type: "
                                    + commitMessage.getClass().getName());
                }
            }
            if (overwrite && staticPartitions != null && !staticPartitions.isEmpty()) {
                Path partitionPath =
                        buildPartitionPath(
                                location,
                                staticPartitions,
                                formatTablePartitionOnlyValueInPath,
                                partitionKeys);
                deletePreviousDataFile(partitionPath);
            } else if (overwrite) {
                Set<Path> partitionPaths = new HashSet<>();
                for (TwoPhaseOutputStream.Committer c : committers) {
                    partitionPaths.add(c.targetFilePath().getParent());
                }
                for (Path p : partitionPaths) {
                    deletePreviousDataFile(p);
                }
            }
            for (TwoPhaseOutputStream.Committer committer : committers) {
                committer.commit(this.fileIO);
            }
            for (TwoPhaseOutputStream.Committer committer : committers) {
                committer.clean(this.fileIO);
            }

        } catch (Exception e) {
            this.abort(commitMessages);
            throw new RuntimeException(e);
        }
    }

    private static Path buildPartitionPath(
            String location,
            Map<String, String> partitionSpec,
            boolean formatTablePartitionOnlyValueInPath,
            List<String> partitionKeys) {
        if (partitionSpec.isEmpty() || partitionKeys.isEmpty()) {
            throw new IllegalArgumentException("partitionSpec or partitionKeys is empty.");
        }
        StringJoiner joiner = new StringJoiner("/");
        for (int i = 0; i < partitionSpec.size(); i++) {
            String key = partitionKeys.get(i);
            if (partitionSpec.containsKey(key)) {
                if (formatTablePartitionOnlyValueInPath) {
                    joiner.add(partitionSpec.get(key));
                } else {
                    joiner.add(key + "=" + partitionSpec.get(key));
                }
            } else {
                throw new RuntimeException("partitionSpec does not contain key: " + key);
            }
        }
        return new Path(location, joiner.toString());
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
                    } catch (FileNotFoundException ignore) {
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
