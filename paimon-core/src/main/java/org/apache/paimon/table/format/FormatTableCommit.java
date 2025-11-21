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

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.DelegateCatalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.FileStatus;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.TwoPhaseOutputStream;
import org.apache.paimon.metrics.MetricRegistry;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.options.Options;
import org.apache.paimon.stats.Statistics;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.TableCommit;
import org.apache.paimon.utils.PartitionPathUtils;

import javax.annotation.Nullable;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
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
    private Catalog hiveCatalog;
    private Identifier tableIdentifier;

    public FormatTableCommit(
            String location,
            List<String> partitionKeys,
            FileIO fileIO,
            boolean formatTablePartitionOnlyValueInPath,
            boolean overwrite,
            Identifier tableIdentifier,
            @Nullable Map<String, String> staticPartitions,
            @Nullable String syncHiveUri,
            CatalogContext catalogContext) {
        this.location = location;
        this.fileIO = fileIO;
        this.formatTablePartitionOnlyValueInPath = formatTablePartitionOnlyValueInPath;
        validateStaticPartition(staticPartitions, partitionKeys);
        this.staticPartitions = staticPartitions;
        this.overwrite = overwrite;
        this.partitionKeys = partitionKeys;
        this.tableIdentifier = tableIdentifier;
        if (syncHiveUri != null) {
            try {
                Options options = new Options();
                options.set(CatalogOptions.URI, syncHiveUri);
                options.set(CatalogOptions.METASTORE, "hive");
                CatalogContext context =
                        CatalogContext.create(options, catalogContext.hadoopConf());
                this.hiveCatalog = CatalogFactory.createCatalog(context);
            } catch (Exception e) {
                throw new RuntimeException(
                        String.format(
                                "Failed to initialize Hive catalog with URI: %s", syncHiveUri),
                        e);
            }
        }
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

            Set<Map<String, String>> partitionSpecs = new HashSet<>();

            if (staticPartitions != null && !staticPartitions.isEmpty()) {
                Path partitionPath =
                        buildPartitionPath(
                                location,
                                staticPartitions,
                                formatTablePartitionOnlyValueInPath,
                                partitionKeys);
                if (staticPartitions.size() == partitionKeys.size()) {
                    partitionSpecs.add(staticPartitions);
                }
                if (overwrite) {
                    deletePreviousDataFile(partitionPath);
                }
                if (!fileIO.exists(partitionPath)) {
                    fileIO.mkdirs(partitionPath);
                }
            } else if (overwrite) {
                Set<Path> partitionPaths = new HashSet<>();
                for (TwoPhaseOutputStream.Committer c : committers) {
                    partitionPaths.add(c.targetPath().getParent());
                }
                for (Path p : partitionPaths) {
                    deletePreviousDataFile(p);
                }
            }

            for (TwoPhaseOutputStream.Committer committer : committers) {
                committer.commit(this.fileIO);
                if (partitionKeys != null && !partitionKeys.isEmpty() && hiveCatalog != null) {
                    partitionSpecs.add(
                            extractPartitionSpecFromPath(
                                    committer.targetPath().getParent(), partitionKeys));
                }
            }
            for (TwoPhaseOutputStream.Committer committer : committers) {
                committer.clean(this.fileIO);
            }
            for (Map<String, String> partitionSpec : partitionSpecs) {
                if (hiveCatalog != null) {
                    try {
                        if (hiveCatalog instanceof DelegateCatalog) {
                            hiveCatalog = ((DelegateCatalog) hiveCatalog).wrapped();
                        }
                        Method hiveCreatePartitionsInHmsMethod =
                                getHiveCreatePartitionsInHmsMethod();
                        hiveCreatePartitionsInHmsMethod.invoke(
                                hiveCatalog,
                                tableIdentifier,
                                Collections.singletonList(partitionSpec),
                                formatTablePartitionOnlyValueInPath);
                    } catch (Exception ex) {
                        throw new RuntimeException("Failed to sync partition to hms", ex);
                    }
                }
            }

        } catch (Exception e) {
            this.abort(commitMessages);
            throw new RuntimeException(e);
        }
    }

    private Method getHiveCreatePartitionsInHmsMethod() throws NoSuchMethodException {
        Method hiveCreatePartitionsInHmsMethod =
                hiveCatalog
                        .getClass()
                        .getDeclaredMethod(
                                "createPartitionsUtil",
                                Identifier.class,
                                List.class,
                                boolean.class);
        hiveCreatePartitionsInHmsMethod.setAccessible(true);
        return hiveCreatePartitionsInHmsMethod;
    }

    private LinkedHashMap<String, String> extractPartitionSpecFromPath(
            Path partitionPath, List<String> partitionKeys) {
        if (formatTablePartitionOnlyValueInPath) {
            return PartitionPathUtils.extractPartitionSpecFromPathOnlyValue(
                    partitionPath, partitionKeys);
        } else {
            return PartitionPathUtils.extractPartitionSpecFromPath(partitionPath);
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
