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
import org.apache.paimon.partition.Partition;
import org.apache.paimon.partition.PartitionStatistics;
import org.apache.paimon.stats.Statistics;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.TableCommit;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.PartitionPathUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.paimon.table.format.FormatBatchWriteBuilder.validateStaticPartition;

/** Commit for Format Table. */
public class FormatTableCommit implements BatchTableCommit {

    private static final Logger LOG = LoggerFactory.getLogger(FormatTableCommit.class);

    private String location;
    private final boolean formatTablePartitionOnlyValueInPath;
    private final String defaultPartName;
    private FileIO fileIO;
    private List<String> partitionKeys;
    protected Map<String, String> staticPartitions;
    protected boolean overwrite = false;
    private Catalog hiveCatalog;
    private Identifier tableIdentifier;
    @Nullable private final FormatTablePartitionManager partitionManager;
    private final boolean dynamicPartitionOverwrite;

    public FormatTableCommit(
            String location,
            List<String> partitionKeys,
            FileIO fileIO,
            boolean formatTablePartitionOnlyValueInPath,
            String defaultPartName,
            boolean overwrite,
            Identifier tableIdentifier,
            @Nullable Map<String, String> staticPartitions,
            @Nullable String syncHiveUri,
            CatalogContext catalogContext,
            @Nullable FormatTablePartitionManager partitionManager,
            boolean dynamicPartitionOverwrite) {
        this.location = location;
        this.fileIO = fileIO;
        this.formatTablePartitionOnlyValueInPath = formatTablePartitionOnlyValueInPath;
        this.defaultPartName = defaultPartName;
        validateStaticPartition(staticPartitions, partitionKeys);
        this.staticPartitions = staticPartitions;
        this.overwrite = overwrite;
        this.partitionKeys = partitionKeys;
        this.tableIdentifier = tableIdentifier;
        this.partitionManager = partitionManager;
        this.dynamicPartitionOverwrite = dynamicPartitionOverwrite;
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
            // One reading for the whole commit: stat-ing each file back costs a request per
            // file for a coarser number.
            long commitTime = System.currentTimeMillis();
            List<TwoPhaseCommitMessage> messages = new ArrayList<>();
            for (CommitMessage commitMessage : commitMessages) {
                if (commitMessage instanceof TwoPhaseCommitMessage) {
                    messages.add((TwoPhaseCommitMessage) commitMessage);
                } else {
                    throw new RuntimeException(
                            "Unsupported commit message type: "
                                    + commitMessage.getClass().getName());
                }
            }

            Set<Map<String, String>> partitionSpecs = new HashSet<>();
            Set<Path> clearedPartitionPaths = new HashSet<>();

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
                    // A static partition may name only the leading keys, in which case the path
                    // is a prefix and the partition directories of the remaining keys sit below.
                    clearedPartitionPaths.addAll(
                            deletePreviousDataFile(
                                    partitionPath, partitionKeys.size() - staticPartitions.size()));
                }
                if (!fileIO.exists(partitionPath)) {
                    fileIO.mkdirs(partitionPath);
                }
            } else if (overwrite) {
                if (replacesOnlyWrittenPartitions()) {
                    Set<Path> partitionPaths = new HashSet<>();
                    for (TwoPhaseCommitMessage message : messages) {
                        partitionPaths.add(message.getCommitter().targetPath().getParent());
                    }
                    for (Path p : partitionPaths) {
                        // The parent of a written file is a complete partition directory - the
                        // table directory itself when the table is unpartitioned - so there is no
                        // partition level below it to descend, and it is a partition this commit
                        // writes anyway.
                        deletePreviousDataFile(p, 0);
                    }
                } else {
                    // Overwriting without naming a partition replaces the table, so what has to go
                    // is everything the table holds rather than the files this commit happens to
                    // write: a statement whose query returns nothing still empties the table.
                    for (Path dataDirectory : tableDataDirectories()) {
                        clearedPartitionPaths.addAll(deletePreviousDataFile(dataDirectory, 0));
                    }
                }
            }

            boolean registersPartitions =
                    partitionKeys != null
                            && !partitionKeys.isEmpty()
                            && (hiveCatalog != null || partitionManager != null);
            boolean reportsStatistics = registersPartitions && partitionManager != null;
            Map<Map<String, String>, PartitionStatistics> statisticsByPartition =
                    new LinkedHashMap<>();
            for (TwoPhaseCommitMessage message : messages) {
                TwoPhaseOutputStream.Committer committer = message.getCommitter();
                committer.commit(this.fileIO);
                if (registersPartitions) {
                    // Extracted once: registration and statistics must key on the same spec.
                    Map<String, String> spec =
                            extractPartitionSpecFromPath(
                                    committer.targetPath().getParent(), partitionKeys);
                    partitionSpecs.add(spec);
                    if (reportsStatistics) {
                        statisticsByPartition.merge(
                                spec,
                                new PartitionStatistics(
                                        spec,
                                        message.recordCount(),
                                        message.fileSizeInBytes(),
                                        1,
                                        commitTime,
                                        PartitionStatistics.UNKNOWN_TOTAL_BUCKETS),
                                FormatTableCommit::sum);
                    }
                }
            }
            for (TwoPhaseCommitMessage message : messages) {
                message.getCommitter().clean(this.fileIO);
            }
            if (reportsStatistics) {
                reportPartitions(
                        partitionSpecs,
                        statisticsByPartition,
                        clearedPartitionPaths,
                        commitTime,
                        overwrite);
            } else if (partitionManager != null && !partitionSpecs.isEmpty()) {
                // Concurrent writers may touch the same partition, so registration ignores the
                // ones that already exist rather than failing the commit.
                partitionManager.createPartitions(new ArrayList<>(partitionSpecs), true);
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

    /**
     * Registers the partitions this commit touched, carrying the statistics of what it wrote. An
     * overwrite also empties partitions it writes nothing to - those below a static prefix, and
     * every partition the table has when the statement names none and dynamic partition overwrite
     * is off; those report an exact zero and are registered with the rest, since a statistic can
     * only be reported for a partition its own request registers. A truncation writes nothing and
     * reports every partition it emptied.
     */
    private void reportPartitions(
            Set<Map<String, String>> writtenPartitionSpecs,
            Map<Map<String, String>, PartitionStatistics> statisticsByPartition,
            Set<Path> clearedPartitionPaths,
            long commitTime,
            boolean replaceStatistics) {
        for (Path cleared : clearedPartitionPaths) {
            Map<String, String> spec = clearedPartitionSpec(cleared);
            if (spec != null) {
                // Emptied and not written to: an exact zero, dated to the commit that did it.
                statisticsByPartition.putIfAbsent(spec, emptyStatistics(spec, commitTime));
            }
        }

        // Statistics are matched by spec, not by position: the specs need only be a superset.
        Set<Map<String, String>> specs = new LinkedHashSet<>(writtenPartitionSpecs);
        specs.addAll(statisticsByPartition.keySet());
        if (specs.isEmpty()) {
            return;
        }
        // A commit that replaced what the partitions held reports a total; an appending one saw
        // only its own files, so its numbers are an increment.
        partitionManager.createPartitions(
                new ArrayList<>(specs),
                true,
                new ArrayList<>(statisticsByPartition.values()),
                replaceStatistics);
    }

    /** What one commit wrote into a partition, with one more of its files folded in. */
    private static PartitionStatistics sum(PartitionStatistics summed, PartitionStatistics file) {
        return new PartitionStatistics(
                summed.spec(),
                add(summed.recordCount(), file.recordCount()),
                add(summed.fileSizeInBytes(), file.fileSizeInBytes()),
                summed.fileCount() + file.fileCount(),
                summed.lastFileCreationTime(),
                PartitionStatistics.UNKNOWN_TOTAL_BUCKETS);
    }

    /** A count nobody took leaves that field unknown for the whole partition. */
    private static long add(long sum, long value) {
        return PartitionStatistics.isKnown(sum) && PartitionStatistics.isKnown(value)
                ? sum + value
                : PartitionStatistics.UNKNOWN;
    }

    /**
     * The partition a cleared directory belongs to, or null when it is none of this table's.
     * Requiring the spec to rebuild the same directory rules out one nested below a partition,
     * whose trailing components would otherwise read as some other partition; such a directory is
     * left alone, since stale statistics beat statistics of the wrong partition.
     */
    @Nullable
    private Map<String, String> clearedPartitionSpec(Path clearedPath) {
        LinkedHashMap<String, String> spec =
                formatTablePartitionOnlyValueInPath
                        ? PartitionPathUtils.extractPartitionSpecFromPathOnlyValue(
                                clearedPath, partitionKeys)
                        : PartitionPathUtils.extractPartitionSpecFromPath(
                                clearedPath, partitionKeys);
        if (spec == null) {
            LOG.warn(
                    "Cleared directory {} of table {} is not one of its partition directories; "
                            + "its partition statistics are left unchanged.",
                    clearedPath,
                    tableIdentifier.getFullName());
            return null;
        }
        Path rebuilt =
                buildPartitionPath(
                        location, spec, formatTablePartitionOnlyValueInPath, partitionKeys);
        if (!samePathComponent(rebuilt, clearedPath)) {
            LOG.warn(
                    "Cleared directory {} of table {} does not rebuild from partition spec {}; "
                            + "its partition statistics are left unchanged.",
                    clearedPath,
                    tableIdentifier.getFullName(),
                    spec);
            return null;
        }
        return spec;
    }

    /**
     * Whether two paths name the same directory, ignoring scheme and authority: a {@link FileIO}
     * that delegates answers a listing under the scheme it used, not the one it was asked with.
     */
    private static boolean samePathComponent(Path left, Path right) {
        return trimTrailingSeparators(left.toUri().normalize().getPath())
                .equals(trimTrailingSeparators(right.toUri().normalize().getPath()));
    }

    private static String trimTrailingSeparators(String path) {
        String trimmed = path;
        while (trimmed.length() > 1 && trimmed.endsWith(Path.SEPARATOR)) {
            trimmed = trimmed.substring(0, trimmed.length() - 1);
        }
        return trimmed;
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
        // Only the trailing partitionKeys.size() components are the spec: the table location
        // itself may contain foreign 'k=v' segments that must not leak into what is registered.
        LinkedHashMap<String, String> partitionSpec =
                formatTablePartitionOnlyValueInPath
                        ? PartitionPathUtils.extractPartitionSpecFromPathOnlyValue(
                                partitionPath, partitionKeys)
                        : PartitionPathUtils.extractPartitionSpecFromPath(
                                partitionPath, partitionKeys);
        if (partitionSpec == null) {
            throw new IllegalArgumentException(
                    String.format(
                            "Partition path '%s' does not end in the %s partition directories of "
                                    + "table %s declared by partition keys %s.",
                            partitionPath,
                            partitionKeys.size(),
                            tableIdentifier.getFullName(),
                            partitionKeys));
        }
        return partitionSpec;
    }

    private static Path buildPartitionPath(
            String location,
            Map<String, String> partitionSpec,
            boolean formatTablePartitionOnlyValueInPath,
            List<String> partitionKeys) {
        if (partitionSpec.isEmpty() || partitionKeys.isEmpty()) {
            throw new IllegalArgumentException("partitionSpec or partitionKeys is empty.");
        }
        if (partitionSpec.size() > partitionKeys.size()) {
            throw new IllegalArgumentException(
                    String.format(
                            "Partition spec %s names more values than the partition keys %s.",
                            partitionSpec, partitionKeys));
        }
        LinkedHashMap<String, String> orderedSpec = new LinkedHashMap<>();
        for (int i = 0; i < partitionSpec.size(); i++) {
            String key = partitionKeys.get(i);
            if (partitionSpec.containsKey(key)) {
                orderedSpec.put(key, partitionSpec.get(key));
            } else {
                throw new RuntimeException("partitionSpec does not contain key: " + key);
            }
        }
        return new Path(
                location,
                PartitionPathUtils.generatePartitionPathUtil(
                        orderedSpec, formatTablePartitionOnlyValueInPath));
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

    /**
     * Whether an overwrite that names no partition replaces only the partitions this commit wrote
     * rather than everything the table holds. Same condition a data table commit applies: the
     * option is about which partitions to replace, so an unpartitioned table has nothing for it to
     * select.
     */
    private boolean replacesOnlyWrittenPartitions() {
        return partitionKeys != null && !partitionKeys.isEmpty() && dynamicPartitionOverwrite;
    }

    /**
     * The directories this table's data sits in: the table directory itself when the table is
     * unpartitioned, and one per partition otherwise, taken from wherever the table reads its
     * partitions. A directory no scan of the table reads holds none of its data - one the catalog
     * has not registered, or one whose name does not parse into the partition keys - and replacing
     * what the table holds leaves it alone, the way {@link #truncateTable()} does.
     */
    private List<Path> tableDataDirectories() {
        if (partitionKeys == null || partitionKeys.isEmpty()) {
            return Collections.singletonList(new Path(location));
        }
        List<Path> directories = new ArrayList<>();
        if (partitionManager != null) {
            for (Map<String, String> spec : registeredPartitions(Collections.emptyMap())) {
                directories.add(
                        buildPartitionPath(
                                location,
                                spec,
                                formatTablePartitionOnlyValueInPath,
                                partitionKeys));
            }
            return directories;
        }
        for (Pair<LinkedHashMap<String, String>, Path> partition : partitionsInTheFileSystem()) {
            directories.add(partition.getRight());
        }
        return directories;
    }

    /** The partition directories a table that discovers its partitions from the files has. */
    private List<Pair<LinkedHashMap<String, String>, Path>> partitionsInTheFileSystem() {
        return PartitionPathUtils.searchPartSpecAndPaths(
                fileIO,
                new Path(location),
                partitionKeys.size(),
                partitionKeys,
                formatTablePartitionOnlyValueInPath,
                null,
                null,
                defaultPartName);
    }

    /**
     * Deletes the data files below a path and returns the directories they sat in. Those can be
     * partitions this commit never writes: a static prefix overwrite clears the partitions sitting
     * below the prefix, and an overwrite that names no partition clears every partition the table
     * has.
     */
    private Set<Path> deletePreviousDataFile(Path partitionPath, int partitionLevels)
            throws IOException {
        Set<Path> clearedPartitionPaths = new HashSet<>();
        if (fileIO.exists(partitionPath)) {
            // Committed data files only: what sits under a staging directory is another writer's
            // uncommitted output, whatever its name looks like.
            for (FileStatus file :
                    FormatTableScan.listDataFiles(
                            fileIO,
                            partitionPath,
                            partitionLevels,
                            formatTablePartitionOnlyValueInPath,
                            defaultPartName)) {
                boolean deleted;
                try {
                    deleted = fileIO.delete(file.getPath(), false);
                } catch (FileNotFoundException ignore) {
                    continue;
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                if (deleted) {
                    // Only what this commit removed: a file another writer deleted first would
                    // have every concurrent writer report the whole subtree.
                    clearedPartitionPaths.add(file.getPath().getParent());
                } else if (fileIO.exists(file.getPath())) {
                    // A refusal is not that race: the file is still readable, and going on would
                    // report the partition as holding nothing while its rows are still there.
                    throw new IOException(
                            String.format(
                                    "Failed to delete data file %s of table %s.",
                                    file.getPath(), tableIdentifier.getFullName()));
                }
            }
        }
        return clearedPartitionPaths;
    }

    @Override
    public void truncateTable() {
        // Data files only. The partition directories stay, and so do their catalog registrations:
        // emptying a table does not redefine which partitions it has.
        if (partitionKeys == null || partitionKeys.isEmpty()) {
            try {
                deletePreviousDataFile(new Path(location), 0);
            } catch (IOException e) {
                throw new RuntimeException(
                        String.format(
                                "Failed to truncate table %s.", tableIdentifier.getFullName()),
                        e);
            }
            return;
        }
        // Emptying the table is emptying every partition it has, and which those are is answered
        // by whatever the table reads its partitions from.
        if (partitionManager != null) {
            truncate(registeredPartitions(Collections.emptyMap()));
            return;
        }
        // Filesystem partition discovery: the partition directories the scan reads are the table.
        // A directory that does not parse into the partition keys is not one of them, so
        // truncating leaves it alone.
        for (Pair<LinkedHashMap<String, String>, Path> partition : partitionsInTheFileSystem()) {
            try {
                deletePreviousDataFile(partition.getRight(), 0);
            } catch (IOException e) {
                throw new RuntimeException(
                        String.format(
                                "Failed to truncate partition %s of table %s.",
                                partition.getLeft(), tableIdentifier.getFullName()),
                        e);
            }
        }
    }

    @Override
    public void truncatePartitions(List<Map<String, String>> partitionSpecs) {
        if (partitionManager == null) {
            truncate(partitionSpecs);
            return;
        }
        // Complete specs are asked for in one request; only a prefix has to be listed on its own.
        List<Map<String, String>> complete = new ArrayList<>();
        for (Map<String, String> partitionSpec : partitionSpecs) {
            if (partitionSpec.size() == partitionKeys.size()) {
                complete.add(partitionSpec);
            }
        }
        Set<Map<String, String>> registered =
                complete.isEmpty()
                        ? Collections.emptySet()
                        : partitionManager.listPartitionsByNames(complete).stream()
                                .map(Partition::spec)
                                .collect(Collectors.toSet());
        List<Map<String, String>> partitions = new ArrayList<>();
        for (Map<String, String> partitionSpec : partitionSpecs) {
            if (partitionSpec.size() == partitionKeys.size()) {
                if (registered.contains(partitionSpec)) {
                    partitions.add(partitionSpec);
                }
            } else {
                partitions.addAll(registeredPartitions(partitionSpec));
            }
        }
        truncate(partitions);
    }

    /**
     * The registered partitions named by {@code prefix}, which names only the leading partition
     * keys, or none of them. The catalog says which partitions a catalog-managed table has, so
     * truncating neither empties nor registers a directory still waiting for MSCK REPAIR TABLE.
     */
    private List<Map<String, String>> registeredPartitions(Map<String, String> prefix) {
        return partitionManager.listPartitions(prefix, null).stream()
                .map(Partition::spec)
                .collect(Collectors.toList());
    }

    private void truncate(List<Map<String, String>> partitionSpecs) {
        long truncateTime = System.currentTimeMillis();
        Set<Path> clearedPartitionPaths = new HashSet<>();
        // Statistics are keyed by the spec that named the partition, so only a complete one can
        // seed them; a prefix reaches here only for a table with nowhere to report to.
        Map<Map<String, String>, PartitionStatistics> emptied = new LinkedHashMap<>();
        RuntimeException failure = null;
        for (Map<String, String> partitionSpec : partitionSpecs) {
            Path partitionPath =
                    buildPartitionPath(
                            location,
                            partitionSpec,
                            formatTablePartitionOnlyValueInPath,
                            partitionKeys);
            try {
                clearedPartitionPaths.addAll(
                        deletePreviousDataFile(
                                partitionPath, partitionKeys.size() - partitionSpec.size()));
            } catch (Exception e) {
                failure =
                        new RuntimeException(
                                String.format(
                                        "Failed to truncate partition %s of table %s.",
                                        partitionSpec, tableIdentifier.getFullName()),
                                e);
                break;
            }
            if (partitionSpec.size() == partitionKeys.size()) {
                emptied.put(partitionSpec, emptyStatistics(partitionSpec, truncateTime));
            }
        }
        if (partitionManager != null) {
            // Truncating states that the partition holds nothing, whoever deleted the files, so
            // one that was already empty reports zero as well. An overwrite reports only what it
            // removed itself, so that concurrent writers do not each claim the whole subtree;
            // truncation makes the claim on purpose. What a failed truncation emptied is reported
            // too, so the catalog stops describing files that are gone.
            try {
                reportPartitions(
                        Collections.emptySet(),
                        emptied,
                        clearedPartitionPaths,
                        truncateTime,
                        /* replaceStatistics */ true);
            } catch (RuntimeException e) {
                if (failure == null) {
                    throw e;
                }
                // The deletion that failed first is the one that explains what went wrong.
                failure.addSuppressed(e);
            }
        }
        if (failure != null) {
            throw failure;
        }
    }

    /** What a partition holds once it has been emptied, dated to the commit that emptied it. */
    private static PartitionStatistics emptyStatistics(
            Map<String, String> partitionSpec, long emptiedTime) {
        return new PartitionStatistics(
                partitionSpec, 0, 0, 0, emptiedTime, PartitionStatistics.UNKNOWN_TOTAL_BUCKETS);
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
