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

package org.apache.paimon.operation.commit;

import org.apache.paimon.Snapshot;
import org.apache.paimon.Snapshot.CommitKind;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.index.IndexFileHandler;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.manifest.SimpleFileEntry;
import org.apache.paimon.mergetree.dataevolution.ColumnFileGroups;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.FileStorePathFactory;
import org.apache.paimon.utils.Pair;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;

/** Conflict detection for primary-key tables. */
public class PrimaryKeyConflictDetection extends ConflictDetection {

    private static final Logger LOG = LoggerFactory.getLogger(PrimaryKeyConflictDetection.class);

    private final FileStorePathFactory pathFactory;
    private final Comparator<InternalRow> keyComparator;
    private final boolean pkClusteringOverride;
    private final boolean dataEvolutionEnabled;

    public PrimaryKeyConflictDetection(
            String tableName,
            String commitUser,
            RowType partitionType,
            FileStorePathFactory pathFactory,
            Comparator<InternalRow> keyComparator,
            BucketMode bucketMode,
            boolean deletionVectorsEnabled,
            boolean pkClusteringOverride,
            boolean dataEvolutionEnabled,
            IndexFileHandler indexFileHandler,
            CommitScanner commitScanner) {
        super(
                tableName,
                commitUser,
                partitionType,
                pathFactory,
                bucketMode,
                deletionVectorsEnabled,
                indexFileHandler,
                commitScanner);
        this.pathFactory = pathFactory;
        this.keyComparator = keyComparator;
        this.pkClusteringOverride = pkClusteringOverride;
        this.dataEvolutionEnabled = dataEvolutionEnabled;
    }

    @Override
    public Comparator<InternalRow> keyComparator() {
        return keyComparator;
    }

    @Override
    protected Optional<RuntimeException> checkTableSpecificConflicts(
            Snapshot latestSnapshot,
            List<SimpleFileEntry> baseEntries,
            List<SimpleFileEntry> deltaEntries,
            List<IndexManifestEntry> deltaIndexEntries,
            Collection<SimpleFileEntry> mergedEntries,
            @Nullable RowIdConflictChecker rowIdConflictChecker,
            CommitKind commitKind,
            String baseCommitUser) {
        if (dataEvolutionEnabled) {
            Optional<String> alignmentConflict = checkAlignment(deltaEntries, mergedEntries);
            if (alignmentConflict.isPresent()) {
                Pair<RuntimeException, RuntimeException> conflictException =
                        createConflictException(
                                "Data evolution alignment conflicts detected! Give up committing. "
                                        + alignmentConflict.get(),
                                baseCommitUser,
                                baseEntries,
                                deltaEntries,
                                null);
                LOG.warn("", conflictException.getLeft());
                return Optional.of(conflictException.getRight());
            }
        }

        if (pkClusteringOverride) {
            return Optional.empty();
        }

        Map<LevelIdentifier, List<SimpleFileEntry>> levels = new HashMap<>();
        for (SimpleFileEntry entry : mergedEntries) {
            int level = entry.level();
            // Column files carry no keys, they belong to the LSM run of their base file.
            if (level >= 1 && !ColumnFileGroups.isColumnFile(entry.writeCols(), entry.minKey())) {
                levels.computeIfAbsent(
                                new LevelIdentifier(entry.partition(), entry.bucket(), level),
                                lv -> new ArrayList<>())
                        .add(entry);
            }
        }

        for (List<SimpleFileEntry> entries : levels.values()) {
            entries.sort((a, b) -> keyComparator.compare(a.minKey(), b.minKey()));
            for (int i = 0; i + 1 < entries.size(); i++) {
                SimpleFileEntry a = entries.get(i);
                SimpleFileEntry b = entries.get(i + 1);
                if (keyComparator.compare(a.maxKey(), b.minKey()) >= 0) {
                    Pair<RuntimeException, RuntimeException> conflictException =
                            createConflictException(
                                    "LSM conflicts detected! Give up committing. Conflict files are:\n"
                                            + a.identifier().toString(pathFactory)
                                            + "\n"
                                            + b.identifier().toString(pathFactory),
                                    baseCommitUser,
                                    baseEntries,
                                    deltaEntries,
                                    null);
                    LOG.warn("", conflictException.getLeft());
                    return Optional.of(conflictException.getRight());
                }
            }
        }
        return Optional.empty();
    }

    /**
     * Checks the alignment groups of every bucket changed by the delta. A group is the set of files
     * sharing {@code (partition, bucket, firstRowId)}: exactly one full-row base file and any
     * number of column files, all with the same row count, each column held by at most one file.
     * Ranges of different groups must not overlap.
     *
     * <p>This is the only check of these invariants at commit time, it relies on {@link
     * #checkTableSpecificConflicts} running for every commit. A path that skips conflict detection
     * for primary key tables would skip it too.
     */
    static Optional<String> checkAlignment(
            List<SimpleFileEntry> deltaEntries, Collection<SimpleFileEntry> mergedEntries) {
        Set<Pair<BinaryRow, Integer>> changedBuckets = new HashSet<>();
        for (SimpleFileEntry entry : deltaEntries) {
            changedBuckets.add(Pair.of(entry.partition(), entry.bucket()));
        }

        Map<Pair<BinaryRow, Integer>, TreeMap<Long, List<SimpleFileEntry>>> buckets =
                new HashMap<>();
        for (SimpleFileEntry entry : mergedEntries) {
            Pair<BinaryRow, Integer> bucket = Pair.of(entry.partition(), entry.bucket());
            if (entry.kind() != FileKind.ADD || !changedBuckets.contains(bucket)) {
                continue;
            }
            if (entry.firstRowId() == null) {
                if (ColumnFileGroups.isColumnFile(entry.writeCols(), entry.minKey())) {
                    return Optional.of(
                            String.format(
                                    "Column file %s does not have an alignment range.",
                                    entry.fileName()));
                }
                continue;
            }
            buckets.computeIfAbsent(bucket, b -> new TreeMap<>())
                    .computeIfAbsent(entry.firstRowId(), id -> new ArrayList<>())
                    .add(entry);
        }

        for (TreeMap<Long, List<SimpleFileEntry>> groups : buckets.values()) {
            long previousEnd = Long.MIN_VALUE;
            String previousFile = null;
            for (Map.Entry<Long, List<SimpleFileEntry>> group : groups.entrySet()) {
                Optional<String> groupConflict = checkGroup(group.getKey(), group.getValue());
                if (groupConflict.isPresent()) {
                    return groupConflict;
                }
                SimpleFileEntry first = group.getValue().get(0);
                if (group.getKey() < previousEnd) {
                    return Optional.of(
                            String.format(
                                    "Alignment range of file %s overlaps with file %s.",
                                    first.fileName(), previousFile));
                }
                previousEnd = group.getKey() + first.rowCount();
                previousFile = first.fileName();
            }
        }
        return Optional.empty();
    }

    private static Optional<String> checkGroup(long firstRowId, List<SimpleFileEntry> files) {
        long rowCount = files.get(0).rowCount();
        int baseFiles = 0;
        Map<String, String> columnOwners = new HashMap<>();
        for (SimpleFileEntry file : files) {
            if (file.rowCount() != rowCount) {
                return Optional.of(
                        String.format(
                                "Files %s and %s share first row id %s but have different row counts %s and %s.",
                                files.get(0).fileName(),
                                file.fileName(),
                                firstRowId,
                                rowCount,
                                file.rowCount()));
            }
            List<String> writeCols = file.writeCols();
            if (!ColumnFileGroups.isColumnFile(writeCols, file.minKey())) {
                baseFiles++;
                continue;
            }
            for (String column : writeCols) {
                String owner = columnOwners.put(column, file.fileName());
                if (owner != null) {
                    return Optional.of(
                            String.format(
                                    "Column %s of alignment range starting at %s is written by both %s and %s.",
                                    column, firstRowId, owner, file.fileName()));
                }
            }
        }
        if (baseFiles != 1) {
            return Optional.of(
                    String.format(
                            "Alignment range starting at %s must have exactly one file with the primary keys, but has %s. Files are %s.",
                            firstRowId, baseFiles, fileNames(files)));
        }
        return Optional.empty();
    }

    private static List<String> fileNames(List<SimpleFileEntry> files) {
        List<String> names = new ArrayList<>(files.size());
        for (SimpleFileEntry file : files) {
            names.add(file.fileName());
        }
        return names;
    }

    private static class LevelIdentifier {

        private final BinaryRow partition;
        private final int bucket;
        private final int level;

        private LevelIdentifier(BinaryRow partition, int bucket, int level) {
            this.partition = partition;
            this.bucket = bucket;
            this.level = level;
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof LevelIdentifier)) {
                return false;
            }
            LevelIdentifier that = (LevelIdentifier) o;
            return Objects.equals(partition, that.partition)
                    && bucket == that.bucket
                    && level == that.level;
        }

        @Override
        public int hashCode() {
            return Objects.hash(partition, bucket, level);
        }
    }
}
