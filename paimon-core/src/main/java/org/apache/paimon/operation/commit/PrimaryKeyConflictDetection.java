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
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.manifest.SimpleFileEntry;
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
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/** Conflict detection for primary-key tables. */
public class PrimaryKeyConflictDetection extends ConflictDetection {

    private static final Logger LOG = LoggerFactory.getLogger(PrimaryKeyConflictDetection.class);

    private final FileStorePathFactory pathFactory;
    private final Comparator<InternalRow> keyComparator;
    private final boolean pkClusteringOverride;

    public PrimaryKeyConflictDetection(
            String tableName,
            String commitUser,
            RowType partitionType,
            FileStorePathFactory pathFactory,
            Comparator<InternalRow> keyComparator,
            BucketMode bucketMode,
            boolean deletionVectorsEnabled,
            boolean pkClusteringOverride,
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
        if (pkClusteringOverride) {
            return Optional.empty();
        }

        Map<LevelIdentifier, List<SimpleFileEntry>> levels = new HashMap<>();
        for (SimpleFileEntry entry : mergedEntries) {
            int level = entry.level();
            if (level >= 1) {
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
