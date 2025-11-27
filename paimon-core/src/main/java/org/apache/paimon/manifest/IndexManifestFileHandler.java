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

package org.apache.paimon.manifest;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.index.DeletionVectorMeta;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.table.BucketMode;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.paimon.deletionvectors.DeletionVectorsIndexFile.DELETION_VECTORS_INDEX;
import static org.apache.paimon.index.HashIndexFile.HASH_INDEX;
import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.paimon.utils.Preconditions.checkState;

/** IndexManifestFile Handler. */
public class IndexManifestFileHandler {

    private final IndexManifestFile indexManifestFile;

    private final BucketMode bucketMode;

    IndexManifestFileHandler(IndexManifestFile indexManifestFile, BucketMode bucketMode) {
        this.indexManifestFile = indexManifestFile;
        this.bucketMode = bucketMode;
    }

    String write(@Nullable String previousIndexManifest, List<IndexManifestEntry> newIndexFiles) {
        List<IndexManifestEntry> entries =
                previousIndexManifest == null
                        ? new ArrayList<>()
                        : indexManifestFile.read(previousIndexManifest);
        for (IndexManifestEntry entry : entries) {
            checkArgument(entry.kind() == FileKind.ADD);
        }

        Map<String, List<IndexManifestEntry>> previous = separateIndexEntries(entries);
        Map<String, List<IndexManifestEntry>> current = separateIndexEntries(newIndexFiles);

        List<IndexManifestEntry> indexEntries = new ArrayList<>();
        Set<String> indexes = new HashSet<>();
        indexes.addAll(previous.keySet());
        indexes.addAll(current.keySet());
        for (String indexName : indexes) {
            indexEntries.addAll(
                    getIndexManifestFileCombine(indexName)
                            .combine(
                                    previous.getOrDefault(indexName, Collections.emptyList()),
                                    current.getOrDefault(indexName, Collections.emptyList())));
        }

        return indexManifestFile.writeWithoutRolling(indexEntries);
    }

    private Map<String, List<IndexManifestEntry>> separateIndexEntries(
            List<IndexManifestEntry> indexFiles) {
        Map<String, List<IndexManifestEntry>> result = new HashMap<>();

        for (IndexManifestEntry entry : indexFiles) {
            String indexType = entry.indexFile().indexType();
            result.computeIfAbsent(indexType, k -> new ArrayList<>()).add(entry);
        }
        return result;
    }

    private IndexManifestFileCombiner getIndexManifestFileCombine(String indexType) {
        if (!DELETION_VECTORS_INDEX.equals(indexType) && !HASH_INDEX.equals(indexType)) {
            return new GlobalFileNameCombiner();
        }

        if (DELETION_VECTORS_INDEX.equals(indexType) && BucketMode.BUCKET_UNAWARE == bucketMode) {
            return new GlobalCombiner();
        } else {
            return new BucketedCombiner();
        }
    }

    interface IndexManifestFileCombiner {
        List<IndexManifestEntry> combine(
                List<IndexManifestEntry> prevIndexFiles, List<IndexManifestEntry> newIndexFiles);
    }

    /**
     * We combine the previous and new index files by the file name. This is only used for tables
     * without bucket.
     */
    static class GlobalCombiner implements IndexManifestFileCombiner {

        @Override
        public List<IndexManifestEntry> combine(
                List<IndexManifestEntry> prevIndexFiles, List<IndexManifestEntry> newIndexFiles) {
            Map<String, IndexManifestEntry> indexEntries = new HashMap<>();
            Set<String> dvDataFiles = new HashSet<>();
            for (IndexManifestEntry entry : prevIndexFiles) {
                indexEntries.put(entry.indexFile().fileName(), entry);
                LinkedHashMap<String, DeletionVectorMeta> dvRanges = entry.indexFile().dvRanges();
                if (dvRanges != null) {
                    dvDataFiles.addAll(dvRanges.keySet());
                }
            }

            for (IndexManifestEntry entry : newIndexFiles) {
                String fileName = entry.indexFile().fileName();
                LinkedHashMap<String, DeletionVectorMeta> dvRanges = entry.indexFile().dvRanges();
                if (entry.kind() == FileKind.ADD) {
                    checkState(
                            !indexEntries.containsKey(fileName),
                            "Trying to add file %s which is already added.",
                            fileName);
                    if (dvRanges != null) {
                        for (String dataFile : dvRanges.keySet()) {
                            checkState(
                                    !dvDataFiles.contains(dataFile),
                                    "Trying to add dv for data file %s which is already added.",
                                    dataFile);
                            dvDataFiles.add(dataFile);
                        }
                    }
                    indexEntries.put(fileName, entry);
                } else {
                    checkState(
                            indexEntries.containsKey(fileName),
                            "Trying to delete file %s which is not exists.",
                            fileName);
                    if (dvRanges != null) {
                        for (String dataFile : dvRanges.keySet()) {
                            checkState(
                                    dvDataFiles.contains(dataFile),
                                    "Trying to delete dv for data file %s which is not exists.",
                                    dataFile);
                            dvDataFiles.remove(dataFile);
                        }
                    }
                    indexEntries.remove(fileName);
                }
            }
            return new ArrayList<>(indexEntries.values());
        }
    }

    /** We combine the previous and new index files by {@link BucketIdentifier}. */
    static class BucketedCombiner implements IndexManifestFileCombiner {

        @Override
        public List<IndexManifestEntry> combine(
                List<IndexManifestEntry> prevIndexFiles, List<IndexManifestEntry> newIndexFiles) {
            Map<BucketIdentifier, IndexManifestEntry> indexEntries = new HashMap<>();
            for (IndexManifestEntry entry : prevIndexFiles) {
                indexEntries.put(identifier(entry), entry);
            }

            // The deleted entry is processed first to avoid overwriting a new entry.
            List<IndexManifestEntry> removed =
                    newIndexFiles.stream()
                            .filter(f -> f.kind() == FileKind.DELETE)
                            .collect(Collectors.toList());
            List<IndexManifestEntry> added =
                    newIndexFiles.stream()
                            .filter(f -> f.kind() == FileKind.ADD)
                            .collect(Collectors.toList());
            for (IndexManifestEntry entry : removed) {
                indexEntries.remove(identifier(entry));
            }
            for (IndexManifestEntry entry : added) {
                indexEntries.put(identifier(entry), entry);
            }
            return new ArrayList<>(indexEntries.values());
        }
    }

    /** We combine the previous and new index files by file name. */
    static class GlobalFileNameCombiner implements IndexManifestFileCombiner {

        @Override
        public List<IndexManifestEntry> combine(
                List<IndexManifestEntry> prevIndexFiles, List<IndexManifestEntry> newIndexFiles) {
            Map<String, IndexManifestEntry> indexEntries = new HashMap<>();
            for (IndexManifestEntry entry : prevIndexFiles) {
                indexEntries.put(entry.indexFile().fileName(), entry);
            }

            // The deleted entry is processed first to avoid overwriting a new entry.
            List<IndexManifestEntry> removed =
                    newIndexFiles.stream()
                            .filter(f -> f.kind() == FileKind.DELETE)
                            .collect(Collectors.toList());
            List<IndexManifestEntry> added =
                    newIndexFiles.stream()
                            .filter(f -> f.kind() == FileKind.ADD)
                            .collect(Collectors.toList());
            for (IndexManifestEntry entry : removed) {
                indexEntries.remove(entry.indexFile().fileName());
            }
            for (IndexManifestEntry entry : added) {
                indexEntries.put(entry.indexFile().fileName(), entry);
            }
            return new ArrayList<>(indexEntries.values());
        }
    }

    private static BucketIdentifier identifier(IndexManifestEntry indexManifestEntry) {
        return new BucketIdentifier(
                indexManifestEntry.partition(),
                indexManifestEntry.bucket(),
                indexManifestEntry.indexFile().indexType());
    }

    /** The {@link BucketIdentifier} of a {@link IndexFileMeta}. */
    private static class BucketIdentifier {

        public final BinaryRow partition;
        public final int bucket;
        public final String indexType;

        private Integer hash;

        private BucketIdentifier(BinaryRow partition, int bucket, String indexType) {
            this.partition = partition;
            this.bucket = bucket;
            this.indexType = indexType;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            BucketIdentifier that = (BucketIdentifier) o;
            return bucket == that.bucket
                    && Objects.equals(partition, that.partition)
                    && Objects.equals(indexType, that.indexType);
        }

        @Override
        public int hashCode() {
            if (hash == null) {
                hash = Objects.hash(partition, bucket, indexType);
            }
            return hash;
        }

        @Override
        public String toString() {
            return "BucketIdentifier{"
                    + "partition="
                    + partition
                    + ", bucket="
                    + bucket
                    + ", indexType='"
                    + indexType
                    + '\''
                    + '}';
        }
    }
}
