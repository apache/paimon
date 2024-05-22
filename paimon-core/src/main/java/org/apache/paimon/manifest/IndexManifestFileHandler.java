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

import org.apache.paimon.table.BucketMode;
import org.apache.paimon.utils.Pair;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.paimon.deletionvectors.DeletionVectorsIndexFile.DELETION_VECTORS_INDEX;
import static org.apache.paimon.index.HashIndexFile.HASH_INDEX;

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
        Pair<List<IndexManifestEntry>, List<IndexManifestEntry>> previous =
                separateIndexEntries(entries);
        Pair<List<IndexManifestEntry>, List<IndexManifestEntry>> current =
                separateIndexEntries(newIndexFiles);

        // Step1: get the hash index files;
        List<IndexManifestEntry> indexEntries =
                getIndexManifestFileCombine(HASH_INDEX)
                        .combine(previous.getLeft(), current.getLeft());

        // Step2: get the dv index files;
        indexEntries.addAll(
                getIndexManifestFileCombine(DELETION_VECTORS_INDEX)
                        .combine(previous.getRight(), current.getRight()));

        return indexManifestFile.writeWithoutRolling(indexEntries);
    }

    private Pair<List<IndexManifestEntry>, List<IndexManifestEntry>> separateIndexEntries(
            List<IndexManifestEntry> indexFiles) {
        List<IndexManifestEntry> hashEntries = new ArrayList<>();
        List<IndexManifestEntry> dvEntries = new ArrayList<>();
        for (IndexManifestEntry entry : indexFiles) {
            String indexType = entry.indexFile().indexType();
            if (indexType.equals(DELETION_VECTORS_INDEX)) {
                dvEntries.add(entry);
            } else if (indexType.equals(HASH_INDEX)) {
                hashEntries.add(entry);
            } else {
                throw new IllegalArgumentException("Can't recognize this index type: " + indexType);
            }
        }
        return Pair.of(hashEntries, dvEntries);
    }

    private IndexManifestFileCombiner getIndexManifestFileCombine(String indexType) {
        if (DELETION_VECTORS_INDEX.equals(indexType) && BucketMode.BUCKET_UNAWARE == bucketMode) {
            return new UnawareBucketCombiner();
        } else {
            return new CommonBucketCombiner();
        }
    }

    interface IndexManifestFileCombiner {
        List<IndexManifestEntry> combine(
                List<IndexManifestEntry> prevIndexFiles, List<IndexManifestEntry> newIndexFiles);
    }

    /**
     * We combine the previous and new index files by the file name. This is only used for tables
     * with UnawareBucket.
     */
    static class UnawareBucketCombiner implements IndexManifestFileCombiner {

        @Override
        public List<IndexManifestEntry> combine(
                List<IndexManifestEntry> prevIndexFiles, List<IndexManifestEntry> newIndexFiles) {
            Map<String, IndexManifestEntry> indexEntries = new LinkedHashMap<>();
            for (IndexManifestEntry entry : prevIndexFiles) {
                if (entry.kind() == FileKind.ADD) {
                    indexEntries.put(entry.indexFile().fileName(), entry);
                }
            }

            for (IndexManifestEntry entry : newIndexFiles) {
                if (entry.kind() == FileKind.ADD) {
                    indexEntries.put(entry.indexFile().fileName(), entry);
                } else {
                    indexEntries.remove(entry.indexFile().fileName());
                }
            }
            return new ArrayList<>(indexEntries.values());
        }
    }

    /** We combine the previous and new index files by {@link IndexManifestEntry#identifier} */
    static class CommonBucketCombiner implements IndexManifestFileCombiner {

        @Override
        public List<IndexManifestEntry> combine(
                List<IndexManifestEntry> prevIndexFiles, List<IndexManifestEntry> newIndexFiles) {
            Map<IndexManifestEntry.Identifier, IndexManifestEntry> indexEntries =
                    prevIndexFiles.stream()
                            .filter(entry -> entry.kind() == FileKind.ADD)
                            .collect(
                                    Collectors.toMap(
                                            IndexManifestEntry::identifier, Function.identity()));
            for (IndexManifestEntry entry : newIndexFiles) {
                if (entry.kind() == FileKind.ADD) {
                    indexEntries.put(entry.identifier(), entry);
                } else {
                    indexEntries.remove(entry.identifier());
                }
            }
            return new ArrayList<>(indexEntries.values());
        }
    }
}
