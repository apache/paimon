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
import org.apache.paimon.index.DataEvolutionIndexSourceMeta;
import org.apache.paimon.index.DeletionVectorMeta;
import org.apache.paimon.index.GlobalIndexMeta;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.utils.Range;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
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
        if (previousIndexManifest == null) {
            return writeNewManifest(newIndexFiles);
        }

        // A cache hit avoids remote I/O and is preferable to re-reading compressed Avro blocks.
        if (indexManifestFile.isCached(previousIndexManifest)) {
            return writeLegacy(previousIndexManifest, newIndexFiles);
        }

        IndexManifestAvroReader reader = indexManifestFile.scanAvroBlocks(previousIndexManifest);
        try {
            if (reader.rawBlockCopySupported()) {
                return rewriteCurrentSchema(reader, newIndexFiles);
            }
            reader.close();
        } catch (IOException e) {
            throw new UncheckedIOException(
                    "Failed to rewrite index manifest " + previousIndexManifest, e);
        }

        // Encoded records from an evolved writer schema cannot be appended to the current schema.
        // Materialize them through the regular Avro resolver instead.
        return writeLegacy(previousIndexManifest, newIndexFiles);
    }

    private String writeNewManifest(List<IndexManifestEntry> changes) {
        RewritePlan plan = new RewritePlan(changes, bucketMode);
        IndexManifestAvroWriter writer = indexManifestFile.createAvroWriter();
        try {
            for (IndexManifestEntry entry : plan.finish()) {
                writer.write(entry);
            }
            writer.close();
            return writer.result();
        } catch (IOException failure) {
            writer.abort(failure);
            throw new UncheckedIOException(failure);
        } catch (RuntimeException | Error failure) {
            writer.abort(failure);
            throw failure;
        }
    }

    private String rewriteCurrentSchema(
            IndexManifestAvroReader reader, List<IndexManifestEntry> newIndexFiles)
            throws IOException {
        RewritePlan plan = new RewritePlan(newIndexFiles, bucketMode);
        IndexManifestEntrySerializer serializer = new IndexManifestEntrySerializer();
        IndexManifestAvroWriter writer;
        try {
            writer = indexManifestFile.createAvroWriter();
        } catch (RuntimeException | Error failure) {
            try {
                reader.close();
            } catch (Throwable cleanupFailure) {
                failure.addSuppressed(cleanupFailure);
            }
            throw failure;
        }
        try {
            // Close the source before committing the output so a source close failure can still
            // abort and delete the incomplete replacement.
            try (IndexManifestAvroReader ignored = reader) {
                while (reader.hasNext()) {
                    IndexManifestAvroReader.RawBlock block = reader.next();
                    IndexManifestAvroReader.RowIterator rows =
                            block.toRows(IndexManifestEntry.MANIFEST_ROW_TYPE);
                    List<ByteBuffer> retainedRecords = new ArrayList<>();
                    while (rows.hasNext()) {
                        IndexManifestEntry entry = serializer.fromRow(rows.next());
                        checkArgument(entry.kind() == FileKind.ADD);
                        if (plan.collectPrevious(entry)) {
                            retainedRecords.add(rows.encodedRecord().duplicate());
                        }
                    }

                    if (retainedRecords.size() == block.recordCount()) {
                        writer.writeEncodedBlock(block.encodedBlock());
                    } else {
                        for (ByteBuffer retainedRecord : retainedRecords) {
                            writer.writeEncoded(retainedRecord);
                        }
                    }
                }
            }

            for (IndexManifestEntry added : plan.finish()) {
                writer.write(added);
            }
            writer.close();
            return writer.result();
        } catch (IOException | RuntimeException | Error failure) {
            writer.abort(failure);
            throw failure;
        }
    }

    private String writeLegacy(
            String previousIndexManifest, List<IndexManifestEntry> newIndexFiles) {
        List<IndexManifestEntry> entries = indexManifestFile.read(previousIndexManifest);
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

    private static class RewritePlan {

        private final Map<String, IndexTypeRewrite> rewrites = new HashMap<>();

        private RewritePlan(List<IndexManifestEntry> changes, BucketMode bucketMode) {
            Map<String, List<IndexManifestEntry>> changesByType = new HashMap<>();
            for (IndexManifestEntry change : changes) {
                changesByType
                        .computeIfAbsent(
                                change.indexFile().indexType(), ignored -> new ArrayList<>())
                        .add(change);
            }
            for (Map.Entry<String, List<IndexManifestEntry>> entry : changesByType.entrySet()) {
                String indexType = entry.getKey();
                List<IndexManifestEntry> typeChanges = entry.getValue();
                IndexTypeRewrite rewrite;
                if (!DELETION_VECTORS_INDEX.equals(indexType) && !HASH_INDEX.equals(indexType)) {
                    rewrite = new GlobalIndexRewrite(typeChanges);
                } else if (DELETION_VECTORS_INDEX.equals(indexType)
                        && BucketMode.BUCKET_UNAWARE == bucketMode) {
                    rewrite = new GlobalRewrite(typeChanges);
                } else {
                    rewrite = new BucketedRewrite(typeChanges);
                }
                rewrites.put(indexType, rewrite);
            }
        }

        private boolean collectPrevious(IndexManifestEntry entry) {
            IndexTypeRewrite rewrite = rewrites.get(entry.indexFile().indexType());
            return rewrite == null || rewrite.collectPrevious(entry);
        }

        private List<IndexManifestEntry> finish() {
            List<IndexManifestEntry> additions = new ArrayList<>();
            for (IndexTypeRewrite rewrite : rewrites.values()) {
                rewrite.finish();
                additions.addAll(rewrite.additions());
            }
            return additions;
        }
    }

    private interface IndexTypeRewrite {

        boolean collectPrevious(IndexManifestEntry entry);

        void finish();

        Collection<IndexManifestEntry> additions();
    }

    private static class GlobalRewrite implements IndexTypeRewrite {

        private final List<IndexManifestEntry> changes;
        private final Set<String> changedFiles = new HashSet<>();
        private final Set<String> previousFiles = new HashSet<>();
        private final Set<String> deletionVectorDataFiles = new HashSet<>();
        private final Map<String, IndexManifestEntry> addedFiles = new HashMap<>();

        private GlobalRewrite(List<IndexManifestEntry> changes) {
            this.changes = changes;
            for (IndexManifestEntry change : changes) {
                changedFiles.add(change.indexFile().fileName());
            }
        }

        @Override
        public boolean collectPrevious(IndexManifestEntry entry) {
            previousFiles.add(entry.indexFile().fileName());
            LinkedHashMap<String, DeletionVectorMeta> dvRanges = entry.indexFile().dvRanges();
            if (dvRanges != null) {
                deletionVectorDataFiles.addAll(dvRanges.keySet());
            }
            return !changedFiles.contains(entry.indexFile().fileName());
        }

        @Override
        public void finish() {
            Set<String> currentFiles = new HashSet<>(previousFiles);
            for (IndexManifestEntry change : changes) {
                String fileName = change.indexFile().fileName();
                LinkedHashMap<String, DeletionVectorMeta> dvRanges = change.indexFile().dvRanges();
                if (change.kind() == FileKind.ADD) {
                    checkState(
                            !currentFiles.contains(fileName),
                            "Trying to add file %s which is already added.",
                            fileName);
                    if (dvRanges != null) {
                        for (String dataFile : dvRanges.keySet()) {
                            checkState(
                                    !deletionVectorDataFiles.contains(dataFile),
                                    "Trying to add dv for data file %s which is already added.",
                                    dataFile);
                            deletionVectorDataFiles.add(dataFile);
                        }
                    }
                    currentFiles.add(fileName);
                    addedFiles.put(fileName, change);
                } else {
                    checkState(
                            currentFiles.contains(fileName),
                            "Trying to delete file %s which is not exists.",
                            fileName);
                    if (dvRanges != null) {
                        for (String dataFile : dvRanges.keySet()) {
                            checkState(
                                    deletionVectorDataFiles.contains(dataFile),
                                    "Trying to delete dv for data file %s which is not exists.",
                                    dataFile);
                            deletionVectorDataFiles.remove(dataFile);
                        }
                    }
                    currentFiles.remove(fileName);
                    addedFiles.remove(fileName);
                }
            }
        }

        @Override
        public Collection<IndexManifestEntry> additions() {
            return addedFiles.values();
        }
    }

    private static class BucketedRewrite implements IndexTypeRewrite {

        private final Set<BucketIdentifier> removed = new HashSet<>();
        private final Map<BucketIdentifier, IndexManifestEntry> added = new HashMap<>();

        private BucketedRewrite(List<IndexManifestEntry> changes) {
            for (IndexManifestEntry change : changes) {
                if (change.kind() == FileKind.DELETE) {
                    removed.add(identifier(change));
                }
            }
            for (IndexManifestEntry change : changes) {
                if (change.kind() == FileKind.ADD) {
                    added.put(identifier(change), change);
                }
            }
        }

        @Override
        public boolean collectPrevious(IndexManifestEntry entry) {
            BucketIdentifier identifier = identifier(entry);
            return !removed.contains(identifier) && !added.containsKey(identifier);
        }

        @Override
        public void finish() {}

        @Override
        public Collection<IndexManifestEntry> additions() {
            return added.values();
        }
    }

    private static class GlobalIndexRewrite implements IndexTypeRewrite {

        private final List<String> removed = new ArrayList<>();
        private final Set<String> removedSet = new HashSet<>();
        private final List<IndexManifestEntry> added = new ArrayList<>();
        private final Map<String, IndexManifestEntry> finalAdded = new HashMap<>();
        private final Set<String> previousFiles = new HashSet<>();

        private @Nullable IllegalStateException overlapFailure;

        private GlobalIndexRewrite(List<IndexManifestEntry> changes) {
            for (IndexManifestEntry change : changes) {
                String fileName = change.indexFile().fileName();
                if (change.kind() == FileKind.DELETE) {
                    removed.add(fileName);
                    removedSet.add(fileName);
                } else {
                    added.add(change);
                    finalAdded.put(fileName, change);
                }
            }
        }

        @Override
        public boolean collectPrevious(IndexManifestEntry entry) {
            String fileName = entry.indexFile().fileName();
            previousFiles.add(fileName);
            if (!removedSet.contains(fileName) && overlapFailure == null) {
                for (IndexManifestEntry addedEntry : added) {
                    try {
                        validateRetainedIndexFile(entry, addedEntry);
                    } catch (IllegalStateException e) {
                        overlapFailure = e;
                        break;
                    }
                }
            }
            return !removedSet.contains(fileName) && !finalAdded.containsKey(fileName);
        }

        @Override
        public void finish() {
            Set<String> currentFiles = new HashSet<>(previousFiles);
            for (String fileName : removed) {
                checkState(
                        currentFiles.remove(fileName),
                        "Trying to delete global index file %s which does not exist.",
                        fileName);
            }
            if (overlapFailure != null) {
                throw overlapFailure;
            }
        }

        @Override
        public Collection<IndexManifestEntry> additions() {
            return finalAdded.values();
        }
    }

    private static void validateRetainedIndexFile(
            IndexManifestEntry retained, IndexManifestEntry added) {
        GlobalIndexMeta retainedMeta = retained.indexFile().globalIndexMeta();
        if (retainedMeta == null) {
            return;
        }
        GlobalIndexMeta addedMeta = added.indexFile().globalIndexMeta();
        if (addedMeta == null
                || (retainedMeta.sourceMeta() != null
                        && addedMeta.sourceMeta() != null
                        && !DataEvolutionIndexSourceMeta.isDataEvolutionMeta(
                                retainedMeta.sourceMeta())
                        && !DataEvolutionIndexSourceMeta.isDataEvolutionMeta(
                                addedMeta.sourceMeta()))
                || retainedMeta.indexFieldId() != addedMeta.indexFieldId()
                || (Arrays.equals(retainedMeta.extraFieldIds(), addedMeta.extraFieldIds())
                        && !Range.intersect(
                                retainedMeta.rowRangeStart(),
                                retainedMeta.rowRangeEnd(),
                                addedMeta.rowRangeStart(),
                                addedMeta.rowRangeEnd()))) {
            return;
        }

        throw new IllegalStateException(
                String.format(
                        "Trying to add global index file %s of type %s for index field %s"
                                + " with row range [%s, %s], but previous file %s still exists"
                                + " with overlapping row range [%s, %s]. Remove the previous file first.",
                        added.indexFile().fileName(),
                        added.indexFile().indexType(),
                        addedMeta.indexFieldId(),
                        addedMeta.rowRangeStart(),
                        addedMeta.rowRangeEnd(),
                        retained.indexFile().fileName(),
                        retainedMeta.rowRangeStart(),
                        retainedMeta.rowRangeEnd()));
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
            return new GlobalIndexCombiner();
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
    static class GlobalIndexCombiner implements IndexManifestFileCombiner {

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
                String fileName = entry.indexFile().fileName();
                checkState(
                        indexEntries.containsKey(fileName),
                        "Trying to delete global index file %s which does not exist.",
                        fileName);
                indexEntries.remove(fileName);
            }
            validateRetainedIndexFiles(indexEntries.values(), added);
            for (IndexManifestEntry entry : added) {
                indexEntries.put(entry.indexFile().fileName(), entry);
            }
            return new ArrayList<>(indexEntries.values());
        }

        private void validateRetainedIndexFiles(
                Iterable<IndexManifestEntry> retainedIndexFiles,
                List<IndexManifestEntry> addedIndexFiles) {
            for (IndexManifestEntry retained : retainedIndexFiles) {
                GlobalIndexMeta retainedMeta = retained.indexFile().globalIndexMeta();
                if (retainedMeta == null) {
                    continue;
                }

                for (IndexManifestEntry added : addedIndexFiles) {
                    GlobalIndexMeta addedMeta = added.indexFile().globalIndexMeta();
                    if (addedMeta == null
                            || (retainedMeta.sourceMeta() != null
                                    && addedMeta.sourceMeta() != null
                                    && !DataEvolutionIndexSourceMeta.isDataEvolutionMeta(
                                            retainedMeta.sourceMeta())
                                    && !DataEvolutionIndexSourceMeta.isDataEvolutionMeta(
                                            addedMeta.sourceMeta()))
                            || retainedMeta.indexFieldId() != addedMeta.indexFieldId()
                            || (Arrays.equals(
                                            retainedMeta.extraFieldIds(), addedMeta.extraFieldIds())
                                    && !Range.intersect(
                                            retainedMeta.rowRangeStart(),
                                            retainedMeta.rowRangeEnd(),
                                            addedMeta.rowRangeStart(),
                                            addedMeta.rowRangeEnd()))) {
                        continue;
                    }

                    throw new IllegalStateException(
                            String.format(
                                    "Trying to add global index file %s of type %s for index field %s"
                                            + " with row range [%s, %s], but previous file %s still exists"
                                            + " with overlapping row range [%s, %s]. Remove the previous file first.",
                                    added.indexFile().fileName(),
                                    added.indexFile().indexType(),
                                    addedMeta.indexFieldId(),
                                    addedMeta.rowRangeStart(),
                                    addedMeta.rowRangeEnd(),
                                    retained.indexFile().fileName(),
                                    retainedMeta.rowRangeStart(),
                                    retainedMeta.rowRangeEnd()));
                }
            }
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
