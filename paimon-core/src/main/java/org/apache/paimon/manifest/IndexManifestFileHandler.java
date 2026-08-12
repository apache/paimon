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
import org.apache.paimon.utils.CloseableIterator;
import org.apache.paimon.utils.Range;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;

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
        Map<String, ChangePlan> plans = createChangePlans(newIndexFiles);
        observePreviousGlobalDeletionVectors(previousIndexManifest, plans);
        CloseableIterator<BinaryIndexManifestEntry> previous =
                previousIndexManifest == null
                        ? CloseableIterator.empty()
                        : indexManifestFile.scan(
                                previousIndexManifest, BinaryIndexManifestEntry.FULL_PROJECTION);
        return indexManifestFile.writeWithoutRolling(new MergedEntries(previous, plans));
    }

    private Map<String, ChangePlan> createChangePlans(List<IndexManifestEntry> newIndexFiles) {
        Map<String, List<IndexManifestEntry>> entriesByType = new LinkedHashMap<>();
        for (IndexManifestEntry entry : newIndexFiles) {
            entriesByType
                    .computeIfAbsent(entry.indexFile().indexType(), ignored -> new ArrayList<>())
                    .add(entry);
        }

        Map<String, ChangePlan> plans = new LinkedHashMap<>();
        for (Map.Entry<String, List<IndexManifestEntry>> entry : entriesByType.entrySet()) {
            String indexType = entry.getKey();
            ChangePlan plan;
            if (!DELETION_VECTORS_INDEX.equals(indexType) && !HASH_INDEX.equals(indexType)) {
                plan = new GlobalIndexChangePlan(entry.getValue());
            } else if (DELETION_VECTORS_INDEX.equals(indexType)
                    && BucketMode.BUCKET_UNAWARE == bucketMode) {
                plan = new GlobalDeletionVectorChangePlan(entry.getValue());
            } else {
                plan = new BucketedChangePlan(entry.getValue());
            }
            plans.put(indexType, plan);
        }
        return plans;
    }

    private void observePreviousGlobalDeletionVectors(
            @Nullable String previousIndexManifest, Map<String, ChangePlan> plans) {
        boolean needsObservation =
                plans.values().stream().anyMatch(ChangePlan::needsPreviousObservation);
        if (!needsObservation) {
            return;
        }

        if (previousIndexManifest != null) {
            try (CloseableIterator<BinaryIndexManifestEntry> previous =
                    indexManifestFile.scan(
                            previousIndexManifest, BinaryIndexManifestEntry.FULL_PROJECTION)) {
                while (previous.hasNext()) {
                    BinaryIndexManifestEntry binary = previous.next();
                    checkArgument(binary != null && binary.isAdd());
                    ChangePlan plan = plans.get(binary.indexType().toString());
                    if (plan != null && plan.needsPreviousObservation()) {
                        plan.observePrevious(binary.copy());
                    }
                }
            } catch (RuntimeException e) {
                throw e;
            } catch (Exception e) {
                throw new RuntimeException("Failed to scan previous index manifest.", e);
            }
        }

        for (ChangePlan plan : plans.values()) {
            if (plan.needsPreviousObservation()) {
                plan.finishObservation();
            }
        }
    }

    private abstract static class ChangePlan {

        boolean needsPreviousObservation() {
            return false;
        }

        void observePrevious(IndexManifestEntry entry) {}

        void finishObservation() {}

        abstract boolean retainPrevious(IndexManifestEntry entry);

        void finishPrevious() {}

        abstract List<IndexManifestEntry> additions();
    }

    private static class GlobalDeletionVectorChangePlan extends ChangePlan {

        private final List<IndexManifestEntry> changes;
        private final Set<String> touchedIndexFiles = new HashSet<>();
        private final Set<String> touchedDataFiles = new HashSet<>();
        private final Map<String, IndexManifestEntry> touchedPrevious = new HashMap<>();
        private final Set<String> existingDeletionVectors = new HashSet<>();
        private List<IndexManifestEntry> additions;

        private GlobalDeletionVectorChangePlan(List<IndexManifestEntry> changes) {
            this.changes = changes;
            for (IndexManifestEntry change : changes) {
                touchedIndexFiles.add(change.indexFile().fileName());
                LinkedHashMap<String, DeletionVectorMeta> ranges = change.indexFile().dvRanges();
                if (ranges != null) {
                    touchedDataFiles.addAll(ranges.keySet());
                }
            }
        }

        @Override
        boolean needsPreviousObservation() {
            return true;
        }

        @Override
        void observePrevious(IndexManifestEntry entry) {
            if (touchedIndexFiles.contains(entry.indexFile().fileName())) {
                touchedPrevious.put(entry.indexFile().fileName(), entry);
            }
            LinkedHashMap<String, DeletionVectorMeta> ranges = entry.indexFile().dvRanges();
            if (ranges != null) {
                for (String dataFile : ranges.keySet()) {
                    if (touchedDataFiles.contains(dataFile)) {
                        existingDeletionVectors.add(dataFile);
                    }
                }
            }
        }

        @Override
        void finishObservation() {
            Map<String, IndexManifestEntry> finalTouchedEntries =
                    new LinkedHashMap<>(touchedPrevious);
            for (IndexManifestEntry change : changes) {
                String fileName = change.indexFile().fileName();
                LinkedHashMap<String, DeletionVectorMeta> ranges = change.indexFile().dvRanges();
                if (change.kind() == FileKind.ADD) {
                    checkState(
                            !finalTouchedEntries.containsKey(fileName),
                            "Trying to add file %s which is already added.",
                            fileName);
                    if (ranges != null) {
                        for (String dataFile : ranges.keySet()) {
                            checkState(
                                    existingDeletionVectors.add(dataFile),
                                    "Trying to add dv for data file %s which is already added.",
                                    dataFile);
                        }
                    }
                    finalTouchedEntries.put(fileName, change);
                } else {
                    checkState(
                            finalTouchedEntries.containsKey(fileName),
                            "Trying to delete file %s which is not exists.",
                            fileName);
                    if (ranges != null) {
                        for (String dataFile : ranges.keySet()) {
                            checkState(
                                    existingDeletionVectors.remove(dataFile),
                                    "Trying to delete dv for data file %s which is not exists.",
                                    dataFile);
                        }
                    }
                    finalTouchedEntries.remove(fileName);
                }
            }
            additions = new ArrayList<>(finalTouchedEntries.values());
        }

        @Override
        boolean retainPrevious(IndexManifestEntry entry) {
            return !touchedIndexFiles.contains(entry.indexFile().fileName());
        }

        @Override
        List<IndexManifestEntry> additions() {
            return additions;
        }
    }

    private static class BucketedChangePlan extends ChangePlan {

        private final Set<BucketIdentifier> changed = new HashSet<>();
        private final Map<BucketIdentifier, IndexManifestEntry> added = new LinkedHashMap<>();

        private BucketedChangePlan(List<IndexManifestEntry> changes) {
            for (IndexManifestEntry change : changes) {
                changed.add(identifier(change));
            }
            for (IndexManifestEntry change : changes) {
                if (change.kind() == FileKind.ADD) {
                    added.put(identifier(change), change);
                }
            }
        }

        @Override
        boolean retainPrevious(IndexManifestEntry entry) {
            return !changed.contains(identifier(entry));
        }

        @Override
        List<IndexManifestEntry> additions() {
            return new ArrayList<>(added.values());
        }
    }

    private static class GlobalIndexChangePlan extends ChangePlan {

        private final List<String> deleted = new ArrayList<>();
        private final Set<String> deletedNames = new HashSet<>();
        private final Set<String> changed = new HashSet<>();
        private final Set<String> existingDeleted = new HashSet<>();
        private final List<IndexManifestEntry> added = new ArrayList<>();
        private final Map<String, IndexManifestEntry> finalAdded = new LinkedHashMap<>();

        private GlobalIndexChangePlan(List<IndexManifestEntry> changes) {
            for (IndexManifestEntry change : changes) {
                String fileName = change.indexFile().fileName();
                changed.add(fileName);
                if (change.kind() == FileKind.DELETE) {
                    deleted.add(fileName);
                    deletedNames.add(fileName);
                } else {
                    added.add(change);
                    finalAdded.put(fileName, change);
                }
            }
        }

        @Override
        boolean needsPreviousObservation() {
            return true;
        }

        @Override
        void observePrevious(IndexManifestEntry entry) {
            String fileName = entry.indexFile().fileName();
            if (deletedNames.contains(fileName)) {
                existingDeleted.add(fileName);
            } else {
                validateRetainedIndexFile(entry, added);
            }
        }

        @Override
        void finishObservation() {
            Set<String> remaining = new HashSet<>(existingDeleted);
            for (String fileName : deleted) {
                checkState(
                        remaining.remove(fileName),
                        "Trying to delete global index file %s which does not exist.",
                        fileName);
            }
        }

        @Override
        boolean retainPrevious(IndexManifestEntry entry) {
            return !changed.contains(entry.indexFile().fileName());
        }

        @Override
        List<IndexManifestEntry> additions() {
            return new ArrayList<>(finalAdded.values());
        }

        private static void validateRetainedIndexFile(
                IndexManifestEntry retained, List<IndexManifestEntry> addedIndexFiles) {
            GlobalIndexMeta retainedMeta = retained.indexFile().globalIndexMeta();
            if (retainedMeta == null) {
                return;
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
                        || (Arrays.equals(retainedMeta.extraFieldIds(), addedMeta.extraFieldIds())
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

    private static class MergedEntries implements CloseableIterator<IndexManifestEntry> {

        private final CloseableIterator<BinaryIndexManifestEntry> previous;
        private final Map<String, ChangePlan> plans;
        private final Iterator<ChangePlan> planIterator;
        private Iterator<IndexManifestEntry> additions;
        private IndexManifestEntry next;
        private boolean previousFinished;
        private boolean previousClosed;

        private MergedEntries(
                CloseableIterator<BinaryIndexManifestEntry> previous,
                Map<String, ChangePlan> plans) {
            this.previous = previous;
            this.plans = plans;
            this.planIterator = plans.values().iterator();
        }

        @Override
        public boolean hasNext() {
            while (next == null && !previousFinished && previous.hasNext()) {
                BinaryIndexManifestEntry binary = previous.next();
                checkArgument(binary != null && binary.isAdd());
                IndexManifestEntry entry = binary.copy();
                ChangePlan plan = plans.get(entry.indexFile().indexType());
                if (plan == null || plan.retainPrevious(entry)) {
                    next = entry;
                }
            }
            if (next != null) {
                return true;
            }
            if (!previousFinished) {
                previousFinished = true;
                closePrevious();
                for (ChangePlan plan : plans.values()) {
                    plan.finishPrevious();
                }
            }
            while (next == null) {
                if (additions != null && additions.hasNext()) {
                    next = additions.next();
                    break;
                }
                if (!planIterator.hasNext()) {
                    break;
                }
                additions = planIterator.next().additions().iterator();
            }
            return next != null;
        }

        @Override
        public IndexManifestEntry next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            IndexManifestEntry result = next;
            next = null;
            return result;
        }

        @Override
        public void close() {
            closePrevious();
        }

        private void closePrevious() {
            if (previousClosed) {
                return;
            }
            previousClosed = true;
            try {
                previous.close();
            } catch (Exception e) {
                throw new RuntimeException("Failed to close previous index manifest.", e);
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
