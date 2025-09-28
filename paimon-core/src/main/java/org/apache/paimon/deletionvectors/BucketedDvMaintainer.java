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

package org.apache.paimon.deletionvectors;

import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.index.IndexFileHandler;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.utils.Pair;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** Maintainer of deletionVectors index. */
public class BucketedDvMaintainer {

    private final DeletionVectorsIndexFile dvIndexFile;
    private final Map<String, DeletionVector> deletionVectors;
    protected final boolean bitmap64;
    private boolean modified;
    @Nullable private IndexFileMeta beforeDvFile;

    private BucketedDvMaintainer(
            DeletionVectorsIndexFile dvIndexFile,
            Map<String, DeletionVector> deletionVectors,
            @Nullable IndexFileMeta restoredDvFile) {
        this.dvIndexFile = dvIndexFile;
        this.deletionVectors = deletionVectors;
        this.bitmap64 = dvIndexFile.bitmap64();
        this.modified = false;
        this.beforeDvFile = restoredDvFile;
    }

    private DeletionVector createNewDeletionVector() {
        return bitmap64 ? new Bitmap64DeletionVector() : new BitmapDeletionVector();
    }

    /**
     * Notifies a new deletion which marks the specified row position as deleted with the given file
     * name.
     *
     * @param fileName The name of the file where the deletion occurred.
     * @param position The row position within the file that has been deleted.
     */
    public void notifyNewDeletion(String fileName, long position) {
        DeletionVector deletionVector =
                deletionVectors.computeIfAbsent(fileName, k -> createNewDeletionVector());
        if (deletionVector.checkedDelete(position)) {
            modified = true;
        }
    }

    /**
     * Notifies a new deletion which marks the specified deletion vector with the given file name.
     *
     * @param fileName The name of the file where the deletion occurred.
     * @param deletionVector The deletion vector
     */
    public void notifyNewDeletion(String fileName, DeletionVector deletionVector) {
        deletionVectors.put(fileName, deletionVector);
        modified = true;
    }

    /**
     * Merge a new deletion which marks the specified deletion vector with the given file name, if
     * the previous deletion vector exist, merge the old one.
     *
     * @param fileName The name of the file where the deletion occurred.
     * @param deletionVector The deletion vector
     */
    public void mergeNewDeletion(String fileName, DeletionVector deletionVector) {
        DeletionVector old = deletionVectors.get(fileName);
        if (old != null) {
            deletionVector.merge(old);
        }
        deletionVectors.put(fileName, deletionVector);
        modified = true;
    }

    /**
     * Removes the specified file's deletion vector, this method is typically used for remove before
     * files' deletion vector in compaction.
     *
     * @param fileName The name of the file whose deletion vector should be removed.
     */
    public void removeDeletionVectorOf(String fileName) {
        if (deletionVectors.containsKey(fileName)) {
            deletionVectors.remove(fileName);
            modified = true;
        }
    }

    /**
     * Write before and new deletion vectors index file pair if any modifications have been made.
     */
    public Pair<IndexFileMeta, IndexFileMeta> writeDeletionVectorsIndex() {
        if (modified) {
            modified = false;
            IndexFileMeta newIndexFile = dvIndexFile.writeSingleFile(deletionVectors);
            IndexFileMeta toRemove = beforeDvFile;
            beforeDvFile = newIndexFile;
            return Pair.of(toRemove, newIndexFile);
        } else {
            return Pair.of(null, null);
        }
    }

    /**
     * Retrieves the deletion vector associated with the specified file name.
     *
     * @param fileName The name of the file for which the deletion vector is requested.
     * @return An {@code Optional} containing the deletion vector if it exists, or an empty {@code
     *     Optional} if not.
     */
    public Optional<DeletionVector> deletionVectorOf(String fileName) {
        return Optional.ofNullable(deletionVectors.get(fileName));
    }

    public DeletionVectorsIndexFile dvIndexFile() {
        return dvIndexFile;
    }

    @VisibleForTesting
    public Map<String, DeletionVector> deletionVectors() {
        return deletionVectors;
    }

    public boolean bitmap64() {
        return bitmap64;
    }

    public static Factory factory(IndexFileHandler handler) {
        return new Factory(handler);
    }

    /** Factory to restore {@link BucketedDvMaintainer}. */
    public static class Factory {

        private final IndexFileHandler handler;

        private Factory(IndexFileHandler handler) {
            this.handler = handler;
        }

        public IndexFileHandler indexFileHandler() {
            return handler;
        }

        public BucketedDvMaintainer create(
                BinaryRow partition, int bucket, @Nullable List<IndexFileMeta> restoredFiles) {
            if (restoredFiles == null) {
                restoredFiles = Collections.emptyList();
            }
            Map<String, DeletionVector> deletionVectors =
                    new HashMap<>(handler.readAllDeletionVectors(partition, bucket, restoredFiles));
            if (restoredFiles.size() > 1) {
                throw new UnsupportedOperationException(
                        "For bucket table, one bucket should only have one dv file at most.");
            }
            IndexFileMeta fileMeta = restoredFiles.isEmpty() ? null : restoredFiles.get(0);
            return new BucketedDvMaintainer(
                    handler.dvIndex(partition, bucket), deletionVectors, fileMeta);
        }
    }
}
