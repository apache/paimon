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

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.index.IndexFileHandler;
import org.apache.paimon.index.IndexFileMeta;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/** Maintainer of deletionVectors index. */
public class DeletionVectorsMaintainer {

    private final IndexFileHandler indexFileHandler;
    private final IndexFileMeta indexFile;
    private final Map<String, DeletionVector> deletionVectors;
    private boolean modified;
    private boolean restored;
    private final Set<String> restoredFileNames;

    private DeletionVectorsMaintainer(
            IndexFileHandler fileHandler,
            @Nullable Long snapshotId,
            BinaryRow partition,
            int bucket) {
        this.indexFileHandler = fileHandler;
        this.indexFile =
                snapshotId == null
                        ? null
                        : fileHandler
                                .scan(
                                        snapshotId,
                                        DeletionVectorsIndexFile.DELETION_VECTORS_INDEX,
                                        partition,
                                        bucket)
                                .orElse(null);
        this.deletionVectors = new HashMap<>();
        this.modified = false;
        this.restored = false;
        this.restoredFileNames = new HashSet<>();
    }

    // -------------------------------------------------------------------------
    //  For writer
    // -------------------------------------------------------------------------

    /**
     * Notifies a new deletion which marks the specified row position as deleted with the given file
     * name.
     *
     * @param fileName The name of the file where the deletion occurred.
     * @param position The row position within the file that has been deleted.
     */
    public void notifyNewDeletion(String fileName, long position) {
        restoreAllDeletionVector();
        DeletionVector deletionVector =
                deletionVectors.computeIfAbsent(fileName, k -> new BitmapDeletionVector());
        if (!deletionVector.isDeleted(position)) {
            deletionVector.delete(position);
            modified = true;
        }
    }

    /**
     * Removes the specified file's deletion vector, this method is typically used for remove before
     * files' deletion vector in compaction.
     *
     * @param fileName The name of the file whose deletion vector should be removed.
     */
    public void removeDeletionVectorOf(String fileName) {
        restoreAllDeletionVector();
        if (deletionVectors.containsKey(fileName)) {
            deletionVectors.remove(fileName);
            modified = true;
        }
    }

    /**
     * Prepares to commit: write new deletion vectors index file if any modifications have been
     * made.
     *
     * @return A list containing the metadata of the deletion vectors index file, or an empty list
     *     if no changes need to be committed.
     */
    public List<IndexFileMeta> prepareCommit() {
        if (modified) {
            IndexFileMeta entry = indexFileHandler.writeDeletionVectorsIndex(deletionVectors);
            modified = false;
            return Collections.singletonList(entry);
        }
        return Collections.emptyList();
    }

    // -------------------------------------------------------------------------
    //  For reader
    // -------------------------------------------------------------------------

    /**
     * Retrieves the deletion vector associated with the specified file name.
     *
     * @param fileName The name of the file for which the deletion vector is requested.
     * @return An {@code Optional} containing the deletion vector if it exists, or an empty {@code
     *     Optional} if not.
     */
    public Optional<DeletionVector> deletionVectorOf(String fileName) {
        restoreDeletionVector(fileName);
        return Optional.ofNullable(deletionVectors.get(fileName));
    }

    // -------------------------------------------------------------------------
    //  Internal methods
    // -------------------------------------------------------------------------

    // Restore all deletionVectors
    private void restoreAllDeletionVector() {
        if (indexFile != null && !restored) {
            deletionVectors.putAll(indexFileHandler.readAllDeletionVectors(indexFile));
            restored = true;
        }
    }

    // Restore the specified deletionVector
    private void restoreDeletionVector(String fileName) {
        if (indexFile != null
                && !restored
                && !deletionVectors.containsKey(fileName)
                && !restoredFileNames.contains(fileName)) {
            restoredFileNames.add(fileName);
            indexFileHandler
                    .readDeletionVector(indexFile, fileName)
                    .ifPresent(deletionVector -> deletionVectors.put(fileName, deletionVector));
        }
    }

    /** Factory to restore {@link DeletionVectorsMaintainer}. */
    public static class DeletionVectorsMaintainerFactory {

        private final IndexFileHandler handler;

        public DeletionVectorsMaintainerFactory(IndexFileHandler handler) {
            this.handler = handler;
        }

        public DeletionVectorsMaintainer createOrRestore(
                @Nullable Long snapshotId, BinaryRow partition, int bucket) {
            return new DeletionVectorsMaintainer(handler, snapshotId, partition, bucket);
        }
    }
}
