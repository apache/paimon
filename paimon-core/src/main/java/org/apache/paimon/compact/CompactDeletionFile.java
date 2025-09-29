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

package org.apache.paimon.compact;

import org.apache.paimon.deletionvectors.BucketedDvMaintainer;
import org.apache.paimon.deletionvectors.DeletionVectorsIndexFile;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.utils.Pair;

import javax.annotation.Nullable;

/** Deletion File from compaction. */
public interface CompactDeletionFile {

    /**
     * Get or compute the deletion file.
     *
     * @return Pair of deleted deletion file and new deletion file.
     */
    Pair<IndexFileMeta, IndexFileMeta> getOrCompute();

    CompactDeletionFile mergeOldFile(CompactDeletionFile old);

    void clean();

    /**
     * Used by async compaction, when compaction task is completed, deletions file will be generated
     * immediately, so when updateCompactResult, we need to merge old deletion files (just delete
     * them).
     */
    static CompactDeletionFile generateFiles(BucketedDvMaintainer maintainer) {
        Pair<IndexFileMeta, IndexFileMeta> pair = maintainer.writeDeletionVectorsIndex();
        return new GeneratedDeletionFile(pair.getLeft(), pair.getRight(), maintainer.dvIndexFile());
    }

    /** For sync compaction, only create deletion files when prepareCommit. */
    static CompactDeletionFile lazyGeneration(BucketedDvMaintainer maintainer) {
        return new LazyCompactDeletionFile(maintainer);
    }

    /** A generated files implementation of {@link CompactDeletionFile}. */
    class GeneratedDeletionFile implements CompactDeletionFile {

        @Nullable private IndexFileMeta deleteDeletionFile;
        @Nullable private final IndexFileMeta newDeletionFile;
        private final DeletionVectorsIndexFile dvIndexFile;

        private boolean getInvoked = false;

        public GeneratedDeletionFile(
                @Nullable IndexFileMeta deletedDeletionFile,
                @Nullable IndexFileMeta newDeletionFile,
                DeletionVectorsIndexFile dvIndexFile) {
            this.deleteDeletionFile = deletedDeletionFile;
            this.newDeletionFile = newDeletionFile;
            this.dvIndexFile = dvIndexFile;
        }

        @Override
        public Pair<IndexFileMeta, IndexFileMeta> getOrCompute() {
            this.getInvoked = true;
            return Pair.of(deleteDeletionFile, newDeletionFile);
        }

        @Override
        public CompactDeletionFile mergeOldFile(CompactDeletionFile old) {
            if (!(old instanceof GeneratedDeletionFile)) {
                throw new IllegalStateException(
                        "old should be a GeneratedDeletionFile, but it is: " + old.getClass());
            }

            if (((GeneratedDeletionFile) old).getInvoked) {
                throw new IllegalStateException("old should not be get, this is a bug.");
            }

            if (newDeletionFile == null) {
                return old;
            }

            old.clean();
            // Keep the old deletion file.
            deleteDeletionFile = ((GeneratedDeletionFile) old).deleteDeletionFile;
            return this;
        }

        @Override
        public void clean() {
            if (newDeletionFile != null) {
                dvIndexFile.delete(newDeletionFile);
            }
        }
    }

    /** A lazy generation implementation of {@link CompactDeletionFile}. */
    class LazyCompactDeletionFile implements CompactDeletionFile {

        private final BucketedDvMaintainer maintainer;

        private boolean generated = false;

        public LazyCompactDeletionFile(BucketedDvMaintainer maintainer) {
            this.maintainer = maintainer;
        }

        @Override
        public Pair<IndexFileMeta, IndexFileMeta> getOrCompute() {
            generated = true;
            return generateFiles(maintainer).getOrCompute();
        }

        @Override
        public CompactDeletionFile mergeOldFile(CompactDeletionFile old) {
            if (!(old instanceof LazyCompactDeletionFile)) {
                throw new IllegalStateException(
                        "old should be a LazyCompactDeletionFile, but it is: " + old.getClass());
            }

            if (((LazyCompactDeletionFile) old).generated) {
                throw new IllegalStateException("old should not be generated, this is a bug.");
            }

            return this;
        }

        @Override
        public void clean() {}
    }
}
