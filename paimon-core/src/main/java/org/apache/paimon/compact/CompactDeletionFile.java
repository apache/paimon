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

import org.apache.paimon.deletionvectors.DeletionVectorsMaintainer;
import org.apache.paimon.index.IndexFileHandler;
import org.apache.paimon.index.IndexFileMeta;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Optional;

/** Deletion File from compaction. */
public interface CompactDeletionFile {

    Optional<IndexFileMeta> getOrCompute();

    CompactDeletionFile mergeOldFile(CompactDeletionFile old);

    void clean();

    /**
     * Used by async compaction, when compaction task is completed, deletions file will be generated
     * immediately, so when updateCompactResult, we need to merge old deletion files (just delete
     * them).
     */
    static CompactDeletionFile generateFiles(DeletionVectorsMaintainer maintainer) {
        List<IndexFileMeta> files = maintainer.writeDeletionVectorsIndex();
        if (files.size() > 1) {
            throw new IllegalStateException(
                    "Should only generate one compact deletion file, this is a bug.");
        }

        return new GeneratedDeletionFile(
                files.isEmpty() ? null : files.get(0), maintainer.indexFileHandler());
    }

    /** For sync compaction, only create deletion files when prepareCommit. */
    static CompactDeletionFile lazyGeneration(DeletionVectorsMaintainer maintainer) {
        return new LazyCompactDeletionFile(maintainer);
    }

    /** A generated files implementation of {@link CompactDeletionFile}. */
    class GeneratedDeletionFile implements CompactDeletionFile {

        @Nullable private final IndexFileMeta deletionFile;
        private final IndexFileHandler fileHandler;

        private boolean getInvoked = false;

        public GeneratedDeletionFile(
                @Nullable IndexFileMeta deletionFile, IndexFileHandler fileHandler) {
            this.deletionFile = deletionFile;
            this.fileHandler = fileHandler;
        }

        @Override
        public Optional<IndexFileMeta> getOrCompute() {
            this.getInvoked = true;
            return Optional.ofNullable(deletionFile);
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

            if (deletionFile == null) {
                return old;
            }

            old.clean();
            return this;
        }

        @Override
        public void clean() {
            if (deletionFile != null) {
                fileHandler.deleteIndexFile(deletionFile);
            }
        }
    }

    /** A lazy generation implementation of {@link CompactDeletionFile}. */
    class LazyCompactDeletionFile implements CompactDeletionFile {

        private final DeletionVectorsMaintainer maintainer;

        private boolean generated = false;

        public LazyCompactDeletionFile(DeletionVectorsMaintainer maintainer) {
            this.maintainer = maintainer;
        }

        @Override
        public Optional<IndexFileMeta> getOrCompute() {
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
