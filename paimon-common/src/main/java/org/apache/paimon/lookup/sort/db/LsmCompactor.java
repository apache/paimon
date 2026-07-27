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

package org.apache.paimon.lookup.sort.db;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/** Coordinates shared LSM compaction state independently of its execution mode. */
abstract class LsmCompactor {

    private final LsmLevels levels;
    private final UniversalCompactor compactor;
    private final int levelZeroFileCountTrigger;
    private final UniversalCompactor.FileSupplier fileSupplier;
    private final UniversalCompactor.FileDeleter fileDeleter;
    private final ThreadLocal<List<File>> deferredCompactionDeletes;

    LsmCompactor(
            LsmLevels levels,
            CompactorFactory compactorFactory,
            int levelZeroFileCountTrigger,
            UniversalCompactor.FileSupplier fileSupplier,
            UniversalCompactor.FileDeleter fileDeleter) {
        this.levels = levels;
        this.levelZeroFileCountTrigger = levelZeroFileCountTrigger;
        this.fileSupplier = fileSupplier;
        this.fileDeleter = fileDeleter;
        this.deferredCompactionDeletes = new ThreadLocal<>();
        this.compactor = compactorFactory.create(this::deferOrDeleteCompactedFile);
    }

    abstract void scheduleIfNeeded() throws IOException;

    abstract void applyBackpressure() throws IOException;

    abstract void checkFailure() throws IOException;

    abstract void await() throws IOException;

    final void fullCompact() throws IOException {
        checkFailure();
        await();
        compactLevelSnapshot(true);
    }

    final boolean needsCompaction() {
        return levels.needsCompaction(levelZeroFileCountTrigger);
    }

    final boolean needsBackpressure() {
        return (long) levels.fileCount(0) >= (long) levelZeroFileCountTrigger * 2;
    }

    final void compactUntilStable() throws IOException {
        while (needsCompaction()) {
            compactLevelSnapshot(false);
        }
    }

    private void compactLevelSnapshot(boolean fullCompaction) throws IOException {
        List<List<SstFileMetadata>> originalLevels = levels.snapshot();
        if (!fullCompaction && originalLevels.get(0).size() < levelZeroFileCountTrigger) {
            return;
        }

        List<List<SstFileMetadata>> compactedLevels = LsmLevels.copy(originalLevels);
        List<File> generatedFiles = new ArrayList<>();
        List<File> compactedFiles = new ArrayList<>();
        boolean published = false;
        deferredCompactionDeletes.set(compactedFiles);
        try {
            UniversalCompactor.FileSupplier trackingFileSupplier =
                    () -> {
                        File file = fileSupplier.newSstFile();
                        generatedFiles.add(file);
                        return file;
                    };
            if (fullCompaction) {
                compactor.fullCompact(compactedLevels, levels.maxLevels(), trackingFileSupplier);
            } else {
                compactor.maybeCompact(compactedLevels, levels.maxLevels(), trackingFileSupplier);
            }
            levels.publishCompaction(originalLevels, compactedLevels, compactedFiles, fileDeleter);
            published = true;
        } finally {
            deferredCompactionDeletes.remove();
            if (!published) {
                for (File generatedFile : generatedFiles) {
                    fileDeleter.deleteFile(generatedFile);
                }
            }
        }
    }

    private void deferOrDeleteCompactedFile(File file) {
        List<File> deferredDeletes = deferredCompactionDeletes.get();
        if (deferredDeletes != null) {
            deferredDeletes.add(file);
        } else {
            fileDeleter.deleteFile(file);
        }
    }

    /** Creates the compactor with the coordinated file-deletion callback. */
    interface CompactorFactory {

        UniversalCompactor create(UniversalCompactor.FileDeleter fileDeleter);
    }
}
