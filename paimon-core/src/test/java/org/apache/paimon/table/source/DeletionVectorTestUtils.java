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

package org.apache.paimon.table.source;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.deletionvectors.BitmapDeletionVector;
import org.apache.paimon.deletionvectors.DeletionVector;
import org.apache.paimon.deletionvectors.append.BaseAppendDeleteFileMaintainer;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.utils.Range;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.paimon.table.BucketMode.UNAWARE_BUCKET;

/** Test utilities for committing deletion vectors. */
class DeletionVectorTestUtils {

    static void commitDeletionVectors(FileStoreTable table, long... deletedRowIds)
            throws Exception {
        Map<String, DeletionVector> deletionVectors = new HashMap<>();
        List<DataFileMeta> dataFiles = dataFiles(table);
        for (long rowId : deletedRowIds) {
            DataFileMeta file = dataFileContaining(dataFiles, rowId);
            deletionVectors
                    .computeIfAbsent(file.fileName(), ignored -> new BitmapDeletionVector())
                    .delete(rowId - file.nonNullFirstRowId());
        }

        BaseAppendDeleteFileMaintainer maintainer =
                BaseAppendDeleteFileMaintainer.forUnawareAppend(
                        table.store().newIndexFileHandler(),
                        table.latestSnapshot().get(),
                        BinaryRow.EMPTY_ROW);
        for (Map.Entry<String, DeletionVector> entry : deletionVectors.entrySet()) {
            maintainer.notifyNewDeletionVector(entry.getKey(), entry.getValue());
        }

        List<IndexFileMeta> newIndexFiles = new ArrayList<>();
        List<IndexFileMeta> deletedIndexFiles = new ArrayList<>();
        for (IndexManifestEntry entry : maintainer.persist()) {
            if (entry.kind() == FileKind.ADD) {
                newIndexFiles.add(entry.indexFile());
            } else if (entry.kind() == FileKind.DELETE) {
                deletedIndexFiles.add(entry.indexFile());
            }
        }

        CommitMessage message =
                new CommitMessageImpl(
                        BinaryRow.EMPTY_ROW,
                        UNAWARE_BUCKET,
                        null,
                        new DataIncrement(
                                Collections.emptyList(),
                                Collections.emptyList(),
                                Collections.emptyList(),
                                newIndexFiles,
                                deletedIndexFiles),
                        CompactIncrement.emptyIncrement());
        try (BatchTableCommit commit = table.newBatchWriteBuilder().newCommit()) {
            commit.commit(Collections.singletonList(message));
        }
    }

    private static List<DataFileMeta> dataFiles(FileStoreTable table) {
        List<DataFileMeta> dataFiles = new ArrayList<>();
        for (ManifestEntry entry : table.store().newScan().plan().files()) {
            dataFiles.add(entry.file());
        }
        return dataFiles;
    }

    private static DataFileMeta dataFileContaining(List<DataFileMeta> dataFiles, long rowId) {
        for (DataFileMeta file : dataFiles) {
            if (file.firstRowId() == null) {
                continue;
            }
            Range range = file.nonNullRowIdRange();
            if (range.from <= rowId && rowId <= range.to) {
                return file;
            }
        }
        throw new IllegalArgumentException("No data file contains row id " + rowId);
    }

    private DeletionVectorTestUtils() {}
}
