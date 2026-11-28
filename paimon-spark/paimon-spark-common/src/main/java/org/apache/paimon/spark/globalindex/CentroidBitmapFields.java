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

package org.apache.paimon.spark.globalindex;

import org.apache.paimon.Snapshot;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.globalindex.CentroidIndexFile;
import org.apache.paimon.globalindex.CentroidIndexMeta;
import org.apache.paimon.index.DataEvolutionIndexSourceMeta;
import org.apache.paimon.index.GlobalIndexMeta;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.index.IndexPathFactory;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Range;
import org.apache.paimon.utils.Triple;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.paimon.utils.Preconditions.checkState;

/** Manifest updates for Bitmap fields of centroid vector indexes. */
public final class CentroidBitmapFields {

    private CentroidBitmapFields() {}

    public static List<CommitMessage> drop(
            FileStoreTable table,
            String indexType,
            DataField vectorField,
            List<DataField> requestedFields,
            @Nullable PartitionPredicate partitionFilter) {
        checkArgument(
                table.coreOptions().dataEvolutionEnabled(),
                "Centroid Bitmap fields require a Data Evolution table.");
        checkArgument(!requestedFields.isEmpty(), "Specify at least one extra field.");
        Set<Integer> requestedIds =
                requestedFields.stream().map(DataField::id).collect(Collectors.toSet());
        Snapshot snapshot = table.snapshotManager().latestSnapshot();
        Collection<List<IndexManifestEntry>> groups =
                scanGroups(table, snapshot, indexType, vectorField, partitionFilter);
        long snapshotId = snapshot.id();
        RowType rowType = table.rowType();
        IndexPathFactory paths = table.store().pathFactory().globalIndexFileFactory();

        List<CommitMessage> messages = new ArrayList<>();
        for (List<IndexManifestEntry> group : groups) {
            IndexManifestEntry first = group.get(0);
            GlobalIndexMeta meta = first.indexFile().globalIndexMeta();
            int[] oldIds = meta.extraFieldIds();
            int[] remainingIds =
                    Arrays.stream(oldIds).filter(id -> !requestedIds.contains(id)).toArray();
            if (remainingIds.length == oldIds.length) {
                continue;
            }
            checkArgument(
                    remainingIds.length > 0,
                    "At least one Bitmap field must remain in row-ID range %s of the centroid %s index on column '%s'.",
                    meta.rowRange(),
                    indexType,
                    vectorField.name());
            for (int id : remainingIds) {
                checkArgument(
                        rowType.containsField(id),
                        "Retained Bitmap field ID %s is missing from the table schema. Rebuild the centroid index.",
                        id);
            }

            List<IndexFileMeta> removed = new ArrayList<>(group.size());
            List<IndexFileMeta> retained = new ArrayList<>(group.size());
            for (IndexManifestEntry entry : group) {
                IndexFileMeta file = entry.indexFile();
                checkState(
                        Arrays.equals(oldIds, file.globalIndexMeta().extraFieldIds()),
                        "Mixed centroid Bitmap fields.");
                removed.add(file);
                if (CentroidIndexMeta.isScalarIndex(file)
                        && requestedIds.contains(
                                ((CentroidIndexFile) CentroidIndexMeta.file(file, paths))
                                        .fieldId())) {
                    continue;
                }
                // Check source changes from this snapshot and keep the reused file's schema.
                long schemaId = DataEvolutionIndexSourceMeta.fromIndexFile(file).sourceSchemaId();
                byte[] sourceMeta =
                        new DataEvolutionIndexSourceMeta(snapshotId, schemaId).serialize();
                retained.add(withExtraFields(file, remainingIds, sourceMeta));
            }
            messages.add(
                    commitMessage(
                            first.partition(), first.bucket(), retained, removed, snapshotId));
        }
        return messages;
    }

    static Collection<List<IndexManifestEntry>> scanGroups(
            FileStoreTable table,
            @Nullable Snapshot snapshot,
            String indexType,
            DataField vectorField,
            @Nullable PartitionPredicate partitionFilter) {
        List<IndexManifestEntry> entries =
                table.store()
                        .newIndexFileHandler()
                        .scan(
                                snapshot,
                                entry -> {
                                    IndexFileMeta file = entry.indexFile();
                                    GlobalIndexMeta meta = file.globalIndexMeta();
                                    return indexType.equals(file.indexType())
                                            && meta != null
                                            && meta.indexFieldId() == vectorField.id()
                                            && CentroidIndexMeta.hasBitmapFields(meta)
                                            && (partitionFilter == null
                                                    || partitionFilter.test(entry.partition()));
                                });
        Map<Triple<BinaryRow, Integer, Range>, List<IndexManifestEntry>> groups = new HashMap<>();
        for (IndexManifestEntry entry : entries) {
            groups.computeIfAbsent(
                            Triple.of(
                                    entry.partition(),
                                    entry.bucket(),
                                    entry.indexFile().globalIndexMeta().rowRange()),
                            ignored -> new ArrayList<>())
                    .add(entry);
        }
        checkArgument(
                !groups.isEmpty(),
                "No centroid %s index with Bitmap fields on column '%s' in the selected partitions.",
                indexType,
                vectorField.name());
        return groups.values();
    }

    static IndexFileMeta withExtraFields(IndexFileMeta file, int[] fieldIds, byte[] sourceMeta) {
        GlobalIndexMeta current = file.globalIndexMeta();
        return new IndexFileMeta(
                file.indexType(),
                file.fileName(),
                file.fileSize(),
                file.rowCount(),
                new GlobalIndexMeta(
                        current.rowRangeStart(),
                        current.rowRangeEnd(),
                        current.indexFieldId(),
                        fieldIds,
                        current.indexMeta(),
                        sourceMeta),
                file.externalPath());
    }

    static CommitMessage commitMessage(
            BinaryRow partition,
            int bucket,
            List<IndexFileMeta> added,
            List<IndexFileMeta> deleted,
            long snapshotId) {
        return new CommitMessageImpl(
                        partition,
                        bucket,
                        null,
                        new DataIncrement(
                                Collections.emptyList(),
                                Collections.emptyList(),
                                Collections.emptyList(),
                                added,
                                deleted),
                        CompactIncrement.emptyIncrement())
                .withCheckFromSnapshot(snapshotId);
    }
}
