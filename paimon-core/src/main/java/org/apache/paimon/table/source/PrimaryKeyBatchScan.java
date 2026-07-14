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

import org.apache.paimon.Snapshot;
import org.apache.paimon.globalindex.GlobalIndexResult;
import org.apache.paimon.index.GlobalIndexMeta;
import org.apache.paimon.index.IndexFileHandler;
import org.apache.paimon.index.pk.PrimaryKeyIndexDefinition;
import org.apache.paimon.index.pk.PrimaryKeyIndexDefinitions;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.snapshot.SnapshotReader;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** Batch scan for primary-key tables and indexes. */
public class PrimaryKeyBatchScan extends AbstractBatchTableScan {

    private final FileStoreTable table;
    private final @Nullable PrimaryKeySortedIndexScan.ReaderFactory readerFactory;

    @Nullable private Predicate filter;
    @Nullable private GlobalIndexSplitResult globalIndexSplitResult;

    public static DataTableScan create(FileStoreTable table, SnapshotReader snapshotReader) {
        return new PrimaryKeyBatchScan(
                table,
                snapshotReader,
                table.catalogEnvironment().tableQueryAuth(table.coreOptions()));
    }

    public PrimaryKeyBatchScan(
            FileStoreTable table, SnapshotReader snapshotReader, TableQueryAuth queryAuth) {
        this(table, snapshotReader, queryAuth, null);
    }

    PrimaryKeyBatchScan(
            FileStoreTable table,
            SnapshotReader snapshotReader,
            TableQueryAuth queryAuth,
            @Nullable PrimaryKeySortedIndexScan.ReaderFactory readerFactory) {
        super(
                table.schema(),
                table.schemaManager(),
                table.coreOptions(),
                snapshotReader,
                queryAuth);
        this.table = table;
        this.readerFactory = readerFactory;
    }

    @Override
    public PrimaryKeyBatchScan withFilter(Predicate predicate) {
        this.filter = predicate;
        super.withFilter(predicate);
        return this;
    }

    @Override
    public PrimaryKeyBatchScan withGlobalIndexResult(GlobalIndexResult globalIndexResult) {
        if (globalIndexResult instanceof GlobalIndexSplitResult) {
            this.globalIndexSplitResult = (GlobalIndexSplitResult) globalIndexResult;
        }
        return this;
    }

    @Override
    @Nullable
    protected Plan globalIndexPlan() {
        if (globalIndexSplitResult == null) {
            return null;
        }
        if (globalIndexSplitResult.snapshotId() > 0) {
            maybeCreateReadProtectionTag(globalIndexSplitResult.snapshotId());
        }
        List<Split> splits = new ArrayList<>(globalIndexSplitResult.splits());
        return new PlanImpl(null, globalIndexSplitResult.snapshotId(), splits);
    }

    @Override
    protected Plan postProcessPlan(Plan dataPlan) {
        if (!(dataPlan instanceof SnapshotReader.Plan)) {
            return dataPlan;
        }
        SnapshotReader.Plan snapshotPlan = (SnapshotReader.Plan) dataPlan;
        if (filter == null
                || !snapshotReader.hasNonPartitionFilter()
                || table.schema().primaryKeys().isEmpty()
                || !options().deletionVectorsEnabled()
                || options().deletionVectorsMergeOnRead()
                || options().bucket() <= 0
                || snapshotPlan.snapshotId() == null
                || snapshotPlan.splits().isEmpty()) {
            return dataPlan;
        }

        List<DataSplit> dataSplits = new ArrayList<>();
        for (Split split : snapshotPlan.splits()) {
            if (!(split instanceof DataSplit) || ((DataSplit) split).isStreaming()) {
                return dataPlan;
            }
            dataSplits.add((DataSplit) split);
        }

        long snapshotId = snapshotPlan.snapshotId();
        Snapshot snapshot = snapshotReader.snapshotManager().snapshot(snapshotId);
        if (snapshot == null) {
            return dataPlan;
        }
        TableSchema snapshotSchema = table.schemaManager().schema(snapshot.schemaId());
        List<PrimaryKeyIndexDefinition> definitions =
                PrimaryKeyIndexDefinitions.create(snapshotSchema).definitions();
        Set<Integer> scalarFields = new HashSet<>();
        for (PrimaryKeyIndexDefinition definition : definitions) {
            if (definition.family() == PrimaryKeyIndexDefinition.Family.BTREE
                    || definition.family() == PrimaryKeyIndexDefinition.Family.BITMAP) {
                scalarFields.add(definition.fieldId());
            }
        }
        if (scalarFields.isEmpty()) {
            return dataPlan;
        }

        IndexFileHandler indexFileHandler = snapshotReader.indexFileHandler();
        if (indexFileHandler == null) {
            return dataPlan;
        }
        List<IndexManifestEntry> indexEntries =
                indexFileHandler.scan(
                        snapshot,
                        entry -> {
                            GlobalIndexMeta meta = entry.indexFile().globalIndexMeta();
                            return entry.kind() == FileKind.ADD
                                    && meta != null
                                    && meta.sourceMeta() != null
                                    && scalarFields.contains(meta.indexFieldId());
                        });
        PrimaryKeySortedIndexScan.Plan indexPlan =
                PrimaryKeySortedIndexScan.plan(snapshotId, dataSplits, definitions, indexEntries);
        PrimaryKeySortedIndexScan.ReaderFactory factory =
                readerFactory == null
                        ? PrimaryKeySortedIndexScan.readerFactory(
                                snapshotReader.snapshotManager().fileIO(),
                                snapshotReader.pathFactory(),
                                snapshotSchema.logicalRowType(),
                                options().toConfiguration())
                        : readerFactory;
        PrimaryKeySortedIndexScan.EvaluatedPlan evaluated =
                PrimaryKeySortedIndexScan.evaluate(
                        indexPlan, snapshotSchema.logicalRowType(), filter, definitions, factory);
        PrimaryKeySortedIndexResult result = new PrimaryKeySortedIndexResult(evaluated);
        return new PlanImpl(
                snapshotPlan.watermark(),
                snapshotPlan.snapshotId(),
                new ArrayList<>(result.splits()));
    }
}
