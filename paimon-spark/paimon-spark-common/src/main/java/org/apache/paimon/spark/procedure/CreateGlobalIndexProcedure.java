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

package org.apache.paimon.spark.procedure;

import org.apache.paimon.Snapshot;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.options.Options;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.spark.globalindex.GlobalIndexBuilder;
import org.apache.paimon.spark.globalindex.GlobalIndexBuilderContext;
import org.apache.paimon.spark.globalindex.GlobalIndexBuilderFactory;
import org.apache.paimon.spark.util.ScanPlanHelper$;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.SpecialFields;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.table.sink.TableCommitImpl;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.snapshot.SnapshotReader;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Pair;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.PaimonUtils;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation;
import org.apache.spark.sql.functions;
import org.apache.spark.storage.StorageLevel;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Procedure to build global index files via Spark. */
public class CreateGlobalIndexProcedure extends CreateGlobalIndexProcedureBase {

    private static final String AUXILIARY_COLUMN_PREFIX = "_group_id_";

    protected CreateGlobalIndexProcedure(TableCatalog tableCatalog) {
        super(tableCatalog);
    }

    @Override
    protected List<IndexManifestEntry> buildIndex(
            FileStoreTable table,
            Identifier tableIdent,
            long rowsPerShard,
            PartitionPredicate partitionPredicate,
            RowType readRowType,
            String indexType,
            DataField indexField,
            Options userOptions,
            GlobalIndexBuilderFactory globalIndexBuilderFactory) {
        Map<BinaryRow, Map<Long, Dataset<Row>>> source =
                sourceDataset(
                        table,
                        rowsPerShard,
                        createRelation(tableIdent),
                        partitionPredicate,
                        readRowType);

        return buildIndex(
                table,
                source,
                indexType,
                readRowType,
                indexField,
                userOptions,
                globalIndexBuilderFactory);
    }

    private Map<BinaryRow, Map<Long, Dataset<Row>>> sourceDataset(
            FileStoreTable table,
            long rowsPerShard,
            DataSourceV2Relation relation,
            @Nullable PartitionPredicate partitionPredicate,
            RowType readRowType) {
        SnapshotReader snapshotReader = table.newSnapshotReader();
        if (partitionPredicate != null) {
            snapshotReader.withPartitionFilter(partitionPredicate);
        }
        Snapshot latest = table.snapshotManager().latestSnapshot();
        checkArgument(
                latest != null && latest.nextRowId() != null,
                "Table must have a snapshot with row tracking enabled.");
        long maxRowId = latest.nextRowId() - 1;
        Map<BinaryRow, DataSplit[]> packedSplits =
                snapshotReader.read().dataSplits().stream()
                        .collect(
                                Collectors.groupingBy(
                                        DataSplit::partition,
                                        Collectors.collectingAndThen(
                                                Collectors.toList(),
                                                list -> list.toArray(new DataSplit[0]))));

        Column[] columns =
                readRowType.getFieldNames().stream().map(functions::col).toArray(Column[]::new);

        Map<BinaryRow, Dataset<Row>> partitionDS =
                packedSplits.entrySet().stream()
                        .collect(
                                Collectors.toMap(
                                        Map.Entry::getKey,
                                        entry ->
                                                PaimonUtils.createDataset(
                                                                spark(),
                                                                ScanPlanHelper$.MODULE$
                                                                        .createNewScanPlan(
                                                                                entry.getValue(),
                                                                                relation))
                                                        .select(columns)));

        return divideDSWithinRowRange(partitionDS, maxRowId, rowsPerShard);
    }

    private Map<BinaryRow, Map<Long, Dataset<Row>>> divideDSWithinRowRange(
            Map<BinaryRow, Dataset<Row>> partitionDS, long maxRowId, long rowsPerShard) {
        int shardNum = (int) (maxRowId / rowsPerShard) + 1;
        String auxiliaryColumn = AUXILIARY_COLUMN_PREFIX + UUID.randomUUID();
        Column rule =
                functions
                        .floor(functions.col(SpecialFields.ROW_ID.name()).divide(rowsPerShard))
                        .cast("int");
        Map<BinaryRow, Map<Long, Dataset<Row>>> preparedDS = new HashMap<>();
        for (Map.Entry<BinaryRow, Dataset<Row>> entry : partitionDS.entrySet()) {
            BinaryRow partition = entry.getKey();
            Dataset<Row> ds = entry.getValue();

            Dataset<Row> taggedAndPartitioned =
                    ds.withColumn(auxiliaryColumn, rule)
                            .repartition(shardNum, functions.col(auxiliaryColumn))
                            .persist(StorageLevel.MEMORY_AND_DISK());
            // trigger persist
            taggedAndPartitioned.count();

            for (int i = 0; i < shardNum; i++) {
                Dataset<Row> subDataset =
                        taggedAndPartitioned
                                .filter(functions.col(auxiliaryColumn).equalTo(i))
                                .drop(auxiliaryColumn);
                preparedDS
                        .computeIfAbsent(partition, k -> new HashMap<>())
                        .put(i * rowsPerShard, subDataset);
            }
        }
        return preparedDS;
    }

    private List<IndexManifestEntry> buildIndex(
            FileStoreTable table,
            Map<BinaryRow, Map<Long, Dataset<Row>>> preparedDS,
            String indexType,
            RowType readType,
            DataField indexField,
            Options options,
            GlobalIndexBuilderFactory globalIndexBuilderFactory) {
        ExecutorService executor = Executors.newCachedThreadPool();
        List<Future<List<IndexManifestEntry>>> futures = new ArrayList<>();
        try {
            for (Map.Entry<BinaryRow, Map<Long, Dataset<Row>>> entry : preparedDS.entrySet()) {
                BinaryRow partition = entry.getKey();
                Map<Long, Dataset<Row>> partitions = entry.getValue();

                for (Map.Entry<Long, Dataset<Row>> partitionEntry : partitions.entrySet()) {
                    Long startOffset = partitionEntry.getKey();
                    Dataset<Row> partitionDS = partitionEntry.getValue();
                    GlobalIndexBuilderContext builderContext =
                            new GlobalIndexBuilderContext(
                                    table,
                                    partition,
                                    readType,
                                    indexField,
                                    indexType,
                                    startOffset,
                                    options);

                    futures.add(
                            executor.submit(
                                    () -> {
                                        GlobalIndexBuilder globalIndexBuilder =
                                                globalIndexBuilderFactory.create(builderContext);
                                        return globalIndexBuilder.build(partitionDS);
                                    }));
                }
            }

            List<IndexManifestEntry> entries = new ArrayList<>();
            for (Future<List<IndexManifestEntry>> future : futures) {
                try {
                    entries.addAll(future.get());
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("Index creation was interrupted", e);
                } catch (ExecutionException e) {
                    throw new RuntimeException("Build index failed", e.getCause());
                }
            }

            return entries;
        } finally {
            executor.shutdown();
        }
    }

    private void commit(FileStoreTable table, List<IndexManifestEntry> indexResults)
            throws Exception {
        Map<BinaryRow, List<IndexFileMeta>> partitionResults =
                indexResults.stream()
                        .map(s -> Pair.of(s.partition(), s.indexFile()))
                        .collect(
                                Collectors.groupingBy(
                                        Pair::getKey,
                                        Collectors.mapping(Pair::getValue, Collectors.toList())));

        List<CommitMessage> commitMessages = new ArrayList<>();
        for (Map.Entry<BinaryRow, List<IndexFileMeta>> entry : partitionResults.entrySet()) {
            BinaryRow partition = entry.getKey();
            List<IndexFileMeta> indexFiles = entry.getValue();
            commitMessages.add(
                    new CommitMessageImpl(
                            partition,
                            0,
                            null,
                            DataIncrement.indexIncrement(indexFiles),
                            CompactIncrement.emptyIncrement()));
        }

        try (TableCommitImpl commit = table.newCommit("global-index-create-" + UUID.randomUUID())) {
            commit.commit(commitMessages);
        }
    }

    public static ProcedureBuilder builder() {
        return new BaseProcedure.Builder<CreateGlobalIndexProcedure>() {
            @Override
            public CreateGlobalIndexProcedure doBuild() {
                return new CreateGlobalIndexProcedure(tableCatalog());
            }
        };
    }
}
