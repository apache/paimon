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

package org.apache.paimon.flink.orphan;

import org.apache.paimon.PagedList;
import org.apache.paimon.Snapshot;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.flink.utils.BoundedOneInputOperator;
import org.apache.paimon.flink.utils.BoundedTwoInputOperator;
import org.apache.paimon.fs.Path;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestFile;
import org.apache.paimon.operation.CleanOrphanFilesResult;
import org.apache.paimon.operation.OrphanFilesClean;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.utils.FileStorePathFactory;
import org.apache.paimon.utils.StringUtils;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.operators.InputSelection;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import static org.apache.flink.api.common.typeinfo.BasicTypeInfo.STRING_TYPE_INFO;
import static org.apache.flink.util.Preconditions.checkState;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Flink {@link OrphanFilesClean}, it will submit a job for multiple tables in batches. */
public class BatchFlinkOrphanFilesClean<T extends FlinkOrphanFilesClean>
        extends FlinkOrphanFilesClean {

    protected static final Logger LOG = LoggerFactory.getLogger(BatchFlinkOrphanFilesClean.class);

    protected String databaseName;
    protected List<T> cleaners;
    // Map to store cleaners by their full identifier name, for quick lookup in ProcessFunctions
    protected Map<String, T> cleanerMap;

    public BatchFlinkOrphanFilesClean(
            T cleaner, long olderThanMillis, boolean dryRun, @Nullable Integer parallelism) {
        super(cleaner.getTable(), olderThanMillis, dryRun, parallelism);
        this.cleanerMap = new HashMap<>();
        FileStoreTable table = cleaner.getTable();
        Identifier id = table.catalogEnvironment().identifier();
        if (id != null) {
            this.cleanerMap.put(id.getFullName(), cleaner);
        }
        this.cleaners = Collections.singletonList(cleaner);
    }

    public BatchFlinkOrphanFilesClean(
            String databaseName,
            List<T> cleaners,
            long olderThanMillis,
            boolean dryRun,
            @Nullable Integer parallelism) {
        super(cleaners.get(0).getTable(), olderThanMillis, dryRun, parallelism);
        this.databaseName = databaseName;
        this.cleaners = cleaners;
        // Initialize cleanerMap for quick lookup
        this.cleanerMap = new HashMap<>();
        for (T cleaner : cleaners) {
            FileStoreTable table = cleaner.getTable();
            Identifier id = table.catalogEnvironment().identifier();
            if (id != null) {
                this.cleanerMap.put(id.getFullName(), cleaner);
            }
        }
    }

    protected DataStream<CleanOrphanFilesResult> buildBranchSnapshotDirDeletedStream(
            StreamExecutionEnvironment env, List<BranchTableInfo> branchTableInfos) {
        return env.fromCollection(branchTableInfos)
                .process(
                        new ProcessFunction<BranchTableInfo, Tuple2<Long, Long>>() {

                            @Override
                            public void processElement(
                                    BranchTableInfo branchTableInfo,
                                    ProcessFunction<BranchTableInfo, Tuple2<Long, Long>>.Context
                                            ctx,
                                    Collector<Tuple2<Long, Long>> out)
                                    throws Exception {
                                // Directly get cleaner from outer class's cleanerMap
                                String tableKey = branchTableInfo.getIdentifier().getFullName();
                                T cleaner = cleanerMap.get(tableKey);
                                if (cleaner == null) {
                                    throw new RuntimeException(
                                            "Cleaner for table "
                                                    + tableKey
                                                    + " not found in cleanerMap");
                                }
                                cleaner.processForBranchSnapshotDirDeleted(
                                        branchTableInfo.getBranch(), out);
                            }
                        })
                .keyBy(tuple -> 1)
                .reduce(
                        (ReduceFunction<Tuple2<Long, Long>>)
                                (value1, value2) ->
                                        new Tuple2<>(value1.f0 + value2.f0, value1.f1 + value2.f1))
                .setParallelism(1)
                .map(tuple -> new CleanOrphanFilesResult(tuple.f0, tuple.f1));
    }

    @Nullable
    @Override
    public DataStream<CleanOrphanFilesResult> doOrphanClean(StreamExecutionEnvironment env) {
        configureFlinkEnvironment(env);
        LOG.info("Starting orphan files clean for {} tables: {}", cleaners.size(), cleaners);
        List<BranchTableInfo> branchTableInfos = new ArrayList<>();
        long start = System.currentTimeMillis();
        for (T cleaner : cleaners) {
            FileStoreTable table = cleaner.getTable();
            Identifier identifier = table.catalogEnvironment().identifier();
            if (identifier == null) {
                LOG.warn("Table {} does not have identifier, skip it", table.name());
                continue;
            }
            Map<String, String> catalogOptions = new HashMap<>();
            org.apache.paimon.catalog.CatalogContext catalogContext =
                    table.catalogEnvironment().catalogContext();
            if (catalogContext != null) {
                catalogOptions.putAll(catalogContext.options().toMap());
            }
            // Use instance method validBranches() from cleaner
            // Since validBranches() is protected, we need to call it through the cleaner instance
            List<String> branches = cleaner.validBranches();
            branches.forEach(
                    branch ->
                            branchTableInfos.add(
                                    new BranchTableInfo(
                                            branch,
                                            identifier.getDatabaseName(),
                                            identifier.getObjectName(),
                                            catalogOptions)));
        }
        LOG.info(
                "End orphan files validBranches: spend [{}] ms",
                System.currentTimeMillis() - start);

        // snapshot and changelog files are the root of everything, so they are handled specially
        // here, and subsequently, we will not count their orphan files.
        DataStream<CleanOrphanFilesResult> branchSnapshotDirDeleted =
                buildBranchSnapshotDirDeletedStream(env, branchTableInfos);

        // branch and manifest file
        final OutputTag<Tuple2<String, String>> manifestOutputTag =
                new OutputTag<Tuple2<String, String>>("manifest-output") {};

        SingleOutputStreamOperator<String> usedManifestFiles =
                env.fromCollection(branchTableInfos)
                        .process(
                                new ProcessFunction<
                                        BranchTableInfo, Tuple2<BranchTableInfo, String>>() {

                                    @Override
                                    public void processElement(
                                            BranchTableInfo branchTableInfo,
                                            ProcessFunction<
                                                                    BranchTableInfo,
                                                                    Tuple2<BranchTableInfo, String>>
                                                            .Context
                                                    ctx,
                                            Collector<Tuple2<BranchTableInfo, String>> out)
                                            throws Exception {
                                        // Directly get cleaner from outer class's cleanerMap
                                        String tableKey =
                                                branchTableInfo.getIdentifier().getFullName();
                                        T cleaner = cleanerMap.get(tableKey);
                                        if (cleaner == null) {
                                            throw new RuntimeException(
                                                    "Cleaner for table "
                                                            + tableKey
                                                            + " not found in cleanerMap");
                                        }
                                        for (Snapshot snapshot :
                                                cleaner.safelyGetAllSnapshots(
                                                        branchTableInfo.getBranch())) {
                                            out.collect(
                                                    new Tuple2<>(
                                                            branchTableInfo, snapshot.toJson()));
                                        }
                                    }
                                })
                        .rebalance()
                        .process(
                                new ProcessFunction<Tuple2<BranchTableInfo, String>, String>() {

                                    @Override
                                    public void processElement(
                                            Tuple2<BranchTableInfo, String> branchAndSnapshot,
                                            ProcessFunction<Tuple2<BranchTableInfo, String>, String>
                                                            .Context
                                                    ctx,
                                            Collector<String> out)
                                            throws Exception {
                                        BranchTableInfo branchTableInfo = branchAndSnapshot.f0;
                                        Snapshot snapshot = Snapshot.fromJson(branchAndSnapshot.f1);
                                        // Directly get cleaner from outer class's cleanerMap
                                        String tableKey =
                                                branchTableInfo.getIdentifier().getFullName();
                                        T cleaner =
                                                BatchFlinkOrphanFilesClean.this.cleanerMap.get(
                                                        tableKey);
                                        if (cleaner == null) {
                                            throw new RuntimeException(
                                                    "Cleaner for table "
                                                            + tableKey
                                                            + " not found in cleanerMap");
                                        }
                                        String branch = branchTableInfo.getBranch();
                                        FileStoreTable tableToUse = cleaner.getTable();
                                        FileStoreTable branchTable =
                                                tableToUse.switchToBranch(branch);
                                        FileStorePathFactory pathFactory =
                                                branchTable.store().pathFactory();
                                        Consumer<String> manifestConsumer =
                                                manifest -> {
                                                    // Output format: branch:tableIdentifier as
                                                    // first string
                                                    Tuple2<String, String> tuple2 =
                                                            new Tuple2<>(
                                                                    branch + ":" + tableKey,
                                                                    manifest);
                                                    ctx.output(manifestOutputTag, tuple2);
                                                };
                                        // Convert relative paths to absolute paths in batch mode
                                        Consumer<String> usedFileConsumer =
                                                fileName -> {
                                                    // Convert relative path to absolute path
                                                    Path absolutePath =
                                                            convertToAbsolutePath(
                                                                    pathFactory, fileName);
                                                    out.collect(absolutePath.toUri().toString());
                                                };
                                        cleaner.collectWithoutDataFile(
                                                branch,
                                                snapshot,
                                                usedFileConsumer,
                                                manifestConsumer);
                                    }
                                });

        DataStream<String> usedFiles =
                usedManifestFiles
                        .getSideOutput(manifestOutputTag)
                        .keyBy(tuple2 -> tuple2.f0 + ":" + tuple2.f1)
                        .transform(
                                "datafile-reader",
                                STRING_TYPE_INFO,
                                new BoundedOneInputOperator<Tuple2<String, String>, String>() {

                                    private final Set<Tuple2<String, String>> manifests =
                                            new HashSet<>();

                                    @Override
                                    public void processElement(
                                            StreamRecord<Tuple2<String, String>> element) {
                                        manifests.add(element.getValue());
                                    }

                                    @Override
                                    public void endInput() throws IOException {
                                        // Group manifests by tableIdentifier
                                        Map<String, Set<Tuple2<String, String>>> manifestsByTable =
                                                new HashMap<>();
                                        for (Tuple2<String, String> tuple2 : manifests) {
                                            // Parse branch:tableIdentifier from tuple2.f0
                                            String[] parts = tuple2.f0.split(":", 2);
                                            String branch = parts[0];
                                            String tableIdentifier =
                                                    parts.length > 1 ? parts[1] : null;

                                            if (tableIdentifier == null) {
                                                throw new RuntimeException(
                                                        "Invalid manifest format: "
                                                                + tuple2.f0
                                                                + ". Expected format: branch:tableIdentifier");
                                            }

                                            manifestsByTable
                                                    .computeIfAbsent(
                                                            tableIdentifier, k -> new HashSet<>())
                                                    .add(new Tuple2<>(branch, tuple2.f1));
                                        }

                                        // Process manifests for each table
                                        for (Map.Entry<String, Set<Tuple2<String, String>>> entry :
                                                manifestsByTable.entrySet()) {
                                            String tableIdentifier = entry.getKey();
                                            Set<Tuple2<String, String>> tableManifests =
                                                    entry.getValue();

                                            try {
                                                T cleanerToUse = cleanerMap.get(tableIdentifier);
                                                if (cleanerToUse == null) {
                                                    throw new RuntimeException(
                                                            "Cleaner for table "
                                                                    + tableIdentifier
                                                                    + " not found in cleanerMap");
                                                }
                                                // In batch mode, convert relative paths to absolute
                                                // paths
                                                // to avoid conflicts when multiple tables have
                                                // files with the same name
                                                endInputForUsedFilesForBatch(
                                                        cleanerToUse, tableManifests, output);
                                            } catch (Exception e) {
                                                throw new IOException(
                                                        "Failed to process manifests for table: "
                                                                + tableIdentifier,
                                                        e);
                                            }
                                        }
                                    }
                                });

        usedFiles = usedFiles.union(usedManifestFiles);
        // Parallelize table processing by passing table identifiers to flatMap
        List<String> tableIdentifiers = new ArrayList<>();
        for (T cleaner : cleaners) {
            FileStoreTable table = cleaner.getTable();
            Identifier id = table.catalogEnvironment().identifier();
            if (id != null) {
                tableIdentifiers.add(id.getFullName());
            }
        }
        DataStream<Tuple2<String, Long>> candidates =
                env.fromCollection(tableIdentifiers)
                        .flatMap(
                                new FlatMapFunction<String, Tuple2<String, Long>>() {
                                    @Override
                                    public void flatMap(
                                            String tableIdentifier,
                                            Collector<Tuple2<String, Long>> out)
                                            throws Exception {
                                        T cleaner =
                                                BatchFlinkOrphanFilesClean.this.cleanerMap.get(
                                                        tableIdentifier);
                                        if (cleaner == null) {
                                            LOG.warn(
                                                    "Table {} not found in cleanerMap, skip it",
                                                    tableIdentifier);
                                            return;
                                        }
                                        cleaner.listPaimonFilesForTable(out);
                                    }
                                });

        DataStream<CleanOrphanFilesResult> deleted =
                usedFiles
                        .keyBy(f -> f)
                        .connect(candidates.keyBy(pathAndSize -> pathAndSize.f0))
                        .transform(
                                "files_join",
                                TypeInformation.of(CleanOrphanFilesResult.class),
                                new BoundedTwoInputOperator<
                                        String, Tuple2<String, Long>, CleanOrphanFilesResult>() {

                                    private boolean buildEnd;
                                    private long emittedFilesCount;
                                    private long emittedFilesLen;

                                    private final Set<String> used = new HashSet<>();

                                    @Override
                                    public InputSelection nextSelection() {
                                        return buildEnd
                                                ? InputSelection.SECOND
                                                : InputSelection.FIRST;
                                    }

                                    @Override
                                    public void endInput(int inputId) {
                                        buildEnd =
                                                endInputForDeleted(
                                                        inputId,
                                                        buildEnd,
                                                        emittedFilesCount,
                                                        emittedFilesLen,
                                                        output);
                                    }

                                    @Override
                                    public void processElement1(StreamRecord<String> element) {
                                        used.add(element.getValue());
                                    }

                                    @Override
                                    public void processElement2(
                                            StreamRecord<Tuple2<String, Long>> element) {
                                        checkState(buildEnd, "Should build ended.");
                                        Tuple2<String, Long> fileInfo = element.getValue();
                                        String value = fileInfo.f0;
                                        // Use full path for comparison to avoid conflicts when
                                        // multiple tables have files with the same name
                                        if (!used.contains(value)) {
                                            emittedFilesCount++;
                                            emittedFilesLen += fileInfo.f1;
                                            Path path = new Path(value);
                                            cleanFile(path);
                                            LOG.info("Dry clean: {}", path);
                                        }
                                    }
                                });
        deleted = deleted.union(branchSnapshotDirDeleted);

        return deleted;
    }

    /**
     * Convert relative paths from manifest entries to absolute paths to avoid conflicts when
     * multiple tables have files with the same name in batch mode.
     */
    private void endInputForUsedFilesForBatch(
            T cleaner, Set<Tuple2<String, String>> manifests, Output<StreamRecord<String>> output)
            throws IOException {
        FileStoreTable tableToUse = cleaner.getTable();
        Map<String, ManifestFile> branchManifests = new HashMap<>();
        for (Tuple2<String, String> tuple2 : manifests) {
            String branch = tuple2.f0;
            FileStoreTable branchTable = tableToUse.switchToBranch(branch);
            ManifestFile manifestFile =
                    branchManifests.computeIfAbsent(
                            branch, key -> branchTable.store().manifestFileFactory().create());
            FileStorePathFactory pathFactory = branchTable.store().pathFactory();
            retryReadingFiles(
                            () -> manifestFile.readWithIOException(tuple2.f1),
                            Collections.<ManifestEntry>emptyList())
                    .forEach(
                            f -> {
                                // Convert relative path to absolute path
                                Path absolutePath =
                                        pathFactory
                                                .createDataFilePathFactory(
                                                        f.partition(), f.bucket())
                                                .toPath(f);
                                output.collect(new StreamRecord<>(absolutePath.toUri().toString()));
                                // Handle extra files
                                for (String extraFile : f.file().extraFiles()) {
                                    Path extraFilePath =
                                            new Path(absolutePath.getParent(), extraFile);
                                    output.collect(
                                            new StreamRecord<>(extraFilePath.toUri().toString()));
                                }
                            });
        }
    }

    /**
     * Convert relative file path to absolute path based on file type (manifest, index, statistics,
     * etc.).
     */
    private Path convertToAbsolutePath(FileStorePathFactory pathFactory, String fileName) {
        // Determine file type based on file name prefix
        if (fileName.startsWith(FileStorePathFactory.MANIFEST_LIST_PREFIX)) {
            return pathFactory.toManifestListPath(fileName);
        } else if (fileName.startsWith(FileStorePathFactory.MANIFEST_PREFIX)) {
            return pathFactory.toManifestFilePath(fileName);
        } else if (fileName.startsWith(FileStorePathFactory.INDEX_MANIFEST_PREFIX)) {
            return pathFactory.toManifestFilePath(fileName);
        } else if (fileName.startsWith(FileStorePathFactory.INDEX_PREFIX)) {
            return new Path(pathFactory.indexPath(), fileName);
        } else if (fileName.startsWith(FileStorePathFactory.STATISTICS_PREFIX)) {
            return new Path(pathFactory.statisticsPath(), fileName);
        } else {
            // For snapshot files (snapshot-xxx format) and other files
            // Snapshot files are stored in snapshot/ directory
            if (fileName.startsWith("snapshot-")) {
                return new Path(new Path(pathFactory.root(), "snapshot"), fileName);
            }
            // This is a fallback, should not happen in normal cases
            LOG.warn(
                    "Unknown file type for fileName: {}, assuming it's in root directory",
                    fileName);
            return new Path(pathFactory.root(), fileName);
        }
    }

    public static CleanOrphanFilesResult executeDatabaseOrphanFiles(
            StreamExecutionEnvironment env,
            Catalog catalog,
            long olderThanMillis,
            boolean dryRun,
            @Nullable Integer parallelism,
            String databaseName,
            @Nullable String tableName,
            int batchSize)
            throws Catalog.DatabaseNotExistException, Catalog.TableNotExistException {
        List<DataStream<CleanOrphanFilesResult>> orphanFilesCleans = new ArrayList<>();

        if (tableName == null || "*".equals(tableName)) {
            processAllTablesInBatches(
                    env,
                    catalog,
                    databaseName,
                    olderThanMillis,
                    dryRun,
                    parallelism,
                    batchSize,
                    orphanFilesCleans);
        } else {
            processSingleTable(
                    env,
                    catalog,
                    databaseName,
                    tableName,
                    olderThanMillis,
                    dryRun,
                    parallelism,
                    orphanFilesCleans);
        }

        DataStream<CleanOrphanFilesResult> result = null;
        for (DataStream<CleanOrphanFilesResult> clean : orphanFilesCleans) {
            if (result == null) {
                result = clean;
            } else {
                result = result.union(clean);
            }
        }

        return sum(result);
    }

    private static void processAllTablesInBatches(
            StreamExecutionEnvironment env,
            Catalog catalog,
            String databaseName,
            long olderThanMillis,
            boolean dryRun,
            @Nullable Integer parallelism,
            int batchSize,
            List<DataStream<CleanOrphanFilesResult>> orphanFilesCleans)
            throws Catalog.DatabaseNotExistException {
        String pageToken = null;
        List<FlinkOrphanFilesClean> batchCleaners = new ArrayList<>();
        int totalTablesCollected = 0;
        int batchCount = 0;
        int pageCount = 0;

        do {
            pageCount++;
            PagedList<Table> pagedTables =
                    catalog.listTableDetailsPaged(databaseName, null, pageToken, null, null);
            LOG.info(
                    "[BATCH_ORPHAN_CLEAN] Page {} START: received {} tables from catalog, current batchCleaners.size() = {}, total collected = {}",
                    pageCount,
                    pagedTables.getElements().size(),
                    batchCleaners.size(),
                    totalTablesCollected);

            for (Table table : pagedTables.getElements()) {
                if (!(table instanceof FileStoreTable)) {
                    LOG.warn("table {} is not a FileStoreTable, so ignore it", table.name());
                    continue;
                }
                FlinkOrphanFilesClean cleaner =
                        new FlinkOrphanFilesClean(
                                (FileStoreTable) table, olderThanMillis, dryRun, parallelism);
                batchCleaners.add(cleaner);
                totalTablesCollected++;

                // When batch reaches batchSize, process it
                if (batchCleaners.size() >= batchSize) {
                    batchCount++;
                    LOG.info(
                            "[BATCH_ORPHAN_CLEAN] Processing batch #{} of {} tables (batch size: {}), total collected so far: {}",
                            batchCount,
                            batchCleaners.size(),
                            batchSize,
                            totalTablesCollected);
                    DataStream<CleanOrphanFilesResult> clean =
                            new BatchFlinkOrphanFilesClean<>(
                                            databaseName,
                                            new ArrayList<>(batchCleaners),
                                            olderThanMillis,
                                            dryRun,
                                            parallelism)
                                    .doOrphanClean(env);
                    if (clean != null) {
                        orphanFilesCleans.add(clean);
                    }
                    batchCleaners.clear();
                    LOG.info(
                            "[BATCH_ORPHAN_CLEAN] Batch #{} processed and cleared, batchCleaners.size() = {}",
                            batchCount,
                            batchCleaners.size());
                }
            }
            pageToken = pagedTables.getNextPageToken();
            LOG.info(
                    "[BATCH_ORPHAN_CLEAN] Page {} FINISHED: batchCleaners.size() = {}, hasNextPage: {}",
                    pageCount,
                    batchCleaners.size(),
                    !StringUtils.isNullOrWhitespaceOnly(pageToken));
        } while (!StringUtils.isNullOrWhitespaceOnly(pageToken));

        // Process remaining tables in the last batch
        if (!batchCleaners.isEmpty()) {
            batchCount++;
            LOG.info(
                    "[BATCH_ORPHAN_CLEAN] Processing final batch #{} of {} tables (batch size: {}), total collected: {}",
                    batchCount,
                    batchCleaners.size(),
                    batchSize,
                    totalTablesCollected);
            DataStream<CleanOrphanFilesResult> clean =
                    new BatchFlinkOrphanFilesClean<>(
                                    databaseName,
                                    batchCleaners,
                                    olderThanMillis,
                                    dryRun,
                                    parallelism)
                            .doOrphanClean(env);
            if (clean != null) {
                orphanFilesCleans.add(clean);
            }
        }

        LOG.info(
                "[BATCH_ORPHAN_CLEAN] Batch processing completed: total batches = {}, total tables collected = {}, total pages = {}",
                batchCount,
                totalTablesCollected,
                pageCount);
    }

    private static void processSingleTable(
            StreamExecutionEnvironment env,
            Catalog catalog,
            String databaseName,
            String tableName,
            long olderThanMillis,
            boolean dryRun,
            @Nullable Integer parallelism,
            List<DataStream<CleanOrphanFilesResult>> orphanFilesCleans)
            throws Catalog.TableNotExistException {
        Identifier identifier = new Identifier(databaseName, tableName);
        Table table = catalog.getTable(identifier);
        checkArgument(
                table instanceof FileStoreTable,
                "Only FileStoreTable supports remove-orphan-files action. The table type is '%s'.",
                table.getClass().getName());

        FlinkOrphanFilesClean cleaner =
                new FlinkOrphanFilesClean(
                        (FileStoreTable) table, olderThanMillis, dryRun, parallelism);
        DataStream<CleanOrphanFilesResult> clean =
                new BatchFlinkOrphanFilesClean<>(cleaner, olderThanMillis, dryRun, parallelism)
                        .doOrphanClean(env);
        if (clean != null) {
            orphanFilesCleans.add(clean);
        }
    }
}
