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

import org.apache.paimon.Snapshot;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.flink.utils.BoundedOneInputOperator;
import org.apache.paimon.flink.utils.BoundedTwoInputOperator;
import org.apache.paimon.flink.utils.OrphanFilesCleanUtil;
import org.apache.paimon.fs.Path;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestFile;
import org.apache.paimon.operation.CleanOrphanFilesResult;
import org.apache.paimon.operation.OrphanFilesClean;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;

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
import java.io.Serializable;
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
import static org.apache.paimon.flink.orphan.FlinkOrphanFilesClean.sum;

/**
 * Flink {@link OrphanFilesClean}, it will submit a job for multiple tables in a combined
 * DataStream.
 */
public class CombinedFlinkOrphanFilesClean<T extends FlinkOrphanFilesClean>
        implements Serializable {

    private static final long serialVersionUID = 1L;

    protected static final Logger LOG =
            LoggerFactory.getLogger(CombinedFlinkOrphanFilesClean.class);

    protected String databaseName;
    protected final long olderThanMillis;
    protected final boolean dryRun;
    @Nullable protected final Integer parallelism;
    protected List<T> cleaners;
    // Map to store cleaners by their full identifier name, for quick lookup in ProcessFunctions
    protected Map<String, T> cleanerMap;
    // Map from table location to cleaner for quick lookup in BoundedTwoInputOperator
    protected Map<String, T> locationToCleanerMap;

    public CombinedFlinkOrphanFilesClean(
            String databaseName,
            List<T> cleaners,
            long olderThanMillis,
            boolean dryRun,
            @Nullable Integer parallelism) {
        this.olderThanMillis = olderThanMillis;
        this.dryRun = dryRun;
        this.databaseName = databaseName;
        this.parallelism = parallelism;
        this.cleaners = cleaners;
        // Initialize cleanerMap for quick lookup
        this.cleanerMap = new HashMap<>();
        this.locationToCleanerMap = new HashMap<>();
        for (T cleaner : cleaners) {
            FileStoreTable table = cleaner.getTable();
            Identifier id = table.catalogEnvironment().identifier();
            if (id != null) {
                this.cleanerMap.put(id.getFullName(), cleaner);
                String locationPath = table.location().toUri().getPath();
                this.locationToCleanerMap.put(locationPath, cleaner);
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
                                    Collector<Tuple2<Long, Long>> out) {
                                T cleaner = getCleanerForTable(branchTableInfo);
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
    public DataStream<CleanOrphanFilesResult> doOrphanClean(StreamExecutionEnvironment env) {
        OrphanFilesCleanUtil.configureFlinkEnvironment(env, parallelism);
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
                "End orphan files validBranches for {} tables: spend [{}] ms",
                cleaners.size(),
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
                                        T cleaner = getCleanerForTable(branchTableInfo);
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
                                        T cleaner = getCleanerForTable(branchTableInfo);
                                        String branch = branchTableInfo.getBranch();
                                        String tableKey =
                                                branchTableInfo.getIdentifier().getFullName();
                                        Consumer<String> manifestConsumer =
                                                manifest -> {
                                                    // Use "::" as delimiter to avoid conflicts with
                                                    // branch names containing ":"
                                                    Tuple2<String, String> tuple2 =
                                                            new Tuple2<>(
                                                                    branch + "::" + tableKey,
                                                                    manifest);
                                                    LOG.trace(
                                                            "[COMBINED_ORPHAN_CLEAN] Outputting manifest to side output: branch={}, tableKey={}, manifest={}",
                                                            branch,
                                                            tableKey,
                                                            manifest);
                                                    ctx.output(manifestOutputTag, tuple2);
                                                };
                                        cleaner.collectWithoutDataFile(
                                                branch, snapshot, out::collect, manifestConsumer);
                                    }
                                });

        DataStream<String> usedFiles =
                usedManifestFiles
                        .getSideOutput(manifestOutputTag)
                        .keyBy(tuple2 -> tuple2.f0 + "::" + tuple2.f1)
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
                                        LOG.trace(
                                                "[COMBINED_ORPHAN_CLEAN] Added manifest to set: {}, current size: {}",
                                                element.getValue(),
                                                manifests.size());
                                    }

                                    @Override
                                    public void endInput() throws IOException {
                                        LOG.trace(
                                                "[COMBINED_ORPHAN_CLEAN] endInput() called, manifests.size()={}",
                                                manifests.size());

                                        // Group manifests by tableIdentifier
                                        Map<String, Set<Tuple2<String, String>>> manifestsByTable =
                                                new HashMap<>();
                                        for (Tuple2<String, String> tuple2 : manifests) {
                                            // Parse branch::tableIdentifier from tuple2.f0
                                            int delimiterIndex = tuple2.f0.indexOf("::");
                                            if (delimiterIndex < 0) {
                                                LOG.error(
                                                        "[COMBINED_ORPHAN_CLEAN] Invalid manifest format: {}, expected format: branch::tableIdentifier",
                                                        tuple2.f0);
                                                throw new RuntimeException(
                                                        "Invalid manifest format: "
                                                                + tuple2.f0
                                                                + ". Expected format: branch::tableIdentifier");
                                            }
                                            String branch = tuple2.f0.substring(0, delimiterIndex);
                                            String tableIdentifier =
                                                    tuple2.f0.substring(delimiterIndex + 2);

                                            LOG.trace(
                                                    "[COMBINED_ORPHAN_CLEAN] Parsed manifest: branch={}, tableIdentifier={}, manifestFile={}",
                                                    branch,
                                                    tableIdentifier,
                                                    tuple2.f1);

                                            manifestsByTable
                                                    .computeIfAbsent(
                                                            tableIdentifier, k -> new HashSet<>())
                                                    .add(new Tuple2<>(branch, tuple2.f1));
                                        }

                                        LOG.trace(
                                                "[COMBINED_ORPHAN_CLEAN] Grouped manifests by table, manifestsByTable.size()={}",
                                                manifestsByTable.size());

                                        // Process manifests for each table
                                        for (Map.Entry<String, Set<Tuple2<String, String>>> entry :
                                                manifestsByTable.entrySet()) {
                                            String tableIdentifier = entry.getKey();
                                            Set<Tuple2<String, String>> tableManifests =
                                                    entry.getValue();

                                            LOG.trace(
                                                    "[COMBINED_ORPHAN_CLEAN] Processing table: {}, manifests count: {}",
                                                    tableIdentifier,
                                                    tableManifests.size());

                                            try {
                                                T cleanerToUse = cleanerMap.get(tableIdentifier);
                                                if (cleanerToUse == null) {
                                                    LOG.error(
                                                            "[COMBINED_ORPHAN_CLEAN] Cleaner for table {} not found in cleanerMap",
                                                            tableIdentifier);
                                                    throw new RuntimeException(
                                                            "Cleaner for table "
                                                                    + tableIdentifier
                                                                    + " not found in cleanerMap");
                                                }
                                                LOG.trace(
                                                        "[COMBINED_ORPHAN_CLEAN] Calling endInputForUsedFilesForCombined for table: {}",
                                                        tableIdentifier);
                                                endInputForUsedFilesForCombined(
                                                        cleanerToUse, tableManifests, output);
                                                LOG.trace(
                                                        "[COMBINED_ORPHAN_CLEAN] Finished endInputForUsedFilesForCombined for table: {}",
                                                        tableIdentifier);
                                            } catch (Exception e) {
                                                LOG.error(
                                                        "[COMBINED_ORPHAN_CLEAN] Failed to process manifests for table: {}",
                                                        tableIdentifier,
                                                        e);
                                                throw new IOException(
                                                        "Failed to process manifests for table: "
                                                                + tableIdentifier,
                                                        e);
                                            }
                                        }

                                        LOG.trace("[COMBINED_ORPHAN_CLEAN] endInput() finished");
                                    }
                                });

        usedFiles = usedFiles.union(usedManifestFiles);
        DataStream<Tuple2<String, Long>> candidates =
                env.fromCollection(Collections.singletonList(1), TypeInformation.of(Integer.class))
                        .process(
                                new ProcessFunction<Integer, Tuple2<String, Long>>() {
                                    @Override
                                    public void processElement(
                                            Integer i,
                                            ProcessFunction<Integer, Tuple2<String, Long>>.Context
                                                    ctx,
                                            Collector<Tuple2<String, Long>> out) {
                                        // Process all tables sequentially in a single thread
                                        for (T cleaner :
                                                CombinedFlinkOrphanFilesClean.this.cleaners) {
                                            cleaner.listPaimonFilesForTable(out);
                                        }
                                    }
                                })
                        .setParallelism(1);

        DataStream<CleanOrphanFilesResult> deleted =
                usedFiles
                        .keyBy(f -> f)
                        .connect(
                                candidates.keyBy(pathAndSize -> new Path(pathAndSize.f0).getName()))
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
                                                OrphanFilesCleanUtil.endInputForDeleted(
                                                        inputId,
                                                        buildEnd,
                                                        emittedFilesCount,
                                                        emittedFilesLen,
                                                        output);
                                    }

                                    @Override
                                    public void processElement1(StreamRecord<String> element) {
                                        used.add(element.getValue());
                                        LOG.trace(
                                                "[COMBINED_ORPHAN_CLEAN] Added to used set: fileName={}, usedSetSize={}",
                                                element.getValue(),
                                                used.size());
                                    }

                                    @Override
                                    public void processElement2(
                                            StreamRecord<Tuple2<String, Long>> element) {
                                        checkState(buildEnd, "Should build ended.");
                                        Tuple2<String, Long> fileInfo = element.getValue();
                                        String value = fileInfo.f0;
                                        Path path = new Path(value);
                                        if (!used.contains(path.getName())) {
                                            T cleanerForPath = getCleanerForPath(path);
                                            // Safety check: only delete files that belong to
                                            // current table
                                            if (cleanerForPath != null) {
                                                emittedFilesCount++;
                                                emittedFilesLen += fileInfo.f1;
                                                cleanerForPath.cleanFile(path);
                                                LOG.info("Dry clean: {}", path);
                                            } else {
                                                LOG.warn(
                                                        "[COMBINED_ORPHAN_CLEAN] Cannot find corresponding cleaner for file: {}, skip cleaning",
                                                        path);
                                            }
                                        }
                                    }
                                });
        deleted = deleted.union(branchSnapshotDirDeleted);

        return deleted;
    }

    private void endInputForUsedFilesForCombined(
            T cleaner, Set<Tuple2<String, String>> manifests, Output<StreamRecord<String>> output)
            throws IOException {
        FileStoreTable tableToUse = cleaner.getTable();
        Map<String, ManifestFile> branchManifests = new HashMap<>();
        for (Tuple2<String, String> tuple2 : manifests) {
            String branch = tuple2.f0;
            ManifestFile manifestFile =
                    branchManifests.computeIfAbsent(
                            branch,
                            key ->
                                    tableToUse
                                            .switchToBranch(key)
                                            .store()
                                            .manifestFileFactory()
                                            .create());
            OrphanFilesClean.retryReadingFiles(
                            () -> manifestFile.readWithIOException(tuple2.f1),
                            Collections.<ManifestEntry>emptyList())
                    .forEach(
                            f -> {
                                // Use file name for comparison, same as FlinkOrphanFilesClean
                                String fileName = f.fileName();
                                LOG.trace(
                                        "[COMBINED_ORPHAN_CLEAN] From manifest: branch={}, manifestFile={}, fileName={}",
                                        branch,
                                        tuple2.f1,
                                        fileName);
                                output.collect(new StreamRecord<>(fileName));
                                // Handle extra files
                                for (String extraFile : f.file().extraFiles()) {
                                    LOG.trace(
                                            "[COMBINED_ORPHAN_CLEAN] From manifest extra file: branch={}, manifestFile={}, extraFile={}",
                                            branch,
                                            tuple2.f1,
                                            extraFile);
                                    output.collect(new StreamRecord<>(extraFile));
                                }
                            });
        }
    }

    public static CleanOrphanFilesResult executeDatabaseOrphanFiles(
            StreamExecutionEnvironment env,
            Catalog catalog,
            long olderThanMillis,
            boolean dryRun,
            @Nullable Integer parallelism,
            String databaseName,
            List<String> tableNames) {
        List<FlinkOrphanFilesClean> cleaners = new ArrayList<>();

        for (String tableName : tableNames) {
            try {
                Identifier identifier = new Identifier(databaseName, tableName);
                Table table = catalog.getTable(identifier);
                if (!(table instanceof FileStoreTable)) {
                    LOG.warn("table {} is not a FileStoreTable, so ignore it", table.name());
                    continue;
                }

                FlinkOrphanFilesClean cleaner =
                        new FlinkOrphanFilesClean(
                                (FileStoreTable) table, olderThanMillis, dryRun, parallelism);
                cleaners.add(cleaner);
            } catch (Catalog.TableNotExistException e) {
                LOG.warn("Table {}.{} does not exist, so ignore it", databaseName, tableName, e);
            } catch (Exception e) {
                LOG.warn("Failed to process table {}.{}, so ignore it", databaseName, tableName, e);
            }
        }

        if (cleaners.isEmpty()) {
            return sum(null);
        }

        // Process all tables in a single DataStream
        DataStream<CleanOrphanFilesResult> clean =
                new CombinedFlinkOrphanFilesClean<>(
                                databaseName, cleaners, olderThanMillis, dryRun, parallelism)
                        .doOrphanClean(env);

        return sum(clean);
    }

    private T getCleanerForTable(BranchTableInfo branchTableInfo) {
        String tableKey = branchTableInfo.getIdentifier().getFullName();
        T cleaner = cleanerMap.get(tableKey);
        if (cleaner == null) {
            throw new RuntimeException(
                    "Cleaner for table " + tableKey + " not found in cleanerMap");
        }
        return cleaner;
    }

    private T getCleanerForPath(Path path) {
        String pathStr = path.toUri().getPath();
        T cleanerForPath = null;
        String longestMatch = null;

        for (Map.Entry<String, T> entry : locationToCleanerMap.entrySet()) {
            String tableLocation = entry.getKey();
            if (pathStr.startsWith(tableLocation)) {
                if (longestMatch == null || tableLocation.length() > longestMatch.length()) {
                    longestMatch = tableLocation;
                    cleanerForPath = entry.getValue();
                }
            }
        }
        return cleanerForPath;
    }

    private static class BranchTableInfo implements Serializable {
        private static final long serialVersionUID = 1L;

        private final String branch;
        private final String databaseName;
        private final String tableName;
        private final Map<String, String> catalogOptions;

        public BranchTableInfo(
                String branch,
                String databaseName,
                String tableName,
                Map<String, String> catalogOptions) {
            this.branch = branch;
            this.databaseName = databaseName;
            this.tableName = tableName;
            this.catalogOptions = catalogOptions;
        }

        public String getBranch() {
            return branch;
        }

        public Identifier getIdentifier() {
            return new Identifier(databaseName, tableName);
        }

        public Map<String, String> getCatalogOptions() {
            return catalogOptions;
        }
    }
}
