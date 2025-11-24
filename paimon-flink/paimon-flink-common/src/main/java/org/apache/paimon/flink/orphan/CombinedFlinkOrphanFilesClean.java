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
import org.apache.paimon.flink.utils.RuntimeContextUtils;
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
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;

import static org.apache.flink.api.common.typeinfo.BasicTypeInfo.STRING_TYPE_INFO;
import static org.apache.flink.util.Preconditions.checkState;
import static org.apache.paimon.flink.orphan.FlinkOrphanFilesClean.sum;

/**
 * Flink {@link OrphanFilesClean}, it will submit a job for multiple tables in a combined
 * DataStream.
 */
public class CombinedFlinkOrphanFilesClean implements Serializable {

    private static final long serialVersionUID = 1L;

    protected static final Logger LOG =
            LoggerFactory.getLogger(CombinedFlinkOrphanFilesClean.class);

    protected final List<Identifier> tableIdentifiers;
    protected final long olderThanMillis;
    protected final boolean dryRun;
    @Nullable protected final Integer parallelism;
    // Map to store cleaners by their full identifier name, for quick lookup in ProcessFunctions
    protected final Map<String, FlinkOrphanFilesClean> cleanerMap;
    // Map from table location to cleaner for quick lookup in BoundedTwoInputOperator
    protected final Map<String, FlinkOrphanFilesClean> locationToCleanerMap;

    public CombinedFlinkOrphanFilesClean(
            List<Identifier> tableIdentifiers,
            Map<String, FlinkOrphanFilesClean> cleanerMap,
            long olderThanMillis,
            boolean dryRun,
            @Nullable Integer parallelism) {
        this.olderThanMillis = olderThanMillis;
        this.dryRun = dryRun;
        this.tableIdentifiers = tableIdentifiers;
        this.parallelism = parallelism;
        this.cleanerMap = cleanerMap;
        this.locationToCleanerMap = new HashMap<>();
        for (Identifier tableIdentifier : tableIdentifiers) {
            FlinkOrphanFilesClean cleaner = cleanerMap.get(tableIdentifier.getFullName());
            FileStoreTable table = cleaner.getTable();
            if (Objects.nonNull(table)) {
                // Add table location
                String tableLocation = table.location().toUri().getPath();
                this.locationToCleanerMap.put(tableLocation, cleaner);
                // Add external paths if they exist
                String externalPaths = table.coreOptions().dataFileExternalPaths();
                if (externalPaths != null && !externalPaths.isEmpty()) {
                    String[] externalPathArr = externalPaths.split(",");
                    for (String externalPathStr : externalPathArr) {
                        String externalPath = new Path(externalPathStr.trim()).toUri().getPath();
                        this.locationToCleanerMap.put(externalPath, cleaner);
                    }
                }
            }
        }
    }

    protected DataStream<CleanOrphanFilesResult> buildBranchSnapshotDirDeletedStream(
            StreamExecutionEnvironment env, List<Identifier> branchIdentifiers) {
        return env.fromCollection(branchIdentifiers)
                .process(
                        new ProcessFunction<Identifier, Tuple2<Long, Long>>() {
                            @Override
                            public void processElement(
                                    Identifier identifier,
                                    ProcessFunction<Identifier, Tuple2<Long, Long>>.Context ctx,
                                    Collector<Tuple2<Long, Long>> out) {
                                FlinkOrphanFilesClean cleaner = getCleanerForTable(identifier);
                                cleaner.processForBranchSnapshotDirDeleted(
                                        identifier.getBranchNameOrDefault(), out);
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
        LOG.info(
                "Starting orphan files clean for {} tables: {}",
                tableIdentifiers.size(),
                tableIdentifiers);
        List<Identifier> branchIdentifiers = new ArrayList<>();
        long start = System.currentTimeMillis();
        for (Identifier tableIdentifier : tableIdentifiers) {
            FlinkOrphanFilesClean cleaner = cleanerMap.get(tableIdentifier.getFullName());
            if (Objects.isNull(cleaner)) {
                LOG.warn("Table {} does not have cleaner, skip it", tableIdentifier);
                continue;
            }
            List<String> branches = cleaner.validBranches();
            branches.forEach(
                    branch ->
                            branchIdentifiers.add(
                                    new Identifier(
                                            tableIdentifier.getDatabaseName(),
                                            tableIdentifier.getTableName(),
                                            branch)));
        }
        LOG.info(
                "End orphan files validBranches for {} tables: spend [{}] ms",
                cleanerMap.size(),
                System.currentTimeMillis() - start);

        // snapshot and changelog files are the root of everything, so they are handled specially
        // here, and subsequently, we will not count their orphan files.
        DataStream<CleanOrphanFilesResult> branchSnapshotDirDeleted =
                buildBranchSnapshotDirDeletedStream(env, branchIdentifiers);

        // branch and manifest file
        // f0: Identifier (table identifier without branch)
        // f1: Tuple2<String, String> (branch, manifest)
        final OutputTag<Tuple2<Identifier, Tuple2<String, String>>> manifestOutputTag =
                new OutputTag<Tuple2<Identifier, Tuple2<String, String>>>("manifest-output") {};

        SingleOutputStreamOperator<String> usedManifestFiles =
                env.fromCollection(branchIdentifiers)
                        .process(
                                new ProcessFunction<Identifier, Tuple2<Identifier, String>>() {

                                    @Override
                                    public void processElement(
                                            Identifier identifier,
                                            ProcessFunction<Identifier, Tuple2<Identifier, String>>
                                                            .Context
                                                    ctx,
                                            Collector<Tuple2<Identifier, String>> out)
                                            throws Exception {
                                        FlinkOrphanFilesClean cleaner =
                                                getCleanerForTable(identifier);
                                        for (Snapshot snapshot :
                                                cleaner.safelyGetAllSnapshots(
                                                        identifier.getBranchNameOrDefault())) {
                                            out.collect(
                                                    new Tuple2<>(identifier, snapshot.toJson()));
                                        }
                                    }
                                })
                        .rebalance()
                        .process(
                                new ProcessFunction<Tuple2<Identifier, String>, String>() {

                                    @Override
                                    public void processElement(
                                            Tuple2<Identifier, String> branchAndSnapshot,
                                            ProcessFunction<Tuple2<Identifier, String>, String>
                                                            .Context
                                                    ctx,
                                            Collector<String> out)
                                            throws Exception {
                                        Identifier identifier = branchAndSnapshot.f0;
                                        Snapshot snapshot = Snapshot.fromJson(branchAndSnapshot.f1);
                                        FlinkOrphanFilesClean cleaner =
                                                getCleanerForTable(identifier);
                                        String branch = identifier.getBranchNameOrDefault();
                                        Identifier tableIdentifier =
                                                Identifier.create(
                                                        identifier.getDatabaseName(),
                                                        identifier.getTableName());
                                        Consumer<String> manifestConsumer =
                                                manifest -> {
                                                    Tuple2<Identifier, Tuple2<String, String>>
                                                            tuple2 =
                                                                    new Tuple2<>(
                                                                            tableIdentifier,
                                                                            new Tuple2<>(
                                                                                    branch,
                                                                                    manifest));
                                                    LOG.trace(
                                                            "[COMBINED_ORPHAN_CLEAN] Outputting manifest to side output: identifier={}, branch={}, manifest={}",
                                                            tableIdentifier,
                                                            branch,
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
                        .keyBy(
                                tuple2 ->
                                        tuple2.f0) // Group by table identifier to process manifests
                        // per table
                        .transform(
                                "datafile-reader",
                                STRING_TYPE_INFO,
                                new BoundedOneInputOperator<
                                        Tuple2<Identifier, Tuple2<String, String>>, String>() {

                                    // Map from Identifier to Set of (branch, manifest) tuples
                                    private final Map<Identifier, Set<Tuple2<String, String>>>
                                            manifestsByTable = new HashMap<>();

                                    @Override
                                    public void processElement(
                                            StreamRecord<Tuple2<Identifier, Tuple2<String, String>>>
                                                    element) {
                                        Tuple2<Identifier, Tuple2<String, String>> value =
                                                element.getValue();
                                        Identifier tableIdentifier = value.f0;
                                        Tuple2<String, String> branchAndManifest = value.f1;
                                        String branch = branchAndManifest.f0;
                                        String manifest = branchAndManifest.f1;

                                        manifestsByTable
                                                .computeIfAbsent(
                                                        tableIdentifier, k -> new HashSet<>())
                                                .add(new Tuple2<>(branch, manifest));

                                        LOG.trace(
                                                "[COMBINED_ORPHAN_CLEAN] Added manifest to set: identifier={}, branch={}, manifest={}, current size: {}",
                                                tableIdentifier,
                                                branch,
                                                manifest,
                                                manifestsByTable.get(tableIdentifier).size());
                                    }

                                    @Override
                                    public void endInput() throws IOException {
                                        LOG.trace(
                                                "[COMBINED_ORPHAN_CLEAN] endInput() called, manifestsByTable.size()={}",
                                                manifestsByTable.size());

                                        // Process manifests for each table
                                        for (Map.Entry<Identifier, Set<Tuple2<String, String>>>
                                                entry : manifestsByTable.entrySet()) {
                                            Identifier tableIdentifier = entry.getKey();
                                            Set<Tuple2<String, String>> tableManifests =
                                                    entry.getValue();
                                            String tableKey = buildTableKey(tableIdentifier);

                                            LOG.trace(
                                                    "[COMBINED_ORPHAN_CLEAN] Processing table: {}, manifests count: {}",
                                                    tableIdentifier,
                                                    tableManifests.size());

                                            try {
                                                FlinkOrphanFilesClean cleanerToUse =
                                                        cleanerMap.get(tableKey);
                                                if (cleanerToUse == null) {
                                                    LOG.error(
                                                            "[COMBINED_ORPHAN_CLEAN] Cleaner for table {} not found in cleanerMap",
                                                            tableKey);
                                                    throw new RuntimeException(
                                                            "Cleaner for table "
                                                                    + tableKey
                                                                    + " not found in cleanerMap");
                                                }
                                                LOG.trace(
                                                        "[COMBINED_ORPHAN_CLEAN] Calling endInputForUsedFilesForCombined for table: {}",
                                                        tableKey);
                                                endInputForUsedFilesForCombined(
                                                        cleanerToUse, tableManifests, output);
                                                LOG.trace(
                                                        "[COMBINED_ORPHAN_CLEAN] Finished endInputForUsedFilesForCombined for table: {}",
                                                        tableKey);
                                            } catch (Exception e) {
                                                LOG.error(
                                                        "[COMBINED_ORPHAN_CLEAN] Failed to process manifests for table: {}",
                                                        tableKey,
                                                        e);
                                                throw new IOException(
                                                        "Failed to process manifests for table: "
                                                                + tableKey,
                                                        e);
                                            }
                                        }

                                        LOG.trace("[COMBINED_ORPHAN_CLEAN] endInput() finished");
                                    }
                                });

        usedFiles = usedFiles.union(usedManifestFiles);
        DataStream<Tuple2<String, Long>> candidates =
                env.fromCollection(tableIdentifiers, TypeInformation.of(Identifier.class))
                        .process(
                                new ProcessFunction<Identifier, Tuple2<String, Long>>() {
                                    @Override
                                    public void processElement(
                                            Identifier identifier,
                                            ProcessFunction<Identifier, Tuple2<String, Long>>
                                                            .Context
                                                    ctx,
                                            Collector<Tuple2<String, Long>> out) {
                                        LOG.info(
                                                "Processing table {} in subtask {}",
                                                identifier,
                                                RuntimeContextUtils.getIndexOfThisSubtask(
                                                        getRuntimeContext()));

                                        if (cleanerMap.containsKey(buildTableKey(identifier))) {
                                            cleanerMap
                                                    .get(buildTableKey(identifier))
                                                    .listPaimonFilesForTable(out);
                                        }
                                    }
                                })
                        .setParallelism(Objects.nonNull(parallelism) ? parallelism : 1);

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
                                            FlinkOrphanFilesClean cleanerForPath =
                                                    getCleanerForPath(path);
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
            FlinkOrphanFilesClean cleaner,
            Set<Tuple2<String, String>> manifests,
            Output<StreamRecord<String>> output)
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
            List<Identifier> tableIdentifiers) {
        Map<String, FlinkOrphanFilesClean> cleanersMap = new HashMap<>();
        List<Identifier> validTableIdentifiers = new ArrayList<>();
        for (Identifier tableIdentifier : tableIdentifiers) {
            try {
                Table table = catalog.getTable(tableIdentifier);
                if (!(table instanceof FileStoreTable)) {
                    LOG.warn("table {} is not a FileStoreTable, so ignore it", table.name());
                    continue;
                }

                FlinkOrphanFilesClean cleaner =
                        new FlinkOrphanFilesClean(
                                (FileStoreTable) table, olderThanMillis, dryRun, parallelism);
                validTableIdentifiers.add(tableIdentifier);
                cleanersMap.put(tableIdentifier.getFullName(), cleaner);
            } catch (Catalog.TableNotExistException e) {
                LOG.warn("Table {} does not exist, so ignore it", tableIdentifier.getFullName(), e);
            } catch (Exception e) {
                LOG.warn(
                        "Failed to process table {}, so ignore it",
                        tableIdentifier.getFullName(),
                        e);
            }
        }

        if (cleanersMap.isEmpty()) {
            return sum(null);
        }

        // Process all tables in a single DataStream
        DataStream<CleanOrphanFilesResult> clean =
                new CombinedFlinkOrphanFilesClean(
                                validTableIdentifiers,
                                cleanersMap,
                                olderThanMillis,
                                dryRun,
                                parallelism)
                        .doOrphanClean(env);

        return sum(clean);
    }

    private FlinkOrphanFilesClean getCleanerForTable(Identifier identifier) {
        String tableKey = buildTableKey(identifier);
        FlinkOrphanFilesClean cleaner = cleanerMap.get(tableKey);
        if (cleaner == null) {
            throw new RuntimeException(
                    "Cleaner for table " + tableKey + " not found in cleanerMap");
        }
        return cleaner;
    }

    private FlinkOrphanFilesClean getCleanerForPath(Path path) {
        String pathStr = path.toUri().getPath();
        FlinkOrphanFilesClean cleanerForPath = null;
        String longestMatch = null;

        for (Map.Entry<String, FlinkOrphanFilesClean> entry : locationToCleanerMap.entrySet()) {
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

    // build table full name which does not include branch name
    private String buildTableKey(Identifier identifier) {
        return String.format("%s.%s", identifier.getDatabaseName(), identifier.getTableName());
    }
}
