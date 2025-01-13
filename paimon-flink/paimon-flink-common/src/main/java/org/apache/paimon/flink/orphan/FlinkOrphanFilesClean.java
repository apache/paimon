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
import org.apache.paimon.fs.FileStatus;
import org.apache.paimon.fs.Path;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestFile;
import org.apache.paimon.operation.CleanOrphanFilesResult;
import org.apache.paimon.operation.OrphanFilesClean;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.SerializableConsumer;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.operators.InputSelection;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.apache.flink.api.common.typeinfo.BasicTypeInfo.STRING_TYPE_INFO;
import static org.apache.flink.util.Preconditions.checkState;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Flink {@link OrphanFilesClean}, it will submit a job for a table. */
public class FlinkOrphanFilesClean extends OrphanFilesClean {

    @Nullable protected final Integer parallelism;

    public FlinkOrphanFilesClean(
            FileStoreTable table,
            long olderThanMillis,
            SerializableConsumer<Path> fileCleaner,
            @Nullable Integer parallelism) {
        super(table, olderThanMillis, fileCleaner);
        this.parallelism = parallelism;
    }

    @Nullable
    public DataStream<CleanOrphanFilesResult> doOrphanClean(StreamExecutionEnvironment env) {
        Configuration flinkConf = new Configuration();
        flinkConf.set(ExecutionOptions.RUNTIME_MODE, RuntimeExecutionMode.BATCH);
        flinkConf.set(ExecutionOptions.SORT_INPUTS, false);
        flinkConf.set(ExecutionOptions.USE_BATCH_STATE_BACKEND, false);
        if (parallelism != null) {
            flinkConf.set(CoreOptions.DEFAULT_PARALLELISM, parallelism);
        }
        // Flink 1.17 introduced this config, use string to keep compatibility
        flinkConf.setString("execution.batch.adaptive.auto-parallelism.enabled", "false");
        env.configure(flinkConf);

        List<String> branches = validBranches();

        // snapshot and changelog files are the root of everything, so they are handled specially
        // here, and subsequently, we will not count their orphan files.
        AtomicLong deletedFilesCountInLocal = new AtomicLong(0);
        AtomicLong deletedFilesLenInBytesInLocal = new AtomicLong(0);
        cleanSnapshotDir(
                branches,
                path -> deletedFilesCountInLocal.incrementAndGet(),
                deletedFilesLenInBytesInLocal::addAndGet);

        // branch and manifest file
        final OutputTag<Tuple2<String, String>> manifestOutputTag =
                new OutputTag<Tuple2<String, String>>("manifest-output") {};

        SingleOutputStreamOperator<String> usedManifestFiles =
                env.fromCollection(branches)
                        .process(
                                new ProcessFunction<String, Tuple2<String, String>>() {
                                    @Override
                                    public void processElement(
                                            String branch,
                                            ProcessFunction<String, Tuple2<String, String>>.Context
                                                    ctx,
                                            Collector<Tuple2<String, String>> out)
                                            throws Exception {
                                        for (Snapshot snapshot : safelyGetAllSnapshots(branch)) {
                                            out.collect(new Tuple2<>(branch, snapshot.toJson()));
                                        }
                                    }
                                })
                        .rebalance()
                        .process(
                                new ProcessFunction<Tuple2<String, String>, String>() {

                                    @Override
                                    public void processElement(
                                            Tuple2<String, String> branchAndSnapshot,
                                            ProcessFunction<Tuple2<String, String>, String>.Context
                                                    ctx,
                                            Collector<String> out)
                                            throws Exception {
                                        String branch = branchAndSnapshot.f0;
                                        Snapshot snapshot = Snapshot.fromJson(branchAndSnapshot.f1);
                                        Consumer<String> manifestConsumer =
                                                manifest -> {
                                                    Tuple2<String, String> tuple2 =
                                                            new Tuple2<>(branch, manifest);
                                                    ctx.output(manifestOutputTag, tuple2);
                                                };
                                        collectWithoutDataFile(
                                                branch, snapshot, out::collect, manifestConsumer);
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
                                        Map<String, ManifestFile> branchManifests = new HashMap<>();
                                        for (Tuple2<String, String> tuple2 : manifests) {
                                            ManifestFile manifestFile =
                                                    branchManifests.computeIfAbsent(
                                                            tuple2.f0,
                                                            key ->
                                                                    table.switchToBranch(key)
                                                                            .store()
                                                                            .manifestFileFactory()
                                                                            .create());
                                            retryReadingFiles(
                                                            () ->
                                                                    manifestFile
                                                                            .readWithIOException(
                                                                                    tuple2.f1),
                                                            Collections.<ManifestEntry>emptyList())
                                                    .forEach(
                                                            f -> {
                                                                List<String> files =
                                                                        new ArrayList<>();
                                                                files.add(f.fileName());
                                                                files.addAll(f.file().extraFiles());
                                                                files.forEach(
                                                                        file ->
                                                                                output.collect(
                                                                                        new StreamRecord<>(
                                                                                                file)));
                                                            });
                                        }
                                    }
                                });

        usedFiles = usedFiles.union(usedManifestFiles);

        List<String> fileDirs =
                listPaimonFileDirs().stream()
                        .map(Path::toUri)
                        .map(Object::toString)
                        .collect(Collectors.toList());
        DataStream<Pair<String, Long>> candidates =
                env.fromCollection(fileDirs)
                        .process(
                                new ProcessFunction<String, Pair<String, Long>>() {
                                    @Override
                                    public void processElement(
                                            String dir,
                                            ProcessFunction<String, Pair<String, Long>>.Context ctx,
                                            Collector<Pair<String, Long>> out) {
                                        for (FileStatus fileStatus :
                                                tryBestListingDirs(new Path(dir))) {
                                            if (oldEnough(fileStatus)) {
                                                out.collect(
                                                        Pair.of(
                                                                fileStatus
                                                                        .getPath()
                                                                        .toUri()
                                                                        .toString(),
                                                                fileStatus.getLen()));
                                            }
                                        }
                                    }
                                });

        DataStream<CleanOrphanFilesResult> deleted =
                usedFiles
                        .keyBy(f -> f)
                        .connect(
                                candidates.keyBy(
                                        pathAndSize -> new Path(pathAndSize.getKey()).getName()))
                        .transform(
                                "files_join",
                                TypeInformation.of(CleanOrphanFilesResult.class),
                                new BoundedTwoInputOperator<
                                        String, Pair<String, Long>, CleanOrphanFilesResult>() {

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
                                        switch (inputId) {
                                            case 1:
                                                checkState(!buildEnd, "Should not build ended.");
                                                LOG.info("Finish build phase.");
                                                buildEnd = true;
                                                break;
                                            case 2:
                                                checkState(buildEnd, "Should build ended.");
                                                LOG.info("Finish probe phase.");
                                                LOG.info(
                                                        "Clean files count : {}",
                                                        emittedFilesCount);
                                                LOG.info("Clean files size : {}", emittedFilesLen);
                                                output.collect(
                                                        new StreamRecord<>(
                                                                new CleanOrphanFilesResult(
                                                                        emittedFilesCount,
                                                                        emittedFilesLen)));
                                                break;
                                        }
                                    }

                                    @Override
                                    public void processElement1(StreamRecord<String> element) {
                                        used.add(element.getValue());
                                    }

                                    @Override
                                    public void processElement2(
                                            StreamRecord<Pair<String, Long>> element) {
                                        checkState(buildEnd, "Should build ended.");
                                        Pair<String, Long> fileInfo = element.getValue();
                                        String value = fileInfo.getLeft();
                                        Path path = new Path(value);
                                        if (!used.contains(path.getName())) {
                                            emittedFilesCount++;
                                            emittedFilesLen += fileInfo.getRight();
                                            fileCleaner.accept(path);
                                            LOG.info("Dry clean: {}", path);
                                        }
                                    }
                                });

        if (deletedFilesCountInLocal.get() != 0 || deletedFilesLenInBytesInLocal.get() != 0) {
            deleted =
                    deleted.union(
                            env.fromElements(
                                    new CleanOrphanFilesResult(
                                            deletedFilesCountInLocal.get(),
                                            deletedFilesLenInBytesInLocal.get())));
        }

        return deleted;
    }

    public static CleanOrphanFilesResult executeDatabaseOrphanFiles(
            StreamExecutionEnvironment env,
            Catalog catalog,
            long olderThanMillis,
            SerializableConsumer<Path> fileCleaner,
            @Nullable Integer parallelism,
            String databaseName,
            @Nullable String tableName)
            throws Catalog.DatabaseNotExistException, Catalog.TableNotExistException {
        List<String> tableNames = Collections.singletonList(tableName);
        if (tableName == null || "*".equals(tableName)) {
            tableNames = catalog.listTables(databaseName);
        }

        List<DataStream<CleanOrphanFilesResult>> orphanFilesCleans =
                new ArrayList<>(tableNames.size());
        for (String t : tableNames) {
            Identifier identifier = new Identifier(databaseName, t);
            Table table = catalog.getTable(identifier);
            checkArgument(
                    table instanceof FileStoreTable,
                    "Only FileStoreTable supports remove-orphan-files action. The table type is '%s'.",
                    table.getClass().getName());

            DataStream<CleanOrphanFilesResult> clean =
                    new FlinkOrphanFilesClean(
                                    (FileStoreTable) table,
                                    olderThanMillis,
                                    fileCleaner,
                                    parallelism)
                            .doOrphanClean(env);
            if (clean != null) {
                orphanFilesCleans.add(clean);
            }
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

    private static CleanOrphanFilesResult sum(DataStream<CleanOrphanFilesResult> deleted) {
        long deletedFilesCount = 0;
        long deletedFilesLenInBytes = 0;
        if (deleted != null) {
            try {
                CloseableIterator<CleanOrphanFilesResult> iterator =
                        deleted.global().executeAndCollect("OrphanFilesClean");
                while (iterator.hasNext()) {
                    CleanOrphanFilesResult cleanOrphanFilesResult = iterator.next();
                    deletedFilesCount += cleanOrphanFilesResult.getDeletedFileCount();
                    deletedFilesLenInBytes +=
                            cleanOrphanFilesResult.getDeletedFileTotalLenInBytes();
                }
                iterator.close();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        return new CleanOrphanFilesResult(deletedFilesCount, deletedFilesLenInBytes);
    }
}
