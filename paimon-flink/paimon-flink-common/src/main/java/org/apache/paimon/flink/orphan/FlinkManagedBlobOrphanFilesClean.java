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
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.manifest.ManifestList;
import org.apache.paimon.operation.CleanOrphanFilesResult;
import org.apache.paimon.operation.ManagedBlobOrphanFilesClean;
import org.apache.paimon.operation.ManagedBlobOrphanFilesClean.SidecarWorkItem;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.utils.DataFilePathFactories;
import org.apache.paimon.utils.FileStorePathFactory;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.apache.flink.streaming.api.operators.InputSelection;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;

import static org.apache.flink.api.common.typeinfo.BasicTypeInfo.STRING_TYPE_INFO;
import static org.apache.flink.util.Preconditions.checkState;
import static org.apache.paimon.utils.FileStorePathFactory.BUCKET_PATH_PREFIX;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Flink {@link ManagedBlobOrphanFilesClean}. */
public class FlinkManagedBlobOrphanFilesClean extends ManagedBlobOrphanFilesClean {

    private static final Logger LOG =
            LoggerFactory.getLogger(FlinkManagedBlobOrphanFilesClean.class);

    @Nullable private final Integer parallelism;

    public FlinkManagedBlobOrphanFilesClean(
            FileStoreTable table,
            long olderThanMillis,
            boolean dryRun,
            @Nullable Integer parallelism) {
        super(table, olderThanMillis, dryRun);
        validateParallelism(parallelism);
        this.parallelism = parallelism;
    }

    @Nullable
    public DataStream<CleanOrphanFilesResult> doClean(StreamExecutionEnvironment env) {
        List<String> topologyBefore;
        try {
            topologyBefore = snapshotTopology();
        } catch (java.io.IOException e) {
            throw new RuntimeException(e);
        }

        Configuration flinkConf = new Configuration();
        flinkConf.set(ExecutionOptions.RUNTIME_MODE, RuntimeExecutionMode.BATCH);
        flinkConf.set(ExecutionOptions.SORT_INPUTS, false);
        flinkConf.set(ExecutionOptions.USE_BATCH_STATE_BACKEND, false);
        if (parallelism != null) {
            flinkConf.set(CoreOptions.DEFAULT_PARALLELISM, parallelism);
        }
        flinkConf.setString("execution.batch.adaptive.auto-parallelism.enabled", "false");
        env.configure(flinkConf);

        List<String> branches = validBranches();
        final OutputTag<Boolean> firstMarkSkipGcTag =
                new OutputTag<Boolean>("first-managed-blob-mark-skip") {};
        SingleOutputStreamOperator<Tuple2<String, String>> firstManifestLists =
                env.fromCollection(branches)
                        .name("branch-source")
                        .process(
                                new ProcessFunction<String, Tuple2<String, String>>() {
                                    @Override
                                    public void processElement(
                                            String branch,
                                            ProcessFunction<String, Tuple2<String, String>>.Context
                                                    ctx,
                                            Collector<Tuple2<String, String>> out)
                                            throws Exception {
                                        emitManifestLists(branch, out::collect);
                                    }
                                })
                        .name("collect-first-mark-manifest-lists");

        SingleOutputStreamOperator<String> usedPacks =
                collectUsedPacks(firstManifestLists, firstMarkSkipGcTag, "first");

        DataStream<Boolean> firstMarkCompleted = markCompletion(usedPacks, "first");
        SingleOutputStreamOperator<Tuple2<String, String>> secondManifestLists =
                firstMarkCompleted
                        .transform(
                                "wait-before-second-managed-blob-mark",
                                Types.TUPLE(Types.STRING, Types.STRING),
                                new BoundedOneInputOperator<Boolean, Tuple2<String, String>>() {

                                    @Override
                                    public void processElement(StreamRecord<Boolean> element) {}

                                    @Override
                                    public void endInput() throws Exception {
                                        for (String branch : branches) {
                                            emitManifestLists(
                                                    branch,
                                                    manifestList ->
                                                            output.collect(
                                                                    new StreamRecord<>(
                                                                            manifestList)));
                                        }
                                    }
                                })
                        .forceNonParallel();

        final OutputTag<Boolean> secondMarkSkipGcTag =
                new OutputTag<Boolean>("second-managed-blob-mark-skip") {};
        SingleOutputStreamOperator<String> usedPacks2 =
                collectUsedPacks(secondManifestLists, secondMarkSkipGcTag, "second");

        DataStream<Boolean> usedPacksChanged = compareUsedPacks(usedPacks, usedPacks2);
        DataStream<Boolean> topologyChanged =
                markCompletion(usedPacks2, "second")
                        .transform(
                                "check-managed-blob-snapshot-topology",
                                TypeInformation.of(Boolean.class),
                                new BoundedOneInputOperator<Boolean, Boolean>() {

                                    @Override
                                    public void processElement(StreamRecord<Boolean> element) {}

                                    @Override
                                    public void endInput() throws Exception {
                                        List<String> topologyAfter = snapshotTopology();
                                        if (!topologyBefore.equals(topologyAfter)) {
                                            LOG.warn(
                                                    "Skip managed blob pack GC for table {} because snapshot topology changed during used-pack collection.",
                                                    table.fullName());
                                            output.collect(new StreamRecord<>(Boolean.TRUE));
                                        }
                                    }
                                })
                        .forceNonParallel();

        final OutputTag<Boolean> candidateSkipGcTag =
                new OutputTag<Boolean>("candidate-managed-blob-skip") {};
        SingleOutputStreamOperator<Tuple3<String, String, Long>> candidates =
                env.fromCollection(Collections.singletonList(1), TypeInformation.of(Integer.class))
                        .process(
                                new ProcessFunction<Integer, String>() {
                                    @Override
                                    public void processElement(
                                            Integer i,
                                            ProcessFunction<Integer, String>.Context ctx,
                                            Collector<String> out) {
                                        FileStorePathFactory pathFactory =
                                                table.store().pathFactory();
                                        listPaimonFileDirs(
                                                        table.fullName(),
                                                        pathFactory.manifestPath().toString(),
                                                        pathFactory.indexPath().toString(),
                                                        pathFactory.statisticsPath().toString(),
                                                        pathFactory.dataFilePath().toString(),
                                                        partitionKeysNum,
                                                        table.coreOptions().dataFileExternalPaths())
                                                .stream()
                                                .map(Path::toUri)
                                                .map(Object::toString)
                                                .forEach(out::collect);
                                    }
                                })
                        .name("list-dirs")
                        .forceNonParallel()
                        .process(
                                new ProcessFunction<String, Tuple3<String, String, Long>>() {
                                    @Override
                                    public void processElement(
                                            String dir,
                                            ProcessFunction<String, Tuple3<String, String, Long>>
                                                            .Context
                                                    ctx,
                                            Collector<Tuple3<String, String, Long>> out) {
                                        for (FileStatus file : tryBestListingDirs(new Path(dir))) {
                                            if (!file.isDir()
                                                    && oldEnough(file)
                                                    && isManagedBlobPackName(
                                                            file.getPath().getName())) {
                                                Optional<String> identity =
                                                        FlinkManagedBlobOrphanFilesClean.this
                                                                .packIdentityForCleanup(
                                                                        file.getPath());
                                                if (identity.isPresent()) {
                                                    out.collect(
                                                            Tuple3.of(
                                                                    identity.get(),
                                                                    file.getPath().toString(),
                                                                    file.getLen()));
                                                } else {
                                                    LOG.warn(
                                                            "Cannot safely identify candidate managed blob pack {}. Skip pack GC this run.",
                                                            file.getPath());
                                                    ctx.output(candidateSkipGcTag, Boolean.TRUE);
                                                }
                                            }
                                        }
                                    }
                                })
                        .name("collect-candidate-packs");

        final OutputTag<Tuple2<String, Long>> unusedPackTag =
                new OutputTag<Tuple2<String, Long>>("unused-managed-blob") {};

        SingleOutputStreamOperator<CleanOrphanFilesResult> unusedJoin =
                usedPacks2
                        .keyBy(identity -> identity)
                        .connect(candidates.keyBy(candidate -> candidate.f0))
                        .transform(
                                "join-used-and-candidate-packs",
                                TypeInformation.of(CleanOrphanFilesResult.class),
                                new BoundedTwoInputOperator<
                                        String,
                                        Tuple3<String, String, Long>,
                                        CleanOrphanFilesResult>() {

                                    private boolean buildEnd;
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
                                                buildEnd = true;
                                                break;
                                            case 2:
                                                checkState(buildEnd, "Should build ended.");
                                                output.collect(
                                                        new StreamRecord<>(
                                                                new CleanOrphanFilesResult(0, 0)));
                                                break;
                                        }
                                    }

                                    @Override
                                    public void processElement1(StreamRecord<String> element) {
                                        used.add(element.getValue());
                                    }

                                    @Override
                                    public void processElement2(
                                            StreamRecord<Tuple3<String, String, Long>> element) {
                                        checkState(buildEnd, "Should build ended.");
                                        Tuple3<String, String, Long> candidate = element.getValue();
                                        if (!used.contains(candidate.f0)) {
                                            output.collect(
                                                    unusedPackTag,
                                                    new StreamRecord<>(
                                                            Tuple2.of(candidate.f1, candidate.f2)));
                                        }
                                    }
                                });

        DataStream<Boolean> skipGc =
                usedPacks
                        .getSideOutput(firstMarkSkipGcTag)
                        .union(
                                usedPacks2.getSideOutput(secondMarkSkipGcTag),
                                usedPacksChanged,
                                topologyChanged,
                                candidates.getSideOutput(candidateSkipGcTag));

        final OutputTag<Path> emptyDirTag = new OutputTag<Path>("empty-managed-blob-dir") {};
        SingleOutputStreamOperator<CleanOrphanFilesResult> cleaned =
                unusedJoin
                        .getSideOutput(unusedPackTag)
                        .connect(skipGc.broadcast())
                        .transform(
                                "clean-unused-managed-blobs",
                                TypeInformation.of(CleanOrphanFilesResult.class),
                                new BoundedTwoInputOperator<
                                        Tuple2<String, Long>, Boolean, CleanOrphanFilesResult>() {

                                    private boolean skipEnded;
                                    private boolean skipGc;
                                    private long emittedFilesCount;
                                    private long emittedFilesLen;

                                    @Override
                                    public InputSelection nextSelection() {
                                        return skipEnded
                                                ? InputSelection.FIRST
                                                : InputSelection.SECOND;
                                    }

                                    @Override
                                    public void endInput(int inputId) {
                                        switch (inputId) {
                                            case 2:
                                                checkState(!skipEnded, "Should not skip ended.");
                                                skipEnded = true;
                                                LOG.info("Managed blob GC skip flag: {}", skipGc);
                                                break;
                                            case 1:
                                                checkState(skipEnded, "Should skip ended.");
                                                output.collect(
                                                        new StreamRecord<>(
                                                                new CleanOrphanFilesResult(
                                                                        emittedFilesCount,
                                                                        emittedFilesLen)));
                                                break;
                                        }
                                    }

                                    @Override
                                    public void processElement1(
                                            StreamRecord<Tuple2<String, Long>> element) {
                                        checkState(skipEnded, "Should skip ended.");
                                        if (skipGc) {
                                            return;
                                        }
                                        Tuple2<String, Long> fileInfo = element.getValue();
                                        Path path = new Path(fileInfo.f0);
                                        if (cleanPack(path)) {
                                            emittedFilesCount++;
                                            emittedFilesLen += fileInfo.f1;
                                            Path parent = path.getParent();
                                            if (parent != null
                                                    && parent.toString()
                                                            .contains(BUCKET_PATH_PREFIX)) {
                                                output.collect(
                                                        emptyDirTag, new StreamRecord<>(parent));
                                            }
                                            LOG.info("Cleaned managed blob pack: {}", path);
                                        }
                                    }

                                    @Override
                                    public void processElement2(StreamRecord<Boolean> element) {
                                        skipGc = true;
                                    }
                                });

        cleaned.getSideOutput(emptyDirTag)
                .transform(
                        "clean-empty-dirs",
                        STRING_TYPE_INFO,
                        new BoundedOneInputOperator<Path, String>() {

                            private final Set<Path> bucketDirs = new HashSet<>();

                            @Override
                            public void processElement(StreamRecord<Path> element) {
                                bucketDirs.add(element.getValue());
                            }

                            @Override
                            public void endInput() {
                                tryCleanDataDirectory(bucketDirs, partitionKeysNum + 1);
                            }
                        })
                .forceNonParallel()
                .sinkTo(new DiscardingSink<>())
                .name("end")
                .setParallelism(1)
                .setMaxParallelism(1);

        return cleaned;
    }

    private SingleOutputStreamOperator<String> collectUsedPacks(
            DataStream<Tuple2<String, String>> manifestLists,
            OutputTag<Boolean> skipGcTag,
            String markName) {
        SingleOutputStreamOperator<Tuple2<String, String>> distinctManifestLists =
                deduplicateNamedFiles(manifestLists, markName + "-managed-blob-manifest-lists");

        SingleOutputStreamOperator<Tuple2<String, String>> manifests =
                distinctManifestLists
                        .process(
                                new ProcessFunction<
                                        Tuple2<String, String>, Tuple2<String, String>>() {

                                    private transient Map<String, ManifestList> readers;

                                    // @Override is skipped for compatibility between Flink versions
                                    public void open(OpenContext openContext) {
                                        open(new Configuration());
                                    }

                                    // @Override is skipped for compatibility between Flink versions
                                    public void open(Configuration parameters) {
                                        readers = new HashMap<>();
                                    }

                                    @Override
                                    public void processElement(
                                            Tuple2<String, String> branchAndList,
                                            ProcessFunction<
                                                                    Tuple2<String, String>,
                                                                    Tuple2<String, String>>
                                                            .Context
                                                    ctx,
                                            Collector<Tuple2<String, String>> out)
                                            throws Exception {
                                        String branch = branchAndList.f0;
                                        ManifestList reader = readers.get(branch);
                                        if (reader == null) {
                                            reader =
                                                    table.switchToBranch(branch)
                                                            .store()
                                                            .manifestListFactory()
                                                            .create();
                                            readers.put(branch, reader);
                                        }
                                        final ManifestList manifestListReader = reader;
                                        List<ManifestFileMeta> listed =
                                                retryReadingFiles(
                                                        () ->
                                                                manifestListReader
                                                                        .readWithIOException(
                                                                                branchAndList.f1),
                                                        null);
                                        if (listed == null) {
                                            LOG.warn(
                                                    "Manifest list {} is missing while collecting used managed blob packs. Skip pack GC this run.",
                                                    branchAndList.f1);
                                            out.collect(Tuple2.of(branch, SKIP_MANAGED_BLOB_GC));
                                            return;
                                        }
                                        for (ManifestFileMeta meta : listed) {
                                            out.collect(Tuple2.of(branch, meta.fileName()));
                                        }
                                    }
                                })
                        .name("read-" + markName + "-managed-blob-manifest-lists");
        if (parallelism != null) {
            manifests.setParallelism(parallelism);
        }

        SingleOutputStreamOperator<Tuple2<String, String>> distinctManifests =
                deduplicateNamedFiles(manifests, markName + "-managed-blob-manifests");
        final OutputTag<String> manifestSkipGcTag =
                new OutputTag<String>(markName + "-managed-blob-manifest-skip") {};
        SingleOutputStreamOperator<SidecarWorkItem> sidecars =
                distinctManifests
                        .process(
                                new ProcessFunction<Tuple2<String, String>, SidecarWorkItem>() {

                                    private transient Map<String, ManifestFile> manifestFiles;
                                    private transient Map<String, DataFilePathFactories>
                                            pathFactories;

                                    // @Override is skipped for compatibility between Flink versions
                                    public void open(OpenContext openContext) {
                                        open(new Configuration());
                                    }

                                    // @Override is skipped for compatibility between Flink versions
                                    public void open(Configuration parameters) {
                                        manifestFiles = new HashMap<>();
                                        pathFactories = new HashMap<>();
                                    }

                                    @Override
                                    public void processElement(
                                            Tuple2<String, String> branchAndManifest,
                                            ProcessFunction<Tuple2<String, String>, SidecarWorkItem>
                                                            .Context
                                                    ctx,
                                            Collector<SidecarWorkItem> out)
                                            throws Exception {
                                        if (SKIP_MANAGED_BLOB_GC.equals(branchAndManifest.f1)) {
                                            ctx.output(manifestSkipGcTag, SKIP_MANAGED_BLOB_GC);
                                            return;
                                        }

                                        String branch = branchAndManifest.f0;
                                        ManifestFile manifestFile = manifestFiles.get(branch);
                                        DataFilePathFactories factories = pathFactories.get(branch);
                                        if (manifestFile == null) {
                                            FileStoreTable branchTable =
                                                    table.switchToBranch(branch);
                                            manifestFile =
                                                    branchTable
                                                            .store()
                                                            .manifestFileFactory()
                                                            .create();
                                            factories =
                                                    new DataFilePathFactories(
                                                            branchTable.store().pathFactory());
                                            manifestFiles.put(branch, manifestFile);
                                            pathFactories.put(branch, factories);
                                        }

                                        final ManifestFile manifestReader = manifestFile;
                                        List<ManifestEntry> entries =
                                                retryReadingFiles(
                                                        () ->
                                                                manifestReader.readWithIOException(
                                                                        branchAndManifest.f1),
                                                        null);
                                        if (entries == null) {
                                            LOG.warn(
                                                    "Manifest {} is missing while collecting used managed blob packs. Skip pack GC this run.",
                                                    branchAndManifest.f1);
                                            ctx.output(manifestSkipGcTag, SKIP_MANAGED_BLOB_GC);
                                            return;
                                        }
                                        for (ManifestEntry entry : entries) {
                                            for (SidecarWorkItem workItem :
                                                    FlinkManagedBlobOrphanFilesClean.this
                                                            .createSidecarWorkItems(
                                                                    entry,
                                                                    factories.get(
                                                                            entry.partition(),
                                                                            entry.bucket()))) {
                                                out.collect(workItem);
                                            }
                                        }
                                    }
                                })
                        .name("collect-" + markName + "-managed-blob-sidecars")
                        .returns(TypeInformation.of(SidecarWorkItem.class));
        if (parallelism != null) {
            sidecars.setParallelism(parallelism);
        }

        SingleOutputStreamOperator<SidecarWorkItem> distinctSidecars =
                sidecars.keyBy(SidecarWorkItem::dedupIdentity)
                        .transform(
                                "deduplicate-" + markName + "-managed-blob-sidecars",
                                TypeInformation.of(SidecarWorkItem.class),
                                new BoundedOneInputOperator<SidecarWorkItem, SidecarWorkItem>() {

                                    private final Set<String> identities = new HashSet<>();

                                    @Override
                                    public void processElement(
                                            StreamRecord<SidecarWorkItem> element) {
                                        if (identities.add(element.getValue().dedupIdentity())) {
                                            output.collect(element);
                                        }
                                    }

                                    @Override
                                    public void endInput() {}
                                });
        if (parallelism != null) {
            distinctSidecars.setParallelism(parallelism);
        }

        SingleOutputStreamOperator<String> marked =
                distinctSidecars
                        .process(
                                new ProcessFunction<SidecarWorkItem, String>() {

                                    private transient ManagedBlobOrphanFilesClean.ReachabilityScan
                                            scan;

                                    // @Override is skipped for compatibility between Flink versions
                                    public void open(OpenContext openContext) {
                                        open(new Configuration());
                                    }

                                    // @Override is skipped for compatibility between Flink versions
                                    public void open(Configuration parameters) {
                                        scan =
                                                FlinkManagedBlobOrphanFilesClean.this
                                                        .newReachabilityScan();
                                    }

                                    @Override
                                    public void processElement(
                                            SidecarWorkItem workItem,
                                            ProcessFunction<SidecarWorkItem, String>.Context ctx,
                                            Collector<String> out) {
                                        FlinkManagedBlobOrphanFilesClean.this.emitUsedPacks(
                                                workItem, scan, out::collect);
                                    }
                                })
                        .name(markName + "-managed-blob-mark")
                        .returns(STRING_TYPE_INFO);
        if (parallelism != null) {
            marked.setParallelism(parallelism);
        }

        DataStream<String> markedAndManifestSkips =
                marked.union(sidecars.getSideOutput(manifestSkipGcTag));
        return markedAndManifestSkips
                .keyBy(identity -> identity)
                .transform(
                        "deduplicate-" + markName + "-managed-blob-mark",
                        STRING_TYPE_INFO,
                        new BoundedOneInputOperator<String, String>() {

                            private final Set<String> identities = new HashSet<>();

                            @Override
                            public void processElement(StreamRecord<String> element) {
                                identities.add(element.getValue());
                            }

                            @Override
                            public void endInput() {
                                for (String identity : identities) {
                                    if (SKIP_MANAGED_BLOB_GC.equals(identity)) {
                                        output.collect(skipGcTag, new StreamRecord<>(Boolean.TRUE));
                                    } else {
                                        output.collect(new StreamRecord<>(identity));
                                    }
                                }
                            }
                        });
    }

    private SingleOutputStreamOperator<Tuple2<String, String>> deduplicateNamedFiles(
            DataStream<Tuple2<String, String>> files, String operatorName) {
        SingleOutputStreamOperator<Tuple2<String, String>> deduplicated =
                files.keyBy(file -> namedFileIdentity(file.f0, file.f1))
                        .transform(
                                "deduplicate-" + operatorName,
                                Types.TUPLE(Types.STRING, Types.STRING),
                                new BoundedOneInputOperator<
                                        Tuple2<String, String>, Tuple2<String, String>>() {

                                    private final Set<String> identities = new HashSet<>();

                                    @Override
                                    public void processElement(
                                            StreamRecord<Tuple2<String, String>> element) {
                                        Tuple2<String, String> file = element.getValue();
                                        if (identities.add(namedFileIdentity(file.f0, file.f1))) {
                                            output.collect(
                                                    new StreamRecord<>(
                                                            Tuple2.of(file.f0, file.f1)));
                                        }
                                    }

                                    @Override
                                    public void endInput() {}
                                });
        if (parallelism != null) {
            deduplicated.setParallelism(parallelism);
        }
        return deduplicated;
    }

    private static String namedFileIdentity(String branch, String fileName) {
        return branch + '\0' + fileName;
    }

    private void emitManifestLists(String branch, Consumer<Tuple2<String, String>> manifestLists)
            throws Exception {
        for (Snapshot snapshot : safelyGetAllSnapshots(branch)) {
            emitManifestList(branch, snapshot.changelogManifestList(), manifestLists);
            emitManifestList(branch, snapshot.deltaManifestList(), manifestLists);
            emitManifestList(branch, snapshot.baseManifestList(), manifestLists);
        }
    }

    private static void emitManifestList(
            String branch,
            @Nullable String manifestList,
            Consumer<Tuple2<String, String>> manifestLists) {
        if (manifestList != null) {
            manifestLists.accept(Tuple2.of(branch, manifestList));
        }
    }

    private DataStream<Boolean> markCompletion(DataStream<String> usedPacks, String markName) {
        return usedPacks.transform(
                markName + "-managed-blob-mark-completion",
                TypeInformation.of(Boolean.class),
                new BoundedOneInputOperator<String, Boolean>() {
                    @Override
                    public void processElement(StreamRecord<String> element) {}

                    @Override
                    public void endInput() {
                        output.collect(new StreamRecord<>(Boolean.TRUE));
                    }
                });
    }

    private DataStream<Boolean> compareUsedPacks(
            DataStream<String> usedPacks, DataStream<String> usedPacks2) {
        return usedPacks
                .keyBy(identity -> identity)
                .connect(usedPacks2.keyBy(identity -> identity))
                .transform(
                        "compare-managed-blob-marks",
                        TypeInformation.of(Boolean.class),
                        new BoundedTwoInputOperator<String, String, Boolean>() {

                            private boolean firstMarkEnded;
                            private boolean marksDiffer;
                            private final Set<String> firstMark = new HashSet<>();

                            @Override
                            public InputSelection nextSelection() {
                                return firstMarkEnded
                                        ? InputSelection.SECOND
                                        : InputSelection.FIRST;
                            }

                            @Override
                            public void endInput(int inputId) {
                                switch (inputId) {
                                    case 1:
                                        checkState(
                                                !firstMarkEnded,
                                                "Should not have finished the first mark.");
                                        firstMarkEnded = true;
                                        break;
                                    case 2:
                                        checkState(
                                                firstMarkEnded,
                                                "Should have finished the first mark.");
                                        if (marksDiffer || !firstMark.isEmpty()) {
                                            LOG.warn(
                                                    "Skip managed blob pack GC for table {} because the used pack set changed during used-pack collection.",
                                                    table.fullName());
                                            output.collect(new StreamRecord<>(Boolean.TRUE));
                                        }
                                        break;
                                }
                            }

                            @Override
                            public void processElement1(StreamRecord<String> element) {
                                firstMark.add(element.getValue());
                            }

                            @Override
                            public void processElement2(StreamRecord<String> element) {
                                checkState(firstMarkEnded, "Should have finished the first mark.");
                                if (!firstMark.remove(element.getValue())) {
                                    marksDiffer = true;
                                }
                            }
                        });
    }

    private boolean cleanPack(Path path) {
        return cleanManagedBlobFile(path);
    }

    public static CleanOrphanFilesResult executeDatabase(
            StreamExecutionEnvironment env,
            Catalog catalog,
            long olderThanMillis,
            boolean dryRun,
            @Nullable Integer parallelism,
            String databaseName,
            @Nullable String tableName)
            throws Catalog.DatabaseNotExistException, Catalog.TableNotExistException {
        validateParallelism(parallelism);
        List<String> tableNames = Collections.singletonList(tableName);
        if (tableName == null || "*".equals(tableName)) {
            tableNames = catalog.listTables(databaseName);
        }

        List<DataStream<CleanOrphanFilesResult>> cleans = new ArrayList<>(tableNames.size());
        for (String t : tableNames) {
            Identifier identifier = new Identifier(databaseName, t);
            Table table = catalog.getTable(identifier);
            checkArgument(
                    table instanceof FileStoreTable,
                    "Only FileStoreTable supports remove-orphan-blobs action. The table type is '%s'.",
                    table.getClass().getName());
            DataStream<CleanOrphanFilesResult> clean =
                    new FlinkManagedBlobOrphanFilesClean(
                                    (FileStoreTable) table, olderThanMillis, dryRun, parallelism)
                            .doClean(env);
            if (clean != null) {
                cleans.add(clean);
            }
        }

        DataStream<CleanOrphanFilesResult> result = null;
        for (DataStream<CleanOrphanFilesResult> clean : cleans) {
            result = result == null ? clean : result.union(clean);
        }
        return sum(result);
    }

    private static CleanOrphanFilesResult sum(DataStream<CleanOrphanFilesResult> deleted) {
        long deletedFilesCount = 0;
        long deletedFilesLenInBytes = 0;
        if (deleted != null) {
            try (CloseableIterator<CleanOrphanFilesResult> iterator =
                    deleted.global().executeAndCollect("ManagedBlobOrphanFilesClean")) {
                while (iterator.hasNext()) {
                    CleanOrphanFilesResult cleanOrphanFilesResult = iterator.next();
                    deletedFilesCount += cleanOrphanFilesResult.getDeletedFileCount();
                    deletedFilesLenInBytes +=
                            cleanOrphanFilesResult.getDeletedFileTotalLenInBytes();
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        return new CleanOrphanFilesResult(deletedFilesCount, deletedFilesLenInBytes);
    }

    public static void validateParallelism(@Nullable Integer parallelism) {
        checkArgument(
                parallelism == null || parallelism > 0,
                "Parallelism must be greater than 0, but was %s.",
                parallelism);
    }
}
