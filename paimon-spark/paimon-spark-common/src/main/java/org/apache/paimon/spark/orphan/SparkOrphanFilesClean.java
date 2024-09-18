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

package org.apache.paimon.spark.orphan;

import org.apache.paimon.Snapshot;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.fs.FileStatus;
import org.apache.paimon.fs.Path;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestFile;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.operation.OrphanFilesClean;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.SerializableConsumer;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.util.LongAccumulator;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import scala.Tuple2;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Spark {@link OrphanFilesClean}, it will submit a job for a table. */
public class SparkOrphanFilesClean extends OrphanFilesClean {

    @Nullable protected final Integer parallelism;

    public SparkOrphanFilesClean(
            FileStoreTable table,
            long olderThanMillis,
            SerializableConsumer<Path> fileCleaner,
            @Nullable Integer parallelism) {
        super(table, olderThanMillis, fileCleaner);
        this.parallelism = parallelism;
    }

    public static long executeDatabaseOrphanFiles(
            JavaSparkContext ctx,
            Catalog catalog,
            long olderThanMillis,
            SerializableConsumer<Path> fileCleaner,
            @Nullable Integer parallelism,
            String databaseName,
            @Nullable String tableName)
            throws Catalog.DatabaseNotExistException, Catalog.TableNotExistException {
        Long cleanTotal = 0L;
        List<String> tableNames = Collections.singletonList(tableName);
        if (tableName == null || "*".equals(tableName)) {
            tableNames = catalog.listTables(databaseName);
        }

        for (String t : tableNames) {
            Identifier identifier = new Identifier(databaseName, t);
            Table table = catalog.getTable(identifier);
            checkArgument(
                    table instanceof FileStoreTable,
                    "Only FileStoreTable supports remove-orphan-files action. The table type is '%s'.",
                    table.getClass().getName());

            Long cleanNums =
                    new SparkOrphanFilesClean(
                                    (FileStoreTable) table,
                                    olderThanMillis,
                                    fileCleaner,
                                    parallelism)
                            .doOrphanClean(ctx);
            cleanTotal += cleanNums;
        }
        return cleanTotal;
    }

    @Nullable
    public Long doOrphanClean(JavaSparkContext ctx) {
        int parallelism = this.parallelism == null ? ctx.defaultParallelism() : this.parallelism;
        ctx.getConf().set("spark.default.parallelism", String.valueOf(parallelism));
        SparkContext sc = ctx.sc();

        List<String> branches = validBranches();

        // snapshot and changelog files are the root of everything, so they are handled specially
        // here, and subsequently, we will not count their orphan files.
        AtomicLong deletedInLocal = new AtomicLong(0);
        cleanSnapshotDir(branches, p -> deletedInLocal.incrementAndGet());

        LongAccumulator emitted = sc.longAccumulator();

        JavaRDD<Pair> repartitionRDD =
                ctx.parallelize(branches)
                        .mapPartitions(
                                iterator -> {
                                    ArrayList<Pair> pairs = new ArrayList<>();
                                    while (iterator.hasNext()) {
                                        String branch = iterator.next();
                                        for (Snapshot snapshot : safelyGetAllSnapshots(branch)) {
                                            pairs.add(Pair.of(branch, snapshot.toJson()));
                                        }
                                    }
                                    return pairs.iterator();
                                })
                        .repartition(parallelism);
        JavaRDD<Pair<String, String>> outputRdd =
                repartitionRDD.mapPartitions(
                        iterator -> {
                            List<Pair<String, String>> collect =
                                    new ArrayList<Pair<String, String>>();
                            while (iterator.hasNext()) {
                                Pair pair = iterator.next();
                                String branch = (String) pair.getLeft();

                                Snapshot snapshot = Snapshot.fromJson((String) pair.getRight());
                                Consumer<ManifestFileMeta> manifestConsumer =
                                        manifest -> {
                                            Pair<String, String> pair0 =
                                                    Pair.of(branch, manifest.fileName());
                                            collect.add(pair0);
                                        };
                                collectWithoutDataFile(branch, snapshot, null, manifestConsumer);
                            }
                            return collect.iterator();
                        });
        JavaRDD<String> usedManifestFiles =
                repartitionRDD.mapPartitions(
                        iterator -> {
                            List<String> collect = new ArrayList<String>();
                            while (iterator.hasNext()) {
                                Pair pair = iterator.next();
                                String branch = (String) pair.getLeft();

                                Snapshot snapshot = Snapshot.fromJson((String) pair.getRight());
                                collectWithoutDataFile(branch, snapshot, collect::add, null);
                            }
                            return collect.iterator();
                        });

        JavaRDD<String> usedFiles =
                outputRdd
                        .keyBy(pair -> pair.getLeft().toString() + ":" + pair.getRight().toString())
                        .mapPartitions(
                                iterator -> {
                                    Set<Pair<String, String>> manifests = new HashSet<>();
                                    while (iterator.hasNext()) {
                                        Tuple2<String, Pair<String, String>> value =
                                                iterator.next();
                                        manifests.add(value._2());
                                    }
                                    Map<String, ManifestFile> branchManifests = new HashMap<>();
                                    ArrayList<String> output = new ArrayList<>();
                                    for (Pair<String, String> pair : manifests) {
                                        ManifestFile manifestFile =
                                                branchManifests.computeIfAbsent(
                                                        pair.getLeft(),
                                                        key ->
                                                                table.switchToBranch(key)
                                                                        .store()
                                                                        .manifestFileFactory()
                                                                        .create());
                                        retryReadingFiles(
                                                        () ->
                                                                manifestFile.readWithIOException(
                                                                        pair.getRight()),
                                                        Collections.<ManifestEntry>emptyList())
                                                .forEach(
                                                        f -> {
                                                            List<String> files = new ArrayList<>();
                                                            files.add(f.fileName());
                                                            files.addAll(f.file().extraFiles());
                                                            files.forEach(file -> output.add(file));
                                                        });
                                    }
                                    return output.iterator();
                                });

        usedFiles = usedFiles.union(usedManifestFiles);

        List<String> fileDirs =
                listPaimonFileDirs().stream()
                        .map(Path::toUri)
                        .map(Object::toString)
                        .collect(Collectors.toList());

        JavaRDD<String> candidates =
                ctx.parallelize(fileDirs)
                        .mapPartitions(
                                iterator -> {
                                    ArrayList<String> output = new ArrayList<>();
                                    while (iterator.hasNext()) {
                                        String dir = iterator.next();
                                        for (FileStatus fileStatus :
                                                tryBestListingDirs(new Path(dir))) {
                                            if (oldEnough(fileStatus)) {
                                                output.add(fileStatus.getPath().toUri().toString());
                                            }
                                        }
                                    }
                                    return output.iterator();
                                });
        JavaPairRDD<String, Object> resultRdd =
                usedFiles
                        .keyBy(k -> k)
                        .cogroup(candidates.keyBy(path -> new Path(path).getName()))
                        .mapValues(
                                tuple -> {
                                    Set<String> used = new HashSet<>();
                                    Iterator<String> usedStrs = tuple._1.iterator();
                                    Iterator<String> pathStrs = tuple._2.iterator();

                                    if (!usedStrs.hasNext()) {
                                        while (pathStrs.hasNext()) {
                                            String value = pathStrs.next();
                                            Path path = new Path(value);
                                            fileCleaner.accept(path);
                                            LOG.info("Dry clean: {}", path);
                                            emitted.add(1L);
                                        }
                                    }

                                    return null;
                                });

        // call action
        resultRdd.collect();
        return emitted.value() + deletedInLocal.get();
    }
}
