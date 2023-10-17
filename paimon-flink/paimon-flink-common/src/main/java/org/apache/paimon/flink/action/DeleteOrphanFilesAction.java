/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.flink.action;

import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.FileStatus;
import org.apache.paimon.fs.Path;
import org.apache.paimon.table.DataTable;

import org.apache.paimon.shade.guava30.com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * The action used to delete the orphan data file and metadata file, it's list the path of the
 * table's location, and compare to the result of system table, the files not included in the system
 * table will be deleted.
 *
 * <p>By default, we delete the orphan data that are older than 3 days, we can modify it by {@link
 * #olderThan(long)}, this action is not transactional, we must set this {@link #olderThan(long)} to
 * be greater than the expiration time of the snapshot to prevent deleting some files that are in
 * use but not committed.
 *
 * <p>Some idea refer to Iceberg's DeleteOrphanFilesSparkAction.
 */
public class DeleteOrphanFilesAction extends TableActionBase {

    private static final Logger LOGGER = LoggerFactory.getLogger(DeleteOrphanFilesAction.class);
    private static final int MAX_JOBMANAGER_LISTING_DEPTH = 3;
    private static final int MAX_JOBMANAGER_LISTING_DIRECT_SUB_DIRS = 10;
    private static final int MAX_TASKMANAGER_LISTING_DEPTH = 2000;
    private static final int MAX_TASKMANAGER_LISTING_DIRECT_SUB_DIRS = Integer.MAX_VALUE;

    private Consumer<String> deleteFunc = null;
    private String location;
    private long olderThan = System.currentTimeMillis() - TimeUnit.DAYS.toMillis(3);
    private int maxConcurrentDeletes;

    private FileIO fileIO;

    public DeleteOrphanFilesAction(
            String warehouse,
            String databaseName,
            String tableName,
            Map<String, String> catalogConfig) {
        super(warehouse, databaseName, tableName, catalogConfig);
    }

    public DeleteOrphanFilesAction olderThan(long olderThan) {
        this.olderThan = olderThan;
        return this;
    }

    public DeleteOrphanFilesAction deleteWith(Consumer<String> deleteFunc) {
        this.deleteFunc = deleteFunc;
        return this;
    }

    public DeleteOrphanFilesAction location(String location) {
        this.location = location;
        return this;
    }

    public DeleteOrphanFilesAction maxConcurrentDeletes(int maxConcurrentDeletes) {
        this.maxConcurrentDeletes = maxConcurrentDeletes;
        return this;
    }

    @Override
    public void run() throws Exception {
        TypeInformation<?>[] typeInformations =
                new TypeInformation[] {TypeInformation.of(String.class)};
        String[] fieldNames = new String[] {"name"};
        RowTypeInfo rowTypeInfo = new RowTypeInfo(typeInformations, fieldNames);
        List<Row> validFiles = getValidFiles();
        DataStream<Row> validDataStream = env.fromCollection(validFiles).returns(rowTypeInfo);

        DataTable dataTable = (DataTable) table;
        fileIO = dataTable.fileIO();

        typeInformations =
                new TypeInformation[] {
                    TypeInformation.of(String.class), TypeInformation.of(String.class)
                };
        fieldNames = new String[] {"path", "name"};
        rowTypeInfo = new RowTypeInfo(typeInformations, fieldNames);

        List<Row> matchingFiles = new ArrayList<>();

        List<Path> subDirs;
        try {
            subDirs = getAllFiles(fileIO, dataTable.location(), location, matchingFiles);
        } catch (Exception e) {
            LOGGER.error("get all orphan files failed,", e);
            return;
        }
        if (matchingFiles.isEmpty() && subDirs.isEmpty()) {
            LOGGER.info("There is no orphan files");
            return;
        }

        DataStream<Row> matchedDataStream = env.fromCollection(matchingFiles).returns(rowTypeInfo);

        if (!subDirs.isEmpty()) {
            DataStream<Row> allDataStream =
                    env.fromCollection(subDirs)
                            .flatMap(new ListDirsRecursively(olderThan, fileIO))
                            .returns(rowTypeInfo);
            matchedDataStream = allDataStream.union(matchedDataStream);
        }

        batchTEnv.createTemporaryView("allFiles", matchedDataStream);
        batchTEnv.createTemporaryView("validFiles", validDataStream);

        TableResult result =
                batchTEnv.executeSql(
                        "select fullPath from ( select allFiles.path as fullPath , validFiles.`name` as fileName from allFiles left join validFiles on allFiles.name = validFiles.name ) where fileName is null ");
        CloseableIterator<Row> iterator = result.collect();
        List<String> orphanFiles = new ArrayList<>();
        while (iterator.hasNext()) {
            orphanFiles.add(iterator.next().getFieldAs(0));
        }

        if (deleteFunc == null) {
            doDelete(this::deleteOrphanFiles, orphanFiles, maxConcurrentDeletes);
        } else {
            doDelete(deleteFunc, orphanFiles, maxConcurrentDeletes);
        }
    }

    private void doDelete(Consumer<String> consumer, List<String> files, int maxConcurrentDeletes) {
        if (maxConcurrentDeletes > 1) {
            ExecutorService executorService =
                    Executors.newFixedThreadPool(
                            maxConcurrentDeletes,
                            new ThreadFactoryBuilder()
                                    .setDaemon(true)
                                    .setNameFormat("delete-orphan-files-%d")
                                    .build());

            for (String file : files) {
                executorService.submit(() -> deleteOrphanFiles(file));
            }
        } else {
            for (String file : files) {
                consumer.accept(file);
            }
        }
    }

    private void deleteOrphanFiles(String orphanFile) {
        fileIO.deleteQuietly(new Path(orphanFile));
    }

    private List<Row> getValidFiles() {
        batchTEnv.useDatabase(identifier.getDatabaseName());
        List<Row> result = new ArrayList<>();

        // get all files
        TableResult fileResult =
                batchTEnv.executeSql(
                        String.format(
                                "SELECT file_path FROM `%s$files`", identifier.getObjectName()));
        Iterator<Row> iterator = fileResult.collect();
        while (iterator.hasNext()) {
            Row row = iterator.next();
            result.add(row);
        }

        // get all manifests
        TableResult manifestResult =
                batchTEnv.executeSql(
                        String.format(
                                "SELECT file_name FROM `%s$manifests`",
                                identifier.getObjectName()));
        Iterator<Row> iteratorManifest = manifestResult.collect();
        while (iteratorManifest.hasNext()) {
            Row row = iteratorManifest.next();
            result.add(row);
        }

        // get all manifests list
        TableResult manifestListResult =
                batchTEnv.executeSql(
                        String.format(
                                "SELECT base_manifest_list,delta_manifest_list,changelog_manifest_list FROM `%s$snapshots`",
                                identifier.getObjectName()));
        Iterator<Row> iteratorManifestList = manifestListResult.collect();
        while (iteratorManifestList.hasNext()) {
            Row row = iteratorManifestList.next();
            if (row.getField(0) != null) {
                result.add(Row.of(row.getField(0)));
            }
            if (row.getField(1) != null) {
                result.add(Row.of(row.getField(1)));
            }
            if (row.getField(2) != null) {
                result.add(Row.of(row.getField(2)));
            }
        }

        return result;
    }

    private List<Path> getAllFiles(
            FileIO fileIO, Path tableLocation, String location, List<Row> matchingFiles) {
        Path scanPath;

        if (location == null) {
            scanPath = tableLocation;
        } else {
            // the location should be sub dir of the table location.
            if (!location.contains(tableLocation.getPath())) {
                throw new RuntimeException("the location should be sub dir of the table location.");
            }

            if (!location.contains("=")
                    && !location.contains("bucket")
                    && !location.contains("manifest")) {
                throw new RuntimeException("the location should be data dir of manifest dir.");
            }

            scanPath = new Path(location);
        }

        try {
            FileStatus[] fileStatuses = fileIO.listStatus(scanPath);
            List<Path> conditionDir =
                    Arrays.stream(fileStatuses)
                            .filter(
                                    fileStatus -> {
                                        String path = fileStatus.getPath().getPath();
                                        // data dir and manifest dir
                                        return path.contains("=")
                                                || path.contains("bucket")
                                                || path.contains("manifest");
                                    })
                            .map(FileStatus::getPath)
                            .collect(Collectors.toList());

            List<Path> subDirs = new ArrayList<>();
            for (Path dir : conditionDir) {
                listDirRecursively(
                        fileIO,
                        matchingFiles,
                        dir,
                        MAX_JOBMANAGER_LISTING_DEPTH,
                        MAX_JOBMANAGER_LISTING_DIRECT_SUB_DIRS,
                        subDirs,
                        olderThan);
            }

            return subDirs;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static void listDirRecursively(
            FileIO fileIO,
            List<Row> matchingFiles,
            Path dir,
            int maxDepth,
            int maxDirectSubDirs,
            List<Path> remainingSubDirs,
            long olderThan)
            throws IOException {
        if (maxDepth <= 0) {
            remainingSubDirs.add(dir);
            return;
        }

        List<Path> subDirs = new ArrayList<>();
        FileStatus[] fileStatuses = fileIO.listStatus(dir);
        for (FileStatus fs : fileStatuses) {
            if (fs.isDir()) {
                subDirs.add(fs.getPath());
            } else if (fs.getModificationTime() < olderThan) {
                matchingFiles.add(Row.of(fs.getPath().getPath(), fs.getPath().getName()));
            }
        }

        if (subDirs.size() > maxDirectSubDirs) {
            remainingSubDirs.addAll(subDirs);
            return;
        }

        for (Path subDir : subDirs) {
            listDirRecursively(
                    fileIO,
                    matchingFiles,
                    subDir,
                    maxDepth - 1,
                    maxDirectSubDirs,
                    remainingSubDirs,
                    olderThan);
        }
    }

    private static class ListDirsRecursively implements FlatMapFunction<Path, Row> {
        private final long olderThanTimestamp;
        private final FileIO fileIO;

        private ListDirsRecursively(long olderThanTimestamp, FileIO fileIO) {
            this.olderThanTimestamp = olderThanTimestamp;
            this.fileIO = fileIO;
        }

        @Override
        public void flatMap(Path dir, Collector<Row> out) throws Exception {
            List<Path> subDirs = new ArrayList<>();
            List<Row> matchingFiles = new ArrayList<>();

            listDirRecursively(
                    fileIO,
                    matchingFiles,
                    dir,
                    MAX_TASKMANAGER_LISTING_DEPTH,
                    MAX_TASKMANAGER_LISTING_DIRECT_SUB_DIRS,
                    subDirs,
                    olderThanTimestamp);

            matchingFiles.forEach(out::collect);
        }
    }
}
