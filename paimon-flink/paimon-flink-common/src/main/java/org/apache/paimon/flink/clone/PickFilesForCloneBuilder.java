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

package org.apache.paimon.flink.clone;

import org.apache.paimon.FileStore;
import org.apache.paimon.clone.CloneFileInfo;
import org.apache.paimon.clone.CloneTypeAction;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.index.IndexFileHandler;
import org.apache.paimon.manifest.ManifestFile;
import org.apache.paimon.manifest.ManifestList;
import org.apache.paimon.operation.PickFilesUtil;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.SnapshotManager;
import org.apache.paimon.utils.TagManager;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.paimon.utils.Preconditions.checkNotNull;
import static org.apache.paimon.utils.Preconditions.checkState;

/** Source builder to build a Flink batch job. This is for clone jobs. */
public class PickFilesForCloneBuilder {

    private final StreamExecutionEnvironment env;
    private final Path sourceTableRoot;
    private final int partitionNum;
    private final FileIO fileIO;
    private final CloneTypeAction cloneTypeAction;
    private final SnapshotManager snapshotManager;
    private final ManifestList manifestList;
    private final ManifestFile manifestFile;
    private final SchemaManager schemaManager;
    private final IndexFileHandler indexFileHandler;
    private final TagManager tagManager;

    public PickFilesForCloneBuilder(
            StreamExecutionEnvironment env,
            FileStoreTable sourceTable,
            CloneTypeAction cloneTypeAction) {
        this.env = env;
        this.sourceTableRoot = sourceTable.location();
        this.partitionNum = sourceTable.partitionKeys().size();
        this.fileIO = sourceTable.fileIO();
        this.cloneTypeAction = cloneTypeAction;
        FileStore<?> store = sourceTable.store();
        this.schemaManager = new SchemaManager(sourceTable.fileIO(), sourceTable.location());
        this.snapshotManager = store.snapshotManager();
        this.manifestList = store.manifestListFactory().create();
        this.manifestFile = store.manifestFileFactory().create();
        this.indexFileHandler = store.newIndexFileHandler();
        this.tagManager = store.newTagManager();
    }

    /**
     * Get all files of a table.
     *
     * @return fileName -> (relative path, file size)
     */
    private Map<String, Pair<Path, Long>> getTableAllFilesExcludeTableRoot() {
        Map<String, Pair<Path, Long>> tableAllFiles =
                PickFilesUtil.getTableAllFiles(sourceTableRoot, partitionNum, fileIO);

        return tableAllFiles.entrySet().stream()
                .collect(
                        Collectors.toMap(
                                Map.Entry::getKey,
                                e ->
                                        Pair.of(
                                                getPathExcludeTableRoot(e.getValue().getKey()),
                                                e.getValue().getRight())));
    }

    /**
     * Cut table root path of absolutePath.
     *
     * @param absolutePath
     * @return the relative path
     */
    private Path getPathExcludeTableRoot(Path absolutePath) {
        String fileAbsolutePath = absolutePath.toUri().getPath();
        String sourceTableRootPath = sourceTableRoot.toString();

        checkState(
                fileAbsolutePath.startsWith(sourceTableRootPath),
                "This is a bug, please report. fileAbsolutePath is : "
                        + fileAbsolutePath
                        + ", sourceTableRootPath is : "
                        + sourceTableRootPath);

        return new Path(fileAbsolutePath.substring(sourceTableRootPath.length()));
    }

    public Pair<DataStream<CloneFileInfo>, Integer> build() {
        checkNotNull(env, "StreamExecutionEnvironment should not be null.");

        List<CloneFileInfo> cloneFileInfos =
                cloneTypeAction.findCloneFilePaths(
                        getTableAllFilesExcludeTableRoot(),
                        snapshotManager,
                        manifestList,
                        manifestFile,
                        schemaManager,
                        indexFileHandler,
                        tagManager);

        return Pair.of(
                env.fromCollection(cloneFileInfos).setParallelism(1).rebalance(),
                cloneFileInfos.size());
    }
}
