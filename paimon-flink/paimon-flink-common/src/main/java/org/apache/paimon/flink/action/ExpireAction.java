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

package org.apache.paimon.flink.action;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.index.HashIndexFile;
import org.apache.paimon.index.IndexFileHandler;
import org.apache.paimon.manifest.IndexManifestFile;
import org.apache.paimon.manifest.ManifestFile;
import org.apache.paimon.manifest.ManifestList;
import org.apache.paimon.operation.FileStoreExpire;
import org.apache.paimon.operation.FileStoreExpireImpl;
import org.apache.paimon.operation.SnapshotDeletion;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.FileStorePathFactory;
import org.apache.paimon.utils.SnapshotManager;
import org.apache.paimon.utils.TagManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/** Expire action which provides snapshots expire. */
public class ExpireAction extends TableActionBase {

    private static final Logger LOG = LoggerFactory.getLogger(ExpireAction.class);

    private final int numRetainedMin;
    private final int numRetainedMax;
    private final long millisRetained;
    private final SnapshotManager snapshotManager;
    private final SnapshotDeletion snapshotDeletion;
    private final TagManager tagManager;
    private final FileStoreExpire fileStoreExpire;

    public ExpireAction(
            String warehouse,
            String databaseName,
            String tableName,
            int numRetainedMin,
            int numRetainedMax,
            long millisRetained,
            Map<String, String> catalogConfig) {
        super(warehouse, databaseName, tableName, catalogConfig);
        this.numRetainedMin = numRetainedMin;
        this.numRetainedMax = numRetainedMax;
        this.millisRetained = millisRetained;
        this.snapshotManager =
                new SnapshotManager(
                        ((FileStoreTable) table).fileIO(), ((FileStoreTable) table).location());
        this.snapshotDeletion = newSnapshotDeletion();
        this.tagManager = newTagManager();

        this.fileStoreExpire =
                new FileStoreExpireImpl(
                        numRetainedMin,
                        numRetainedMax,
                        millisRetained,
                        snapshotManager,
                        snapshotDeletion,
                        tagManager);
    }

    private SnapshotDeletion newSnapshotDeletion() {
        Path location = ((FileStoreTable) table).location();
        FileIO fileIO = ((FileStoreTable) table).fileIO();

        CoreOptions coreOptions = CoreOptions.fromMap(((FileStoreTable) table).options());
        SchemaManager schemaManager = new SchemaManager(fileIO, location);
        FileStorePathFactory fileStorePathFactory = new FileStorePathFactory(location);
        RowType partitionType = ((FileStoreTable) table).schema().logicalRowType();
        ManifestFile manifestFile =
                new ManifestFile.Factory(
                                fileIO,
                                schemaManager,
                                partitionType,
                                coreOptions.manifestFormat(),
                                fileStorePathFactory,
                                coreOptions.manifestTargetSize().getBytes(),
                                null)
                        .create();
        ManifestList manifestList =
                new ManifestList.Factory(
                                fileIO, coreOptions.manifestFormat(), fileStorePathFactory, null)
                        .create();
        IndexManifestFile indexManifestFile =
                new IndexManifestFile.Factory(
                                fileIO, coreOptions.manifestFormat(), fileStorePathFactory)
                        .create();
        IndexFileHandler indexFileHandler =
                new IndexFileHandler(
                        snapshotManager,
                        indexManifestFile,
                        new HashIndexFile(fileIO, fileStorePathFactory.indexFileFactory()));

        return new SnapshotDeletion(
                fileIO, fileStorePathFactory, manifestFile, manifestList, indexFileHandler);
    }

    private TagManager newTagManager() {
        return new TagManager(
                ((FileStoreTable) table).fileIO(),
                CoreOptions.fromMap(((FileStoreTable) table).options()).path());
    }

    @Override
    public void run() throws Exception {
        LOG.debug(
                "The minimum number of completed snapshots to be retained is {}, "
                        + "the maximum number is {}, and the maximum time of snapshots to be retained is {}",
                numRetainedMin,
                numRetainedMax,
                millisRetained);

        fileStoreExpire.expire();
    }
}
