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

package org.apache.paimon.spark.copy;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.FileStore;
import org.apache.paimon.Snapshot;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.fs.Path;
import org.apache.paimon.index.IndexFileHandler;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.manifest.ManifestList;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.utils.Preconditions;
import org.apache.paimon.utils.SnapshotManager;

import org.apache.paimon.shade.guava30.com.google.common.collect.ImmutableList;
import org.apache.paimon.shade.guava30.com.google.common.collect.ImmutableMap;
import org.apache.paimon.shade.guava30.com.google.common.collect.Iterables;

import org.apache.spark.sql.SparkSession;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/** Copy schema from source to target. */
public class CopySchemaOperator extends CopyFilesOperator {

    public static final String INDEX_MANIFEST_FILES_TAG = "index-manifest-files";
    public static final String DATA_MANIFEST_FILES_TAG = "data-manifest-files";

    public CopySchemaOperator(SparkSession spark, Catalog sourceCatalog, Catalog targetCatalog) {
        super(spark, sourceCatalog, targetCatalog);
    }

    public Map<String, List<CopyFileInfo>> execute(
            Identifier sourceIdentifier, Identifier targetIdentifier) throws Exception {
        Table originalSourceTable = sourceCatalog.getTable(sourceIdentifier);
        Preconditions.checkState(
                originalSourceTable instanceof FileStoreTable,
                String.format(
                        "Only support copy FileStoreTable, but this table %s is %s.",
                        sourceIdentifier, sourceIdentifier.getClass()));
        FileStoreTable sourceTable = (FileStoreTable) originalSourceTable;

        // 1. create target table
        targetCatalog.createDatabase(targetIdentifier.getDatabaseName(), true);
        targetCatalog.createTable(
                targetIdentifier, newSchemaFromTableSchema(sourceTable.schema()), false);
        FileStoreTable targetTable = (FileStoreTable) targetCatalog.getTable(targetIdentifier);

        // 2. get latest snapshot files
        Map<String, List<CopyFileInfo>> result = new HashMap<>();

        FileStore<?> sourceStore = sourceTable.store();
        SnapshotManager sourceSnapshotManager = sourceStore.snapshotManager();
        Snapshot latestSnapshot = sourceSnapshotManager.latestSnapshot();
        if (latestSnapshot == null) {
            return result;
        }

        // 3. pick data manifest files
        List<Path> dataManifestFilePathList = pickDataManifestFiles(sourceStore, latestSnapshot);
        List<CopyFileInfo> dataManifestFiles =
                CopyFilesUtil.toCopyFileInfos(dataManifestFilePathList, sourceTable.location());
        result.put(DATA_MANIFEST_FILES_TAG, dataManifestFiles);

        // 4. pick index manifest files
        Path indexManifestFilePath = pickIndexManifestFiles(sourceStore, latestSnapshot);
        if (indexManifestFilePath != null) {
            List<CopyFileInfo> indexManifestFiles =
                    CopyFilesUtil.toCopyFileInfos(
                            Arrays.asList(indexManifestFilePath), sourceTable.location());
            result.put(INDEX_MANIFEST_FILES_TAG, indexManifestFiles);
        }
        return result;
    }

    private static Schema newSchemaFromTableSchema(TableSchema tableSchema) {
        return new Schema(
                ImmutableList.copyOf(tableSchema.fields()),
                ImmutableList.copyOf(tableSchema.partitionKeys()),
                ImmutableList.copyOf(tableSchema.primaryKeys()),
                ImmutableMap.copyOf(
                        Iterables.filter(
                                tableSchema.options().entrySet(),
                                entry -> !Objects.equals(entry.getKey(), CoreOptions.PATH.key()))),
                tableSchema.comment());
    }

    private List<Path> pickDataManifestFiles(FileStore<?> store, Snapshot snapshot) {
        List<Path> result = new ArrayList<>();
        ManifestList manifestList = store.manifestListFactory().create();
        List<ManifestFileMeta> manifestFileMetas = manifestList.readAllManifests(snapshot);
        List<String> manifestFileName =
                manifestFileMetas.stream()
                        .map(ManifestFileMeta::fileName)
                        .collect(Collectors.toList());
        result.addAll(
                manifestFileName.stream()
                        .map(store.pathFactory()::toManifestFilePath)
                        .collect(Collectors.toList()));
        return result;
    }

    @Nullable
    private Path pickIndexManifestFiles(FileStore<?> store, Snapshot snapshot) {
        IndexFileHandler indexFileHandler = store.newIndexFileHandler();
        String indexManifest = snapshot.indexManifest();
        Path indexManifestPath = null;
        if (indexManifest != null && indexFileHandler.existsManifest(indexManifest)) {
            indexManifestPath = indexFileHandler.indexManifestFilePath(indexManifest);
        }
        return indexManifestPath;
    }
}
