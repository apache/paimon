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

import org.apache.paimon.Snapshot;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.fs.Path;
import org.apache.paimon.globalindex.GlobalIndexSchemaCompatibility;
import org.apache.paimon.index.IndexFileHandler;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.index.IndexFileMetaSerializer;
import org.apache.paimon.index.IndexPathFactory;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.utils.FileStorePathFactory;
import org.apache.paimon.utils.SerializationUtils;

import org.apache.spark.sql.SparkSession;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** List index files. */
public class ListIndexFilesOperator extends CopyFilesOperator {

    private final IndexFileMetaSerializer indexFileSerializer;

    public ListIndexFilesOperator(
            SparkSession spark, Catalog sourceCatalog, Catalog targetCatalog) {
        super(spark, sourceCatalog, targetCatalog);
        this.indexFileSerializer = new IndexFileMetaSerializer();
    }

    public List<CopyFileInfo> execute(
            Identifier sourceIdentifier,
            Identifier targetIdentifier,
            Snapshot snapshot,
            @Nullable PartitionPredicate partitionPredicate,
            List<CopyFileInfo> dataFiles)
            throws Exception {
        if (snapshot == null) {
            return null;
        }
        if (snapshot.indexManifest() == null) {
            return null;
        }
        FileStoreTable sourceTable = (FileStoreTable) sourceCatalog.getTable(sourceIdentifier);
        FileStoreTable targetTable = (FileStoreTable) targetCatalog.getTable(targetIdentifier);
        List<CopyFileInfo> indexFiles = new ArrayList<>();
        IndexFileHandler sourceIndexHandler = sourceTable.store().newIndexFileHandler();
        FileStorePathFactory sourceFileStorePathFactory = sourceTable.store().pathFactory();
        FileStorePathFactory targetFileStorePathFactory = targetTable.store().pathFactory();
        List<IndexManifestEntry> indexManifestEntries =
                sourceIndexHandler.readManifestWithIOException(snapshot.indexManifest());
        List<IndexManifestEntry> dataEvolutionIndexes = new ArrayList<>();
        for (IndexManifestEntry entry : indexManifestEntries) {
            if (isDataEvolutionIndex(sourceTable, entry)) {
                dataEvolutionIndexes.add(entry);
            }
        }
        Set<IndexManifestEntry> compatibleGlobalIndexes =
                new HashSet<>(
                        GlobalIndexSchemaCompatibility.filterCompatible(
                                sourceTable, dataEvolutionIndexes));
        Map<String, String> dataFileNameMapping = dataFileNameMapping(dataFiles);
        for (IndexManifestEntry indexManifestEntry : indexManifestEntries) {
            boolean dataEvolutionIndex = isDataEvolutionIndex(sourceTable, indexManifestEntry);
            if (dataEvolutionIndex && !compatibleGlobalIndexes.contains(indexManifestEntry)) {
                continue;
            }
            if (partitionPredicate == null
                    || partitionPredicate.test(indexManifestEntry.partition())) {
                CopyFileInfo indexFile =
                        pickIndexFiles(
                                indexManifestEntry,
                                sourceFileStorePathFactory,
                                targetFileStorePathFactory,
                                dataEvolutionIndex,
                                isPrimaryKeyPayload(sourceTable, indexManifestEntry),
                                dataEvolutionIndex ? targetTable.schema().id() : null,
                                dataFileNameMapping);
                indexFiles.add(indexFile);
            }
        }
        return indexFiles;
    }

    private CopyFileInfo pickIndexFiles(
            IndexManifestEntry indexManifestEntry,
            FileStorePathFactory sourceFileStorePathFactory,
            FileStorePathFactory targetFileStorePathFactory,
            boolean dataEvolutionIndex,
            boolean primaryKeyPayload,
            @Nullable Long targetSchemaId,
            Map<String, String> dataFileNameMapping)
            throws IOException {
        IndexFileMeta fileMeta = indexManifestEntry.indexFile();
        IndexPathFactory sourceIndexPathFactory =
                indexPathFactory(
                        sourceFileStorePathFactory, indexManifestEntry, dataEvolutionIndex);
        IndexPathFactory targetIndexPathFactory =
                indexPathFactory(
                        targetFileStorePathFactory, indexManifestEntry, dataEvolutionIndex);
        Path indexFilePath = sourceIndexPathFactory.toPath(fileMeta);
        Path targetIndexFilePath = targetIndexPathFactory.newPath();
        IndexFileMeta targetFileMeta =
                primaryKeyPayload
                        ? CopyFilesUtil.toNewPrimaryKeyIndexFileMeta(
                                fileMeta, targetIndexFilePath.getName(), dataFileNameMapping)
                        : CopyFilesUtil.toNewIndexFileMeta(
                                fileMeta, targetIndexFilePath.getName(), targetSchemaId);
        return new CopyFileInfo(
                indexFilePath.toString(),
                targetIndexFilePath.toString(),
                SerializationUtils.serializeBinaryRow(indexManifestEntry.partition()),
                indexManifestEntry.bucket(),
                indexFileSerializer.serializeToBytes(targetFileMeta));
    }

    private static IndexPathFactory indexPathFactory(
            FileStorePathFactory pathFactory,
            IndexManifestEntry entry,
            boolean dataEvolutionIndex) {
        return dataEvolutionIndex
                ? pathFactory.globalIndexFileFactory()
                : pathFactory.indexFileFactory(entry.partition(), entry.bucket());
    }

    private static boolean isDataEvolutionIndex(FileStoreTable table, IndexManifestEntry entry) {
        return table.coreOptions().dataEvolutionEnabled()
                && entry.indexFile().globalIndexMeta() != null;
    }

    private static boolean isPrimaryKeyPayload(FileStoreTable table, IndexManifestEntry entry) {
        return !table.schema().primaryKeys().isEmpty()
                && entry.indexFile().globalIndexMeta() != null
                && entry.indexFile().globalIndexMeta().sourceMeta() != null;
    }

    private static Map<String, String> dataFileNameMapping(List<CopyFileInfo> dataFiles) {
        Map<String, String> mapping = new HashMap<>();
        for (CopyFileInfo dataFile : dataFiles) {
            mapping.put(
                    new Path(dataFile.sourceFilePath()).getName(),
                    new Path(dataFile.targetFilePath()).getName());
        }
        return mapping;
    }
}
