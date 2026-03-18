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
import org.apache.paimon.index.IndexFileHandler;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.index.IndexFileMetaSerializer;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.utils.FileStorePathFactory;
import org.apache.paimon.utils.SerializationUtils;

import org.apache.spark.sql.SparkSession;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

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
            @Nullable PartitionPredicate partitionPredicate)
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
        FileStorePathFactory targetFileStorePathFactory = targetTable.store().pathFactory();
        List<IndexManifestEntry> indexManifestEntries =
                sourceIndexHandler.readManifestWithIOException(snapshot.indexManifest());
        for (IndexManifestEntry indexManifestEntry : indexManifestEntries) {
            if (partitionPredicate == null
                    || partitionPredicate.test(indexManifestEntry.partition())) {
                CopyFileInfo indexFile =
                        pickIndexFiles(
                                indexManifestEntry, sourceIndexHandler, targetFileStorePathFactory);
                indexFiles.add(indexFile);
            }
        }
        return indexFiles;
    }

    private CopyFileInfo pickIndexFiles(
            IndexManifestEntry indexManifestEntry,
            IndexFileHandler sourceIndexFileHandler,
            FileStorePathFactory targetFileStorePathFactory)
            throws IOException {
        Path indexFilePath = sourceIndexFileHandler.filePath(indexManifestEntry);
        Path targetIndexFilePath =
                targetFileStorePathFactory
                        .indexFileFactory(
                                indexManifestEntry.partition(), indexManifestEntry.bucket())
                        .newPath();
        IndexFileMeta fileMeta = indexManifestEntry.indexFile();
        IndexFileMeta targetFileMeta =
                CopyFilesUtil.toNewIndexFileMeta(fileMeta, targetIndexFilePath.getName());
        return new CopyFileInfo(
                indexFilePath.toString(),
                targetIndexFilePath.toString(),
                SerializationUtils.serializeBinaryRow(indexManifestEntry.partition()),
                indexManifestEntry.bucket(),
                indexFileSerializer.serializeToBytes(targetFileMeta));
    }
}
