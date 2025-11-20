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
import org.apache.paimon.io.DataFileMetaSerializer;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.ScanMode;
import org.apache.paimon.utils.FileStorePathFactory;
import org.apache.paimon.utils.SerializationUtils;

import org.apache.spark.sql.SparkSession;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/** List data files. */
public class ListDataFilesOperator extends CopyFilesOperator {

    private final DataFileMetaSerializer dataFileSerializer;

    public ListDataFilesOperator(SparkSession spark, Catalog sourceCatalog, Catalog targetCatalog) {
        super(spark, sourceCatalog, targetCatalog);
        this.dataFileSerializer = new DataFileMetaSerializer();
    }

    public List<CopyFileInfo> execute(
            Identifier sourceIdentifier,
            @Nullable Snapshot snapshot,
            @Nullable PartitionPredicate partitionPredicate)
            throws Exception {
        if (snapshot == null) {
            return null;
        }
        FileStoreTable sourceTable = (FileStoreTable) sourceCatalog.getTable(sourceIdentifier);
        Iterator<ManifestEntry> manifestEntries =
                sourceTable
                        .newSnapshotReader()
                        .withSnapshot(snapshot)
                        .withPartitionFilter(partitionPredicate)
                        .withMode(ScanMode.ALL)
                        .readFileIterator();

        List<CopyFileInfo> dataFiles = new ArrayList<>();
        while (manifestEntries.hasNext()) {
            ManifestEntry manifestEntry = manifestEntries.next();
            CopyFileInfo dataFile =
                    pickDataFiles(
                            manifestEntry,
                            sourceTable.store().pathFactory(),
                            sourceTable.location());
            dataFiles.add(dataFile);
        }
        return dataFiles;
    }

    private CopyFileInfo pickDataFiles(
            ManifestEntry manifestEntry,
            FileStorePathFactory fileStorePathFactory,
            Path sourceTableRoot)
            throws IOException {
        Path dataFilePath =
                fileStorePathFactory
                        .createDataFilePathFactory(
                                manifestEntry.partition(), manifestEntry.bucket())
                        .toPath(manifestEntry);
        Path relativePath = CopyFilesUtil.getPathExcludeTableRoot(dataFilePath, sourceTableRoot);
        return new CopyFileInfo(
                dataFilePath.toString(),
                relativePath.toString(),
                SerializationUtils.serializeBinaryRow(manifestEntry.partition()),
                manifestEntry.bucket(),
                manifestEntry.totalBuckets(),
                dataFileSerializer.serializeToBytes(manifestEntry.file()));
    }
}
