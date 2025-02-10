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
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.flink.FlinkCatalogFactory;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestFile;
import org.apache.paimon.manifest.ManifestFile.ManifestEntryWriter;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.utils.FileStorePathFactory;
import org.apache.paimon.utils.IOUtils;

import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** A Operator to copy files. */
public class CopyManifestFileOperator extends AbstractStreamOperator<CloneFileInfo>
        implements OneInputStreamOperator<CloneFileInfo, CloneFileInfo> {

    private static final Logger LOG = LoggerFactory.getLogger(CopyManifestFileOperator.class);

    private final Map<String, String> sourceCatalogConfig;
    private final Map<String, String> targetCatalogConfig;

    private transient Catalog sourceCatalog;
    private transient Catalog targetCatalog;

    private transient Map<String, FileIO> srcFileIOs;
    private transient Map<String, FileIO> targetFileIOs;
    private transient Map<String, Path> targetLocations;

    public CopyManifestFileOperator(
            Map<String, String> sourceCatalogConfig, Map<String, String> targetCatalogConfig) {
        this.sourceCatalogConfig = sourceCatalogConfig;
        this.targetCatalogConfig = targetCatalogConfig;
    }

    @Override
    public void open() throws Exception {
        sourceCatalog =
                FlinkCatalogFactory.createPaimonCatalog(Options.fromMap(sourceCatalogConfig));
        targetCatalog =
                FlinkCatalogFactory.createPaimonCatalog(Options.fromMap(targetCatalogConfig));
        targetLocations = new HashMap<>();
    }

    @Override
    public void processElement(StreamRecord<CloneFileInfo> streamRecord) throws Exception {
        CloneFileInfo cloneFileInfo = streamRecord.getValue();

        FileIO sourceTableFileIO =
                CloneFilesUtil.getFileIO(
                        srcFileIOs, cloneFileInfo.getSourceIdentifier(), sourceCatalog);
        FileIO targetTableFileIO =
                CloneFilesUtil.getFileIO(
                        targetFileIOs, cloneFileInfo.getSourceIdentifier(), targetCatalog);
        Path targetTableRootPath =
                CloneFilesUtil.getPath(
                        targetLocations, cloneFileInfo.getTargetIdentifier(), targetCatalog);

        String filePathExcludeTableRoot = cloneFileInfo.getFilePathExcludeTableRoot();
        Path sourcePath = new Path(cloneFileInfo.getSourceFilePath());
        Path targetPath = new Path(targetTableRootPath + filePathExcludeTableRoot);

        if (targetTableFileIO.exists(targetPath)
                && targetTableFileIO.getFileSize(targetPath)
                        == sourceTableFileIO.getFileSize(sourcePath)) {

            if (LOG.isDebugEnabled()) {
                LOG.debug(
                        "Skipping clone target file {} because it already exists and has the same size.",
                        targetPath);
            }

            // in this case,we don't need to copy the manifest file, just pick the data files
            copyOrRewriteManifestFile(
                    sourceTableFileIO,
                    targetTableFileIO,
                    sourcePath,
                    targetPath,
                    cloneFileInfo,
                    false);
            return;
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("Begin copy file from {} to {}.", sourcePath, targetPath);
        }
        copyOrRewriteManifestFile(
                sourceTableFileIO, targetTableFileIO, sourcePath, targetPath, cloneFileInfo, true);
        if (LOG.isDebugEnabled()) {
            LOG.debug("End copy file from {} to {}.", sourcePath, targetPath);
        }
    }

    private void copyOrRewriteManifestFile(
            FileIO sourceTableFileIO,
            FileIO targetTableFileIO,
            Path sourcePath,
            Path targetPath,
            CloneFileInfo cloneFileInfo,
            boolean needCopyManifestFile)
            throws IOException, Catalog.TableNotExistException {
        Identifier sourceIdentifier = Identifier.fromString(cloneFileInfo.getSourceIdentifier());
        FileStoreTable sourceTable = (FileStoreTable) sourceCatalog.getTable(sourceIdentifier);
        FileStore<?> store = sourceTable.store();
        ManifestFile manifestFile = store.manifestFileFactory().create();

        List<ManifestEntry> manifestEntries =
                manifestFile.readWithIOException(sourcePath.getName());
        List<ManifestEntry> targetManifestEntries = new ArrayList<>(manifestEntries.size());

        if (needCopyManifestFile) {
            if (containsExternalPath(manifestEntries)) {
                // rewrite it, clone job will clone the source path to target warehouse path, so the
                // target external
                // path is null
                for (ManifestEntry manifestEntry : manifestEntries) {
                    ManifestEntry newManifestEntry =
                            new ManifestEntry(
                                    manifestEntry.kind(),
                                    manifestEntry.partition(),
                                    manifestEntry.bucket(),
                                    manifestEntry.totalBuckets(),
                                    manifestEntry.file().newExternalPath(null));
                    targetManifestEntries.add(newManifestEntry);
                }
                ManifestEntryWriter manifestEntryWriter =
                        manifestFile.createManifestEntryWriter(targetPath);
                manifestEntryWriter.write(targetManifestEntries);
                manifestEntryWriter.close();
            } else {
                // copy it
                IOUtils.copyBytes(
                        sourceTableFileIO.newInputStream(sourcePath),
                        targetTableFileIO.newOutputStream(targetPath, true));
            }
        }
        // pick data files
        pickDataFilesForClone(manifestEntries, store, cloneFileInfo);
    }

    private void pickDataFilesForClone(
            List<ManifestEntry> manifestEntries, FileStore<?> store, CloneFileInfo cloneFileInfo) {
        // pick the data files
        for (ManifestEntry manifestEntry : manifestEntries) {
            FileStorePathFactory fileStorePathFactory = store.pathFactory();
            Path dataFilePath =
                    fileStorePathFactory
                            .createDataFilePathFactory(
                                    manifestEntry.partition(), manifestEntry.bucket())
                            .toPath(manifestEntry);
            Path relativeBucketPath =
                    fileStorePathFactory.relativeBucketPath(
                            manifestEntry.partition(), manifestEntry.bucket());
            Path relativeTablePath = new Path("/" + relativeBucketPath, dataFilePath.getName());

            output.collect(
                    new StreamRecord<>(
                            new CloneFileInfo(
                                    dataFilePath.toString(),
                                    relativeTablePath.toString(),
                                    cloneFileInfo.getSourceIdentifier(),
                                    cloneFileInfo.getTargetIdentifier())));
        }
    }

    private boolean containsExternalPath(List<ManifestEntry> manifestEntries) {
        boolean result = false;
        for (ManifestEntry manifestEntry : manifestEntries) {
            if (manifestEntry.file().externalPath().isPresent()) {
                result = true;
                break;
            }
        }
        return result;
    }

    @Override
    public void close() throws Exception {
        if (sourceCatalog != null) {
            sourceCatalog.close();
        }
        if (targetCatalog != null) {
            targetCatalog.close();
        }
    }
}
