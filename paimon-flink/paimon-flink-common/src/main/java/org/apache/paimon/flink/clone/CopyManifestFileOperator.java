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

import org.apache.paimon.CoreOptions;
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
import org.apache.paimon.table.Table;
import org.apache.paimon.utils.IOUtils;
import org.apache.paimon.utils.Triple;

import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** A Operator to copy files. */
public class CopyManifestFileOperator extends AbstractStreamOperator<CloneFileInfo>
        implements OneInputStreamOperator<CloneFileInfo, CloneFileInfo>, BoundedOneInput {

    private static final Logger LOG = LoggerFactory.getLogger(CopyManifestFileOperator.class);

    private final Map<String, String> sourceCatalogConfig;
    private final Map<String, String> targetCatalogConfig;

    private transient Catalog sourceCatalog;
    private transient Catalog targetCatalog;

    private transient Map<String, Path> targetLocations;

    private final Set<Triple<String, String, Long>> identifiersAndSnapshotIds;

    public CopyManifestFileOperator(
            Map<String, String> sourceCatalogConfig, Map<String, String> targetCatalogConfig) {
        this.sourceCatalogConfig = sourceCatalogConfig;
        this.targetCatalogConfig = targetCatalogConfig;
        identifiersAndSnapshotIds = new HashSet<>();
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
        FileIO sourceTableFileIO = sourceCatalog.fileIO();
        FileIO targetTableFileIO = targetCatalog.fileIO();
        Path targetTableRootPath =
                targetLocations.computeIfAbsent(
                        cloneFileInfo.getTargetIdentifier(),
                        key -> {
                            try {
                                return pathOfTable(
                                        targetCatalog.getTable(Identifier.fromString(key)));
                            } catch (Catalog.TableNotExistException e) {
                                throw new RuntimeException(e);
                            }
                        });

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

            // We still send record to SnapshotHintOperator to avoid the following corner case:
            //
            // When cloning two tables under a catalog, after clone table A is completed,
            // the job fails due to snapshot expiration when cloning table B.
            // If we don't re-send file information of table A to SnapshotHintOperator,
            // the snapshot hint file of A will not be created after the restart.
            identifiersAndSnapshotIds.add(
                    Triple.of(
                            cloneFileInfo.getSourceIdentifier(),
                            cloneFileInfo.getTargetIdentifier(),
                            cloneFileInfo.getSnapshotId()));
            return;
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("Begin copy file from {} to {}.", sourcePath, targetPath);
        }
        copyOrRewriteManifestFile(
                sourceTableFileIO, targetTableFileIO, sourcePath, targetPath, cloneFileInfo);
        if (LOG.isDebugEnabled()) {
            LOG.debug("End copy file from {} to {}.", sourcePath, targetPath);
        }

        identifiersAndSnapshotIds.add(
                Triple.of(
                        cloneFileInfo.getSourceIdentifier(),
                        cloneFileInfo.getTargetIdentifier(),
                        cloneFileInfo.getSnapshotId()));
    }

    private void copyOrRewriteManifestFile(
            FileIO sourceTableFileIO,
            FileIO targetTableFileIO,
            Path sourcePath,
            Path targetPath,
            CloneFileInfo cloneFileInfo)
            throws IOException, Catalog.TableNotExistException {
        Identifier sourceIdentifier = Identifier.fromString(cloneFileInfo.getSourceIdentifier());
        FileStoreTable sourceTable = (FileStoreTable) sourceCatalog.getTable(sourceIdentifier);
        FileStore<?> store = sourceTable.store();
        ManifestFile manifestFile = store.manifestFileFactory().create();

        List<ManifestEntry> manifestEntries =
                manifestFile.readWithIOException(sourcePath.getName());
        List<ManifestEntry> targetManifestEntries = new ArrayList<>(manifestEntries.size());

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

    private Path pathOfTable(Table table) {
        return new Path(table.options().get(CoreOptions.PATH.key()));
    }

    @Override
    public void endInput() throws Exception {
        for (Triple<String, String, Long> identifierAndSnapshotId : identifiersAndSnapshotIds) {
            output.collect(
                    new StreamRecord<>(
                            new CloneFileInfo(
                                    null,
                                    null,
                                    identifierAndSnapshotId.f0,
                                    identifierAndSnapshotId.f1,
                                    identifierAndSnapshotId.f2)));
        }
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
