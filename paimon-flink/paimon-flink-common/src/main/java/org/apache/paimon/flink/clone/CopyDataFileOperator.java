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

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.flink.FlinkCatalogFactory;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.options.Options;
import org.apache.paimon.utils.IOUtils;

import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.util.HashMap;
import java.util.Map;

/** A Operator to copy files. */
public class CopyDataFileOperator extends AbstractStreamOperator<CloneFileInfo>
        implements OneInputStreamOperator<CloneFileInfo, CloneFileInfo> {

    private static final Logger LOG = LoggerFactory.getLogger(CopyDataFileOperator.class);

    private final Map<String, String> sourceCatalogConfig;
    private final Map<String, String> targetCatalogConfig;

    private transient Catalog sourceCatalog;
    private transient Catalog targetCatalog;

    private transient Map<String, FileIO> srcFileIOs;
    private transient Map<String, FileIO> targetFileIOs;
    private transient Map<String, Path> targetLocations;

    public CopyDataFileOperator(
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
        srcFileIOs = new HashMap<>();
        targetFileIOs = new HashMap<>();
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

        try {
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
                output.collect(streamRecord);
                return;
            }
        } catch (FileNotFoundException e) {
            LOG.warn("File {} does not exist. ignore it", sourcePath, e);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("Begin copy file from {} to {}.", sourcePath, targetPath);
        }
        try {
            IOUtils.copyBytes(
                    sourceTableFileIO.newInputStream(sourcePath),
                    targetTableFileIO.newOutputStream(targetPath, true));
        } catch (FileNotFoundException e) {
            LOG.warn("File {} does not exist. ignore it", sourcePath, e);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("End copy file from {} to {}.", sourcePath, targetPath);
        }

        output.collect(streamRecord);
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
