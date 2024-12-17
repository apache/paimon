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
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.flink.FlinkCatalogFactory;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.Table;
import org.apache.paimon.utils.IOUtils;

import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/** A Operator to copy files. */
public class CopyFileOperator extends AbstractStreamOperator<CloneFileInfo>
        implements OneInputStreamOperator<CloneFileInfo, CloneFileInfo> {

    private static final Logger LOG = LoggerFactory.getLogger(CopyFileOperator.class);

    private final Map<String, String> sourceCatalogConfig;
    private final Map<String, String> targetCatalogConfig;

    private transient Catalog sourceCatalog;
    private transient Catalog targetCatalog;

    private transient Map<String, Path> srcLocations;
    private transient Map<String, Path> targetLocations;

    public CopyFileOperator(
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
        srcLocations = new HashMap<>();
        targetLocations = new HashMap<>();
    }

    @Override
    public void processElement(StreamRecord<CloneFileInfo> streamRecord) throws Exception {
        CloneFileInfo cloneFileInfo = streamRecord.getValue();

        FileIO sourceTableFileIO = sourceCatalog.fileIO();
        FileIO targetTableFileIO = targetCatalog.fileIO();

        Path sourceTableRootPath =
                srcLocations.computeIfAbsent(
                        cloneFileInfo.getSourceIdentifier(),
                        key -> {
                            try {
                                return pathOfTable(
                                        sourceCatalog.getTable(Identifier.fromString(key)));
                            } catch (Catalog.TableNotExistException e) {
                                throw new RuntimeException(e);
                            }
                        });
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
        Path sourcePath = new Path(sourceTableRootPath + filePathExcludeTableRoot);
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
            output.collect(streamRecord);
            return;
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("Begin copy file from {} to {}.", sourcePath, targetPath);
        }
        IOUtils.copyBytes(
                sourceTableFileIO.newInputStream(sourcePath),
                targetTableFileIO.newOutputStream(targetPath, true));
        if (LOG.isDebugEnabled()) {
            LOG.debug("End copy file from {} to {}.", sourcePath, targetPath);
        }

        output.collect(streamRecord);
    }

    private Path pathOfTable(Table table) {
        return new Path(table.options().get(CoreOptions.PATH.key()));
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
