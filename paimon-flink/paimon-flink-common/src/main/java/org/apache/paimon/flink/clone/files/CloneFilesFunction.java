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

package org.apache.paimon.flink.clone.files;

import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.format.SimpleStatsExtractor;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataFilePathFactory;
import org.apache.paimon.migrate.FileMetaUtils;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.utils.IOUtils;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;

/** Clone files for table. */
public class CloneFilesFunction extends CloneFilesProcessFunction<CloneFileInfo, DataFileInfo> {

    private static final long serialVersionUID = 1L;

    private transient Map<Identifier, Map<BinaryRow, DataFilePathFactory>> pathFactoryMap;

    public CloneFilesFunction(
            Map<String, String> sourceCatalogConfig, Map<String, String> targetCatalogConfig) {
        super(sourceCatalogConfig, targetCatalogConfig);
    }

    @Override
    public void open(Configuration conf) throws Exception {
        super.open(conf);
        this.pathFactoryMap = new HashMap<>();
    }

    @Override
    public void processElement(
            CloneFileInfo cloneFileInfo,
            ProcessFunction<CloneFileInfo, DataFileInfo>.Context context,
            Collector<DataFileInfo> collector)
            throws Exception {
        Identifier identifier = cloneFileInfo.identifier();
        String format = cloneFileInfo.format();
        Path path = cloneFileInfo.path();
        BinaryRow partition = cloneFileInfo.partition();

        FileIO sourceFileIO = hiveCatalog.fileIO();
        FileStoreTable targetTable = (FileStoreTable) getTable(identifier);

        // copy files
        Path targetFilePath = pathFactory(identifier, partition).newPathFromExtension("." + format);
        IOUtils.copyBytes(
                sourceFileIO.newInputStream(path),
                targetTable.fileIO().newOutputStream(targetFilePath, false));

        // to DataFileMeta
        SimpleStatsExtractor simpleStatsExtractor =
                FileMetaUtils.createSimpleStatsExtractor(targetTable, format);
        DataFileMeta dataFileMeta =
                FileMetaUtils.constructFileMeta(
                        targetFilePath.getName(),
                        cloneFileInfo.fileSize(),
                        path,
                        simpleStatsExtractor,
                        sourceFileIO,
                        targetTable);

        collector.collect(
                new DataFileInfo(
                        identifier, partition, dataFileSerializer.serializeToBytes(dataFileMeta)));
    }

    private DataFilePathFactory pathFactory(Identifier identifier, BinaryRow part) {
        return pathFactoryMap
                .computeIfAbsent(identifier, k -> new HashMap<>())
                .computeIfAbsent(
                        part,
                        k -> {
                            FileStoreTable targetTable = (FileStoreTable) getTable(identifier);
                            return targetTable
                                    .store()
                                    .pathFactory()
                                    .createDataFilePathFactory(part, 0);
                        });
    }
}
