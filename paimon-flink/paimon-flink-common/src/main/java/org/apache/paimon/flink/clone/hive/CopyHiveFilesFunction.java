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

package org.apache.paimon.flink.clone.hive;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.format.SimpleColStats;
import org.apache.paimon.format.SimpleStatsExtractor;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.statistics.SimpleColStatsCollector;
import org.apache.paimon.stats.SimpleStats;
import org.apache.paimon.stats.SimpleStatsConverter;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.IOUtils;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.StatsCollectorFactories;

import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Collections;
import java.util.Map;

/** Copy files for table. */
public class CopyHiveFilesFunction extends CopyProcessFunction<CloneFileInfo, DataFileInfo> {

    private static final long serialVersionUID = 1L;

    public CopyHiveFilesFunction(
            Map<String, String> sourceCatalogConfig, Map<String, String> targetCatalogConfig) {
        super(sourceCatalogConfig, targetCatalogConfig);
    }

    @Override
    public void processElement(
            CloneFileInfo cloneFileInfo,
            ProcessFunction<CloneFileInfo, DataFileInfo>.Context context,
            Collector<DataFileInfo> collector)
            throws Exception {
        Identifier identifier = cloneFileInfo.identifier();
        long fileSize = cloneFileInfo.fileSize();
        String format = cloneFileInfo.format();
        Path path = cloneFileInfo.path();
        BinaryRow partition = cloneFileInfo.partition();

        FileIO sourceFileIO = hiveCatalog.fileIO();
        FileStoreTable targetTable = (FileStoreTable) getTable(identifier);
        // util for collecting stats
        CoreOptions options = targetTable.coreOptions();
        SimpleColStatsCollector.Factory[] factories =
                StatsCollectorFactories.createStatsFactories(
                        options.statsMode(), options, targetTable.rowType().getFieldNames());

        SimpleStatsExtractor simpleStatsExtractor =
                FileFormat.fromIdentifier(format, options.toConfiguration())
                        .createStatsExtractor(targetTable.rowType(), factories)
                        .orElseThrow(
                                () ->
                                        new RuntimeException(
                                                "Can't get table stats extractor for format "
                                                        + format));
        RowType rowTypeWithSchemaId =
                targetTable.schemaManager().schema(targetTable.schema().id()).logicalRowType();

        SimpleStatsConverter statsArraySerializer = new SimpleStatsConverter(rowTypeWithSchemaId);

        // extract stats
        Pair<SimpleColStats[], SimpleStatsExtractor.FileInfo> fileInfo =
                simpleStatsExtractor.extractWithFileInfo(sourceFileIO, path, fileSize);
        SimpleStats stats = statsArraySerializer.toBinaryAllMode(fileInfo.getLeft());

        // new file name
        String suffix = "." + format;
        String fileName = path.getName();
        String newFileName = fileName.endsWith(suffix) ? fileName : fileName + suffix;

        // copy files
        Path targetFilePath = targetTable.store().pathFactory().bucketPath(partition, 0);
        IOUtils.copyBytes(
                sourceFileIO.newInputStream(path),
                targetTable.fileIO().newOutputStream(new Path(targetFilePath, newFileName), false));

        // to DataFileMeta
        DataFileMeta dataFileMeta =
                DataFileMeta.forAppend(
                        newFileName,
                        fileSize,
                        fileInfo.getRight().getRowCount(),
                        stats,
                        0,
                        0,
                        targetTable.schema().id(),
                        Collections.emptyList(),
                        null,
                        FileSource.APPEND,
                        null,
                        null);

        collector.collect(
                new DataFileInfo(
                        identifier, partition, dataFileSerializer.serializeToBytes(dataFileMeta)));
    }
}
