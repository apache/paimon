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

package org.apache.paimon.flink.clone.history;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.clone.FullHistoryCloneMarker;
import org.apache.paimon.clone.FullHistoryClonePlan;
import org.apache.paimon.clone.FullHistoryCloneValidator;
import org.apache.paimon.clone.FullHistoryMetadataRewriter;
import org.apache.paimon.clone.FullHistorySourceFingerprint;
import org.apache.paimon.clone.PathMapping;
import org.apache.paimon.flink.FlinkCatalogFactory;
import org.apache.paimon.flink.utils.BoundedOneInputOperator;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.utils.IOUtils;

import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Map;

/** Publishes rewritten metadata and validates the clone after every payload file is copied. */
class FullHistoryCloneFinalizeOperator extends BoundedOneInputOperator<Boolean, Boolean> {

    private static final long serialVersionUID = 1L;

    private final Map<String, String> sourceCatalogConfig;
    private final Map<String, String> targetCatalogConfig;
    private final String sourceDatabase;
    private final String sourceTableName;
    private final String targetDatabase;
    private final String targetTableName;
    private final List<String> pathMappings;
    private final FullHistoryClonePlan clonePlan;

    private transient Catalog sourceCatalog;
    private transient FileStoreTable sourceTable;
    private transient FileIO targetFileIO;

    FullHistoryCloneFinalizeOperator(
            Map<String, String> sourceCatalogConfig,
            Map<String, String> targetCatalogConfig,
            String sourceDatabase,
            String sourceTableName,
            @Nullable String targetDatabase,
            @Nullable String targetTableName,
            List<String> pathMappings,
            FullHistoryClonePlan clonePlan) {
        this.sourceCatalogConfig = sourceCatalogConfig;
        this.targetCatalogConfig = targetCatalogConfig;
        this.sourceDatabase = sourceDatabase;
        this.sourceTableName = sourceTableName;
        this.targetDatabase = targetDatabase;
        this.targetTableName = targetTableName;
        this.pathMappings = pathMappings;
        this.clonePlan = clonePlan;
    }

    @Override
    public void open() throws Exception {
        sourceCatalog =
                FlinkCatalogFactory.createPaimonCatalog(Options.fromMap(sourceCatalogConfig));
        sourceTable =
                (FileStoreTable)
                        sourceCatalog.getTable(Identifier.create(sourceDatabase, sourceTableName));
        targetFileIO =
                FullHistoryFileIOUtils.createResolvingFileIO(
                        clonePlan.targetRoot(), targetCatalogConfig);
    }

    @Override
    public void processElement(StreamRecord<Boolean> record) {}

    @Override
    public void endInput() throws Exception {
        PathMapping mapping = PathMapping.parse(pathMappings);
        if (FullHistoryCloneMarker.isSuccessful(
                targetFileIO, clonePlan, mapping, targetDatabase, targetTableName)) {
            output.collect(new StreamRecord<>(true));
            return;
        }
        FullHistorySourceFingerprint.verify(sourceTable, clonePlan.sourceFingerprint());
        new FullHistoryMetadataRewriter(sourceTable, targetFileIO, clonePlan.targetRoot(), mapping)
                .rewrite();
        FileStoreTable targetTable =
                FileStoreTableFactory.create(targetFileIO, clonePlan.targetRoot());
        new FullHistoryCloneValidator(sourceTable, targetTable, mapping)
                .validatePublishedCloneStreaming();
        FullHistorySourceFingerprint.verify(sourceTable, clonePlan.sourceFingerprint());
        FullHistoryCloneMarker.markSuccessful(
                targetFileIO, clonePlan, mapping, targetDatabase, targetTableName);
        output.collect(new StreamRecord<>(true));
    }

    @Override
    public void close() throws Exception {
        IOUtils.closeAll(sourceCatalog, targetFileIO, super::close);
    }
}
