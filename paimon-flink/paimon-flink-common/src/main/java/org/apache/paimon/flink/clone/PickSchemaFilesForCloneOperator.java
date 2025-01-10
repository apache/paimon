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
import org.apache.paimon.Snapshot;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.flink.FlinkCatalogFactory;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.utils.SnapshotManager;

import org.apache.paimon.shade.guava30.com.google.common.collect.ImmutableList;
import org.apache.paimon.shade.guava30.com.google.common.collect.ImmutableMap;
import org.apache.paimon.shade.guava30.com.google.common.collect.Iterables;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Pick the files to be cloned of a table based on the input record. The record type it produce is
 * CloneFileInfo that indicate the information of copy file.
 */
public class PickSchemaFilesForCloneOperator extends AbstractStreamOperator<CloneFileInfo>
        implements OneInputStreamOperator<Tuple2<String, String>, CloneFileInfo> {

    private static final Logger LOG =
            LoggerFactory.getLogger(PickSchemaFilesForCloneOperator.class);

    private final Map<String, String> sourceCatalogConfig;
    private final Map<String, String> targetCatalogConfig;

    private Catalog sourceCatalog;
    private Catalog targetCatalog;

    public PickSchemaFilesForCloneOperator(
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
    }

    @Override
    public void processElement(StreamRecord<Tuple2<String, String>> streamRecord) throws Exception {
        String sourceIdentifierStr = streamRecord.getValue().f0;
        Identifier sourceIdentifier = Identifier.fromString(sourceIdentifierStr);
        String targetIdentifierStr = streamRecord.getValue().f1;
        Identifier targetIdentifier = Identifier.fromString(targetIdentifierStr);

        FileStoreTable sourceTable = (FileStoreTable) sourceCatalog.getTable(sourceIdentifier);

        FileStore<?> store = sourceTable.store();
        SnapshotManager snapshotManager = store.snapshotManager();
        Snapshot latestSnapshot = snapshotManager.latestSnapshot();
        if (latestSnapshot == null) {
            throw new IllegalStateException("try to get the latest snapshot failed");
        }
        long snapshotId = latestSnapshot.id();
        targetCatalog.createDatabase(targetIdentifier.getDatabaseName(), true);
        targetCatalog.createTable(
                targetIdentifier, newSchemaFromTableSchema(sourceTable.schema()), true);
        List<CloneFileInfo> result =
                CloneFilesUtil.toCloneFileInfos(
                        CloneFilesUtil.getSchemaUsedFilesForSnapshot(sourceTable, snapshotId),
                        sourceTable.location(),
                        sourceIdentifierStr,
                        targetIdentifierStr,
                        snapshotId);

        if (LOG.isDebugEnabled()) {
            LOG.debug("The CloneFileInfo of table {} is {} : ", sourceTable.location(), result);
        }
        for (CloneFileInfo info : result) {
            output.collect(new StreamRecord<>(info));
        }
    }

    private static Schema newSchemaFromTableSchema(TableSchema tableSchema) {
        return new Schema(
                ImmutableList.copyOf(tableSchema.fields()),
                ImmutableList.copyOf(tableSchema.partitionKeys()),
                ImmutableList.copyOf(tableSchema.primaryKeys()),
                ImmutableMap.copyOf(
                        Iterables.filter(
                                tableSchema.options().entrySet(),
                                entry -> !Objects.equals(entry.getKey(), CoreOptions.PATH.key()))),
                tableSchema.comment());
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
