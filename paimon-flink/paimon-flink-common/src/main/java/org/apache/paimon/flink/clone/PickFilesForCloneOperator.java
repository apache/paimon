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
import org.apache.paimon.fs.Path;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.utils.Preconditions;

import org.apache.paimon.shade.guava30.com.google.common.collect.ImmutableList;
import org.apache.paimon.shade.guava30.com.google.common.collect.ImmutableMap;
import org.apache.paimon.shade.guava30.com.google.common.collect.Iterables;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Pick the files to be cloned of a table based on the input record. The record type it produce is
 * CloneFileInfo that indicate the information of copy file.
 */
public class PickFilesForCloneOperator extends AbstractStreamOperator<CloneFileInfo>
        implements OneInputStreamOperator<Tuple2<String, String>, CloneFileInfo> {

    private static final Logger LOG = LoggerFactory.getLogger(PickFilesForCloneOperator.class);

    private final Map<String, String> sourceCatalogConfig;
    private final Map<String, String> targetCatalogConfig;

    private Catalog sourceCatalog;
    private Catalog targetCatalog;

    public PickFilesForCloneOperator(
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
        targetCatalog.createDatabase(targetIdentifier.getDatabaseName(), true);
        targetCatalog.createTable(
                targetIdentifier, newSchemaFromTableSchema(sourceTable.schema()), true);

        List<CloneFileInfo> result =
                toCloneFileInfos(
                        PickFilesUtil.getUsedFilesForLatestSnapshot(sourceTable),
                        sourceTable.location(),
                        sourceIdentifierStr,
                        targetIdentifierStr);

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

    private List<CloneFileInfo> toCloneFileInfos(
            List<Path> files,
            Path sourceTableRoot,
            String sourceIdentifier,
            String targetIdentifier) {
        List<CloneFileInfo> result = new ArrayList<>();
        for (Path file : files) {
            Path relativePath = getPathExcludeTableRoot(file, sourceTableRoot);
            result.add(
                    new CloneFileInfo(
                            file.toUri().toString(),
                            relativePath.toString(),
                            sourceIdentifier,
                            targetIdentifier));
        }
        return result;
    }

    private Path getPathExcludeTableRoot(Path absolutePath, Path sourceTableRoot) {
        String fileAbsolutePath = absolutePath.toUri().toString();
        String sourceTableRootPath = sourceTableRoot.toString();

        Preconditions.checkState(
                fileAbsolutePath.startsWith(sourceTableRootPath),
                "File absolute path does not start with source table root path. This is unexpected. "
                        + "fileAbsolutePath is: "
                        + fileAbsolutePath
                        + ", sourceTableRootPath is: "
                        + sourceTableRootPath);

        return new Path(fileAbsolutePath.substring(sourceTableRootPath.length()));
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
