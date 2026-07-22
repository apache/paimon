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
import org.apache.paimon.clone.FullHistoryCopyPlan;
import org.apache.paimon.clone.FullHistoryFileCopier;
import org.apache.paimon.flink.FlinkCatalogFactory;
import org.apache.paimon.flink.utils.BoundedOneInputOperator;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.utils.IOUtils;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.util.Collections;
import java.util.Map;

/** Copies one planned full-history payload file per input record. */
class CopyFullHistoryFileOperator
        extends BoundedOneInputOperator<FullHistoryCopyPlan.FileCopy, Boolean> {

    private static final long serialVersionUID = 1L;

    private final Map<String, String> sourceCatalogConfig;
    private final Map<String, String> targetCatalogConfig;
    private final String sourceDatabase;
    private final String sourceTableName;

    private transient Catalog sourceCatalog;
    private transient FileIO sourceFileIO;
    private transient FileIO targetFileIO;
    private transient ValueState<String> copiedFile;

    CopyFullHistoryFileOperator(
            Map<String, String> sourceCatalogConfig,
            Map<String, String> targetCatalogConfig,
            String sourceDatabase,
            String sourceTableName) {
        this.sourceCatalogConfig = sourceCatalogConfig;
        this.targetCatalogConfig = targetCatalogConfig;
        this.sourceDatabase = sourceDatabase;
        this.sourceTableName = sourceTableName;
    }

    CopyFullHistoryFileOperator(FileIO sourceFileIO, FileIO targetFileIO) {
        this.sourceCatalogConfig = Collections.emptyMap();
        this.targetCatalogConfig = Collections.emptyMap();
        this.sourceDatabase = "";
        this.sourceTableName = "";
        this.sourceFileIO = sourceFileIO;
        this.targetFileIO = targetFileIO;
    }

    @Override
    public void open() throws Exception {
        super.open();
        if (sourceFileIO == null) {
            sourceCatalog =
                    FlinkCatalogFactory.createPaimonCatalog(Options.fromMap(sourceCatalogConfig));
            FileStoreTable sourceTable =
                    (FileStoreTable)
                            sourceCatalog.getTable(
                                    Identifier.create(sourceDatabase, sourceTableName));
            sourceFileIO = sourceTable.fileIO();
        }
        copiedFile =
                getPartitionedState(
                        new ValueStateDescriptor<>("full-history-copied-file", String.class));
    }

    @Override
    public void processElement(StreamRecord<FullHistoryCopyPlan.FileCopy> record) throws Exception {
        FullHistoryCopyPlan.FileCopy file = record.getValue();
        String identity = copyIdentity(file);
        String previousIdentity = copiedFile.value();
        if (previousIdentity != null) {
            if (!previousIdentity.equals(identity)) {
                throw new IllegalArgumentException(
                        String.format(
                                "Conflicting clone payloads map to target %s: %s and %s.",
                                file.target(), previousIdentity, identity));
            }
            return;
        }
        if (targetFileIO == null) {
            targetFileIO =
                    FullHistoryFileIOUtils.createResolvingFileIO(
                            file.target(), targetCatalogConfig);
        }
        FullHistoryFileCopier.copyFile(sourceFileIO, targetFileIO, file, false);
        copiedFile.update(identity);
    }

    private static String copyIdentity(FullHistoryCopyPlan.FileCopy file) {
        return file.source()
                + "\n"
                + file.target()
                + "\n"
                + file.kind()
                + "\n"
                + file.expectedSize();
    }

    @Override
    public void endInput() {
        output.collect(new StreamRecord<>(true));
    }

    @Override
    public void close() throws Exception {
        IOUtils.closeAll(sourceCatalog, targetFileIO, super::close);
    }
}
