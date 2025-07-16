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

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataFileMetaSerializer;
import org.apache.paimon.migrate.FileMetaUtils;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.CommitMessage;

import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.paimon.flink.FlinkCatalogFactory.createPaimonCatalog;

/** Commit table operator. */
public class CloneFilesCommitOperator extends AbstractStreamOperator<Long>
        implements OneInputStreamOperator<DataFileInfo, Long>, BoundedOneInput {

    private static final long serialVersionUID = 1L;

    private final Map<String, String> catalogConfig;

    private transient DataFileMetaSerializer dataFileSerializer;
    private transient Map<Identifier, Map<BinaryRow, List<DataFileMeta>>> files;

    public CloneFilesCommitOperator(Map<String, String> catalogConfig) {
        this.catalogConfig = catalogConfig;
    }

    @Override
    public void open() throws Exception {
        super.open();
        this.dataFileSerializer = new DataFileMetaSerializer();
        this.files = new HashMap<>();
    }

    @Override
    public void processElement(StreamRecord<DataFileInfo> streamRecord) throws Exception {
        DataFileInfo file = streamRecord.getValue();
        BinaryRow partition = file.partition();
        List<DataFileMeta> files =
                this.files
                        .computeIfAbsent(file.identifier(), k -> new HashMap<>())
                        .computeIfAbsent(partition, k -> new ArrayList<>());
        files.add(dataFileSerializer.deserializeFromBytes(file.dataFileMeta()));
    }

    @Override
    public void endInput() throws Exception {
        try (Catalog catalog = createPaimonCatalog(Options.fromMap(catalogConfig))) {
            for (Map.Entry<Identifier, Map<BinaryRow, List<DataFileMeta>>> entry :
                    files.entrySet()) {
                List<CommitMessage> commitMessages = new ArrayList<>();
                for (Map.Entry<BinaryRow, List<DataFileMeta>> listEntry :
                        entry.getValue().entrySet()) {
                    commitMessages.add(
                            FileMetaUtils.createCommitMessage(
                                    listEntry.getKey(), 0, listEntry.getValue()));
                }

                Table table = catalog.getTable(entry.getKey());
                try (BatchTableCommit commit =
                        table.newBatchWriteBuilder().withOverwrite().newCommit()) {
                    commit.commit(commitMessages);
                }
            }
        }
    }
}
