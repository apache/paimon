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

package org.apache.paimon.flink.clone.spits;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
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
public class CommitMessageTableOperator extends AbstractStreamOperator<Long>
        implements OneInputStreamOperator<CommitMessageInfo, Long>, BoundedOneInput {

    private static final long serialVersionUID = 1L;

    private final Map<String, String> catalogConfig;

    private transient Map<Identifier, List<CommitMessage>> commitMessages;

    public CommitMessageTableOperator(Map<String, String> catalogConfig) {
        this.catalogConfig = catalogConfig;
    }

    @Override
    public void open() throws Exception {
        super.open();
        this.commitMessages = new HashMap<>();
    }

    @Override
    public void processElement(StreamRecord<CommitMessageInfo> streamRecord) throws Exception {
        CommitMessageInfo info = streamRecord.getValue();
        List<CommitMessage> messages =
                this.commitMessages.computeIfAbsent(info.identifier(), k -> new ArrayList<>());
        messages.add(info.commitMessage());
    }

    @Override
    public void endInput() throws Exception {
        try (Catalog catalog = createPaimonCatalog(Options.fromMap(catalogConfig))) {
            for (Map.Entry<Identifier, List<CommitMessage>> entry : commitMessages.entrySet()) {
                List<CommitMessage> commitMessages = entry.getValue();
                Table table = catalog.getTable(entry.getKey());
                try (BatchTableCommit commit =
                        table.newBatchWriteBuilder().withOverwrite().newCommit()) {
                    commit.commit(commitMessages);
                }
            }
        }
    }
}
