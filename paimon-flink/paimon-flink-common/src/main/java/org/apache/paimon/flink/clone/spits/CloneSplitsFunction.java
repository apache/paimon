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
import org.apache.paimon.options.Options;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.source.TableRead;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.util.List;
import java.util.Map;

import static org.apache.paimon.flink.FlinkCatalogFactory.createPaimonCatalog;

/** Clone splits for table. */
public class CloneSplitsFunction extends ProcessFunction<CloneSplitInfo, CommitMessageInfo> {

    private static final long serialVersionUID = 1L;

    private final Map<String, String> sourceCatalogConfig;
    private final Map<String, String> targetCatalogConfig;

    private transient Catalog sourceCatalog;
    private transient Catalog targetCatalog;

    public CloneSplitsFunction(
            Map<String, String> sourceCatalogConfig, Map<String, String> targetCatalogConfig) {
        this.sourceCatalogConfig = sourceCatalogConfig;
        this.targetCatalogConfig = targetCatalogConfig;
    }

    /**
     * Do not annotate with <code>@override</code> here to maintain compatibility with Flink 1.18-.
     */
    public void open(OpenContext openContext) throws Exception {
        open(new Configuration());
    }

    /**
     * Do not annotate with <code>@override</code> here to maintain compatibility with Flink 2.0+.
     */
    public void open(Configuration conf) throws Exception {
        this.sourceCatalog = createPaimonCatalog(Options.fromMap(sourceCatalogConfig));
        this.targetCatalog = createPaimonCatalog(Options.fromMap(targetCatalogConfig));
    }

    @Override
    public void processElement(
            CloneSplitInfo cloneSplitInfo,
            ProcessFunction<CloneSplitInfo, CommitMessageInfo>.Context context,
            Collector<CommitMessageInfo> collector)
            throws Exception {

        // TODO reuse table read?
        TableRead tableRead =
                sourceCatalog
                        .getTable(cloneSplitInfo.sourceIdentifier())
                        .newReadBuilder()
                        .newRead();
        // TODO reuse table write
        BatchTableWrite write =
                targetCatalog
                        .getTable(cloneSplitInfo.targetidentifier())
                        .newBatchWriteBuilder()
                        .newWrite();

        tableRead
                .createReader(cloneSplitInfo.split())
                .forEachRemaining(
                        row -> {
                            try {
                                write.write(row);
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        });

        List<CommitMessage> commitMessages = write.prepareCommit();
        for (CommitMessage commitMessage : commitMessages) {
            CommitMessageInfo messageInfo =
                    new CommitMessageInfo(cloneSplitInfo.targetidentifier(), commitMessage);
            collector.collect(messageInfo);
        }
        write.close();
    }
}
