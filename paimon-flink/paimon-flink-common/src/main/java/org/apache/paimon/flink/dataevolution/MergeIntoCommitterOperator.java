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

package org.apache.paimon.flink.dataevolution;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.flink.sink.Committable;
import org.apache.paimon.index.GlobalIndexMeta;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Preconditions;

import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A committer for MergeInto which wraps a normal committer and provide the ability to check updated
 * columns.
 *
 * <p>This operator is only for batch mode.
 */
public class MergeIntoCommitterOperator extends AbstractStreamOperator<Committable>
        implements OneInputStreamOperator<Committable, Committable>, BoundedOneInput {
    private static final String MERGE_INTO_COMMITTER_NAME = "merge_into_committer";

    private static final Logger LOG = LoggerFactory.getLogger(MergeIntoCommitterOperator.class);

    private final FileStoreTable table;
    private final Set<String> updatedColumns;
    private transient Set<BinaryRow> affectedPartitions;
    private transient List<CommitMessage> commitMessages;

    public MergeIntoCommitterOperator(
            StreamOperatorParameters<Committable> parameters,
            FileStoreTable table,
            Set<String> updatedColumns) {
        this.table = table;
        this.updatedColumns = updatedColumns;

        setup(parameters.getContainingTask(), parameters.getStreamConfig(), parameters.getOutput());
    }

    @Override
    public void open() throws Exception {
        super.open();

        affectedPartitions = new HashSet<>();
        commitMessages = new ArrayList<>();
    }

    @Override
    public void processElement(StreamRecord<Committable> element) throws Exception {
        commitMessages.add(element.getValue().commitMessage());
        affectedPartitions.add(element.getValue().commitMessage().partition());
    }

    @Override
    public void endInput() throws Exception {
        checkUpdatedColumns();

        if (!commitMessages.isEmpty()) {
            try (BatchTableCommit tableCommit = table.newCommit(MERGE_INTO_COMMITTER_NAME)) {
                tableCommit.commit(commitMessages);
            }
        } else {
            LOG.warn("Empty commit messages, skip commit.");
        }
    }

    private void checkUpdatedColumns() {
        Optional<Snapshot> latestSnapshot = table.latestSnapshot();
        RowType rowType = table.rowType();
        Preconditions.checkState(latestSnapshot.isPresent());

        List<IndexManifestEntry> affectedEntries =
                table.store()
                        .newIndexFileHandler()
                        .scan(
                                latestSnapshot.get(),
                                entry -> {
                                    GlobalIndexMeta globalIndexMeta =
                                            entry.indexFile().globalIndexMeta();
                                    if (globalIndexMeta != null) {
                                        String fieldName =
                                                rowType.getField(globalIndexMeta.indexFieldId())
                                                        .name();
                                        return updatedColumns.contains(fieldName)
                                                && affectedPartitions.contains(entry.partition());
                                    }
                                    return false;
                                });

        if (!affectedEntries.isEmpty()) {
            CoreOptions.GlobalIndexColumnUpdateAction updateAction =
                    table.coreOptions().globalIndexColumnUpdateAction();
            switch (updateAction) {
                case THROW_ERROR:
                    Set<String> conflictedColumns =
                            affectedEntries.stream()
                                    .map(file -> file.indexFile().globalIndexMeta().indexFieldId())
                                    .map(id -> rowType.getField(id).name())
                                    .collect(Collectors.toSet());

                    throw new RuntimeException(
                            String.format(
                                    "MergeInto: update columns contain globally indexed columns, not supported now.\n"
                                            + "Updated columns: %s\nConflicted columns: %s\n",
                                    updatedColumns, conflictedColumns));
                case DROP_PARTITION_INDEX:
                    Map<BinaryRow, List<IndexFileMeta>> entriesByParts =
                            affectedEntries.stream()
                                    .collect(
                                            Collectors.groupingBy(
                                                    IndexManifestEntry::partition,
                                                    Collectors.mapping(
                                                            IndexManifestEntry::indexFile,
                                                            Collectors.toList())));

                    for (Map.Entry<BinaryRow, List<IndexFileMeta>> entry :
                            entriesByParts.entrySet()) {
                        LOG.debug(
                                "Dropping index files {} due to indexed fields update.",
                                entry.getValue());

                        CommitMessage commitMessage =
                                new CommitMessageImpl(
                                        entry.getKey(),
                                        0,
                                        null,
                                        DataIncrement.deleteIndexIncrement(entry.getValue()),
                                        CompactIncrement.emptyIncrement());

                        commitMessages.add(commitMessage);
                    }
                    break;
                default:
                    throw new UnsupportedOperationException("Unsupported option: " + updateAction);
            }
        }
    }
}
