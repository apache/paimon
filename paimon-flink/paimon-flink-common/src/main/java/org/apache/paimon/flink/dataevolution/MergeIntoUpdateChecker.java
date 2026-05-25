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
import org.apache.paimon.flink.utils.BoundedOneInputOperator;
import org.apache.paimon.flink.utils.RuntimeContextUtils;
import org.apache.paimon.index.GlobalIndexMeta;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Preconditions;

import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * The checker for merge into update result. It will check each committable to see if some
 * global-indexed columns are updated. It will take some actions according to {@link
 * CoreOptions#GLOBAL_INDEX_COLUMN_UPDATE_ACTION}.
 */
public class MergeIntoUpdateChecker extends BoundedOneInputOperator<Committable, Committable> {

    private static final Logger LOG = LoggerFactory.getLogger(MergeIntoUpdateChecker.class);

    private final FileStoreTable table;
    private final Set<String> updatedColumns;

    private transient Set<BinaryRow> affectedPartitions;

    public MergeIntoUpdateChecker(FileStoreTable table, Set<String> updatedColumns) {
        this.table = table;
        this.updatedColumns = updatedColumns;
    }

    @Override
    public void open() throws Exception {
        super.open();
        affectedPartitions = new HashSet<>();

        Preconditions.checkState(
                RuntimeContextUtils.getNumberOfParallelSubtasks(getRuntimeContext()) == 1,
                "Parallelism of MergeIntoUpdateChecker must be 1.");
    }

    @Override
    public void processElement(StreamRecord<Committable> element) throws Exception {
        affectedPartitions.add(element.getValue().commitMessage().partition());
        output.collect(element);
    }

    @Override
    public void endInput() throws Exception {
        checkUpdatedColumns();
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

                        Committable committable = new Committable(Long.MAX_VALUE, commitMessage);

                        output.collect(new StreamRecord<>(committable));
                    }
                    break;
                default:
                    throw new UnsupportedOperationException("Unsupported option: " + updateAction);
            }
        }
    }
}
