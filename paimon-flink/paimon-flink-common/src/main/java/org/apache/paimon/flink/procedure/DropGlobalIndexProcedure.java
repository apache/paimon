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

package org.apache.paimon.flink.procedure;

import org.apache.paimon.Snapshot;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.table.sink.TableCommitImpl;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Filter;
import org.apache.paimon.utils.ParameterUtils;

import org.apache.flink.table.annotation.ArgumentHint;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.ProcedureHint;
import org.apache.flink.table.procedure.ProcedureContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.apache.paimon.utils.ParameterUtils.getPartitions;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Procedure to drop global index files via Flink. */
public class DropGlobalIndexProcedure extends ProcedureBase {

    private static final Logger LOG = LoggerFactory.getLogger(DropGlobalIndexProcedure.class);

    public static final String IDENTIFIER = "drop_global_index";

    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    @ProcedureHint(
            argument = {
                @ArgumentHint(name = "table", type = @DataTypeHint("STRING")),
                @ArgumentHint(name = "index_column", type = @DataTypeHint("STRING")),
                @ArgumentHint(name = "index_type", type = @DataTypeHint("STRING")),
                @ArgumentHint(
                        name = "partitions",
                        type = @DataTypeHint("STRING"),
                        isOptional = true)
            })
    public String[] call(
            ProcedureContext procedureContext,
            String tableId,
            String indexColumn,
            String indexType,
            String partitions)
            throws Exception {

        FileStoreTable table = (FileStoreTable) table(tableId);

        // Validate column exists
        RowType rowType = table.rowType();
        checkArgument(
                rowType.containsField(indexColumn),
                "Column '%s' does not exist in table '%s'.",
                indexColumn,
                tableId);

        // Parse partition predicate
        PartitionPredicate partitionPredicate = parsePartitionPredicate(table, partitions);

        // Normalize index type
        final String indexTypeLower = indexType.toLowerCase().trim();

        // Get column field ID for final reference in lambda
        final int columnId = rowType.getField(indexColumn).id();

        // Get latest snapshot
        Snapshot snapshot =
                table.latestSnapshot()
                        .orElseThrow(
                                () ->
                                        new IllegalStateException(
                                                String.format(
                                                        "Table '%s' has no snapshot.", tableId)));

        // Create filter for index entries to delete
        Filter<IndexManifestEntry> filter =
                entry ->
                        entry.indexFile().indexType().equals(indexTypeLower)
                                && entry.indexFile().globalIndexMeta() != null
                                && entry.indexFile().globalIndexMeta().indexFieldId() == columnId
                                && (partitionPredicate == null
                                        || partitionPredicate.test(entry.partition()));

        // Scan for index files to delete
        List<IndexManifestEntry> waitToDelete =
                table.store().newIndexFileHandler().scan(snapshot, filter);

        LOG.info(
                "Found {} {} global index files to delete for column '{}' on table '{}'",
                waitToDelete.size(),
                indexTypeLower,
                indexColumn,
                table.name());

        if (waitToDelete.isEmpty()) {
            return new String[] {
                "No " + indexTypeLower + " global index found for column '" + indexColumn + "'"
            };
        }

        // Group index files by partition
        Map<BinaryRow, List<IndexFileMeta>> deleteEntries =
                waitToDelete.stream()
                        .map(IndexManifestEntry::toDeleteEntry)
                        .collect(
                                Collectors.groupingBy(
                                        IndexManifestEntry::partition,
                                        Collectors.mapping(
                                                IndexManifestEntry::indexFile,
                                                Collectors.toList())));

        // Create commit messages
        List<CommitMessage> commitMessages = new ArrayList<>();
        for (Map.Entry<BinaryRow, List<IndexFileMeta>> entry : deleteEntries.entrySet()) {
            BinaryRow partition = entry.getKey();
            List<IndexFileMeta> indexFileMetas = entry.getValue();
            commitMessages.add(
                    new CommitMessageImpl(
                            partition,
                            0,
                            null,
                            DataIncrement.deleteIndexIncrement(indexFileMetas),
                            CompactIncrement.emptyIncrement()));
        }

        // Commit the deletion
        try (TableCommitImpl commit = table.newCommit("drop-global-index-" + UUID.randomUUID())) {
            commit.commit(commitMessages);
        }

        LOG.info(
                "Successfully dropped {} {} global index files for column '{}' on table '{}'",
                waitToDelete.size(),
                indexTypeLower,
                indexColumn,
                table.name());

        return new String[] {
            "Dropped "
                    + waitToDelete.size()
                    + " "
                    + indexTypeLower
                    + " global index files for column '"
                    + indexColumn
                    + "' on table '"
                    + table.name()
                    + "'"
        };
    }

    private PartitionPredicate parsePartitionPredicate(FileStoreTable table, String partitions) {
        if (partitions == null || partitions.isEmpty()) {
            return null;
        }

        List<Map<String, String>> partitionList = getPartitions(partitions.split(";"));
        Predicate predicate =
                ParameterUtils.toPartitionPredicate(
                        partitionList,
                        table.schema().logicalPartitionType(),
                        table.coreOptions().partitionDefaultName());
        return PartitionPredicate.fromPredicate(table.schema().logicalPartitionType(), predicate);
    }
}
