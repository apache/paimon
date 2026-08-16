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

package org.apache.paimon.spark.procedure;

import org.apache.paimon.Snapshot;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.spark.utils.SparkProcedureUtils;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.table.sink.TableCommitImpl;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Filter;
import org.apache.paimon.utils.StringUtils;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.spark.sql.types.DataTypes.StringType;

/** Procedure to drop global index files via Spark. */
public class DropGlobalIndexProcedure extends BaseProcedure {

    private static final Logger LOG = LoggerFactory.getLogger(DropGlobalIndexProcedure.class);

    private static final ProcedureParameter[] PARAMETERS =
            new ProcedureParameter[] {
                ProcedureParameter.required("table", DataTypes.StringType),
                ProcedureParameter.required("index_column", DataTypes.StringType),
                ProcedureParameter.required("index_type", DataTypes.StringType),
                ProcedureParameter.optional("partitions", StringType),
                ProcedureParameter.optional("dry_run", DataTypes.BooleanType),
            };

    private static final StructType OUTPUT_TYPE =
            new StructType(
                    new StructField[] {
                        new StructField("result", DataTypes.BooleanType, true, Metadata.empty()),
                        new StructField(
                                "dropped_file_count", DataTypes.LongType, true, Metadata.empty())
                    });

    protected DropGlobalIndexProcedure(TableCatalog tableCatalog) {
        super(tableCatalog);
    }

    @Override
    public ProcedureParameter[] parameters() {
        return PARAMETERS;
    }

    @Override
    public StructType outputType() {
        return OUTPUT_TYPE;
    }

    @Override
    public String description() {
        return "Drop global index files for a given column.";
    }

    @Override
    public InternalRow[] call(InternalRow args) {
        Identifier tableIdent = toIdentifier(args.getString(0), PARAMETERS[0].name());
        String column = args.getString(1);
        String indexType = args.getString(2).toLowerCase(Locale.ROOT).trim();
        String partitions =
                (args.isNullAt(3) || StringUtils.isNullOrWhitespaceOnly(args.getString(3)))
                        ? null
                        : args.getString(3);
        boolean dryRun = !args.isNullAt(4) && args.getBoolean(4);

        LOG.info("Starting to drop index for table {} with partitions: {}", tableIdent, partitions);

        List<String> indexColumns =
                Arrays.stream(column.split(","))
                        .map(String::trim)
                        .filter(s -> !s.isEmpty())
                        .collect(Collectors.toList());
        checkArgument(!indexColumns.isEmpty(), "At least one column required.");

        return modifyPaimonTable(
                tableIdent,
                t -> {
                    try {
                        checkArgument(
                                t instanceof FileStoreTable,
                                "Only FileStoreTable supports global index creation.");
                        FileStoreTable table = (FileStoreTable) t;

                        RowType rowType = table.rowType();
                        for (String col : indexColumns) {
                            checkArgument(
                                    rowType.containsField(col),
                                    "Column '%s' does not exist in table '%s'.",
                                    col,
                                    tableIdent);
                        }
                        List<Integer> indexFieldIds =
                                indexColumns.stream()
                                        .map(col -> rowType.getField(col).id())
                                        .collect(Collectors.toList());
                        DataSourceV2Relation relation = createRelation(tableIdent);
                        PartitionPredicate partitionPredicate =
                                SparkProcedureUtils.convertPartitionsToPartitionPredicate(
                                        partitions, table, spark());

                        Snapshot snapshot =
                                t.latestSnapshot()
                                        .orElseThrow(
                                                () ->
                                                        new IllegalStateException(
                                                                String.format(
                                                                        "Table '%s' has no snapshot.",
                                                                        tableIdent)));

                        Filter<IndexManifestEntry> filter =
                                entry ->
                                        entry.indexFile().indexType().equals(indexType)
                                                && entry.indexFile().globalIndexMeta() != null
                                                && entry.indexFile()
                                                        .globalIndexMeta()
                                                        .getIndexedFieldIds()
                                                        .equals(indexFieldIds)
                                                && (partitionPredicate == null
                                                        || partitionPredicate.test(
                                                                entry.partition()));

                        List<IndexManifestEntry> waitDelete =
                                table.store().newIndexFileHandler().scan(snapshot, filter);

                        LOG.info(
                                "Waiting for global index to be deleted size: "
                                        + waitDelete.size());

                        // Dry run: report how many would be dropped, commit nothing.
                        if (dryRun) {
                            return new InternalRow[] {
                                newInternalRow(true, (long) waitDelete.size())
                            };
                        }

                        // Nothing matched: avoid committing an empty change.
                        if (waitDelete.isEmpty()) {
                            return new InternalRow[] {newInternalRow(true, 0L)};
                        }

                        Map<BinaryRow, List<IndexFileMeta>> deleteEntries =
                                waitDelete.stream()
                                        .map(IndexManifestEntry::toDeleteEntry)
                                        .collect(
                                                Collectors.groupingBy(
                                                        IndexManifestEntry::partition,
                                                        Collectors.mapping(
                                                                IndexManifestEntry::indexFile,
                                                                Collectors.toList())));

                        List<CommitMessage> commitMessages = new ArrayList<>();

                        for (Map.Entry<BinaryRow, List<IndexFileMeta>> entry :
                                deleteEntries.entrySet()) {
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

                        try (TableCommitImpl commit =
                                table.newCommit("drop-global-index-" + UUID.randomUUID())) {
                            commit.commit(commitMessages);
                        }

                        return new InternalRow[] {newInternalRow(true, (long) waitDelete.size())};
                    } catch (Exception e) {
                        throw new RuntimeException(
                                String.format(
                                        "Failed to drop %s index for columns '%s' on table '%s'.",
                                        indexType, String.join(",", indexColumns), tableIdent),
                                e);
                    }
                });
    }

    public static ProcedureBuilder builder() {
        return new Builder<DropGlobalIndexProcedure>() {
            @Override
            public DropGlobalIndexProcedure doBuild() {
                return new DropGlobalIndexProcedure(tableCatalog());
            }
        };
    }
}
