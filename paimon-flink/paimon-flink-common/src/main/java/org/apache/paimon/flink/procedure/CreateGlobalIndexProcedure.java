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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.flink.btree.BTreeIndexTopoBuilder;
import org.apache.paimon.globalindex.GlobalIndexSingletonWriter;
import org.apache.paimon.globalindex.ResultEntry;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.options.Options;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.SpecialFields;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.table.sink.TableCommitImpl;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.CloseableIterator;
import org.apache.paimon.utils.ParameterUtils;
import org.apache.paimon.utils.Range;

import org.apache.flink.table.annotation.ArgumentHint;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.ProcedureHint;
import org.apache.flink.table.procedure.ProcedureContext;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.apache.paimon.globalindex.GlobalIndexBuilderUtils.createIndexWriter;
import static org.apache.paimon.globalindex.GlobalIndexBuilderUtils.toIndexFileMetas;
import static org.apache.paimon.io.CompactIncrement.emptyIncrement;
import static org.apache.paimon.io.DataIncrement.indexIncrement;
import static org.apache.paimon.utils.ParameterUtils.getPartitions;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Procedure to create global index files via Flink. */
public class CreateGlobalIndexProcedure extends ProcedureBase {

    public static final String IDENTIFIER = "create_global_index";

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
                        isOptional = true),
                @ArgumentHint(name = "options", type = @DataTypeHint("STRING"), isOptional = true)
            })
    public String[] call(
            ProcedureContext procedureContext,
            String tableId,
            String indexColumn,
            String indexType,
            String partitions,
            String options)
            throws Exception {

        FileStoreTable table = (FileStoreTable) table(tableId);

        // Validate table configuration
        checkArgument(
                table.coreOptions().rowTrackingEnabled(),
                "Table '%s' must enable 'row-tracking.enabled=true' before creating global index.",
                tableId);

        RowType rowType = table.rowType();
        checkArgument(
                rowType.containsField(indexColumn),
                "Column '%s' does not exist in table '%s'.",
                indexColumn,
                tableId);

        // Parse partition predicate
        PartitionPredicate partitionPredicate = parsePartitionPredicate(table, partitions);

        // Parse options
        Map<String, String> parsedOptions = optionalConfigMap(options);
        Options userOptions = Options.fromMap(parsedOptions);

        // Build global index based on index type
        indexType = indexType.toLowerCase().trim();
        if ("btree".equals(indexType)) {
            BTreeIndexTopoBuilder.buildIndex(
                    procedureContext.getExecutionEnvironment(),
                    table,
                    indexColumn,
                    partitionPredicate,
                    userOptions);
            return new String[] {
                "BTree global index created successfully for table: " + table.name()
            };
        } else {
            buildGenericIndex(table, indexColumn, indexType, partitionPredicate, userOptions);
            return new String[] {
                indexType + " global index created successfully for table: " + table.name()
            };
        }
    }

    /**
     * Builds a global index using the generic SPI-based GlobalIndexer mechanism. This supports any
     * index type registered via {@link org.apache.paimon.globalindex.GlobalIndexerFactory} (e.g.
     * lumina-vector-ann, bitmap).
     */
    private void buildGenericIndex(
            FileStoreTable table,
            String indexColumn,
            String indexType,
            PartitionPredicate partitionPredicate,
            Options userOptions)
            throws Exception {
        RowType rowType = table.rowType();
        DataField indexField = rowType.getField(indexColumn);
        RowType projectedRowType = rowType.project(Collections.singletonList(indexColumn));
        RowType readRowType = SpecialFields.rowTypeWithRowId(projectedRowType);

        // Merge table options with user options (user options take precedence)
        Options mergedOptions = new Options(table.options(), userOptions.toMap());

        // Scan manifest entries to determine row ranges
        List<ManifestEntry> entries =
                table.store().newScan().withPartitionFilter(partitionPredicate).plan().files();

        // Group by partition
        Map<BinaryRow, List<ManifestEntry>> entriesByPartition =
                entries.stream().collect(Collectors.groupingBy(ManifestEntry::partition));

        long rowsPerShard =
                mergedOptions
                        .getOptional(CoreOptions.GLOBAL_INDEX_ROW_COUNT_PER_SHARD)
                        .orElse(CoreOptions.GLOBAL_INDEX_ROW_COUNT_PER_SHARD.defaultValue());

        List<CommitMessage> allCommitMessages = new ArrayList<>();

        for (Map.Entry<BinaryRow, List<ManifestEntry>> partEntry : entriesByPartition.entrySet()) {
            BinaryRow partition = partEntry.getKey();
            List<ManifestEntry> partEntries = partEntry.getValue();

            // Compute the row range for this partition
            long minRowId = Long.MAX_VALUE;
            long maxRowId = Long.MIN_VALUE;
            List<org.apache.paimon.io.DataFileMeta> dataFiles = new ArrayList<>();
            for (ManifestEntry entry : partEntries) {
                org.apache.paimon.io.DataFileMeta file = entry.file();
                if (file.firstRowId() == null) {
                    continue;
                }
                Range fileRange = file.nonNullRowIdRange();
                minRowId = Math.min(minRowId, fileRange.from);
                maxRowId = Math.max(maxRowId, fileRange.to);
                dataFiles.add(file);
            }
            if (dataFiles.isEmpty()) {
                continue;
            }

            dataFiles.sort(
                    Comparator.comparingLong(org.apache.paimon.io.DataFileMeta::nonNullFirstRowId));
            Range partitionRange = new Range(minRowId, maxRowId);

            // Build a DataSplit covering all files in this partition
            DataSplit dataSplit =
                    DataSplit.builder()
                            .withPartition(partition)
                            .withBucket(0)
                            .withDataFiles(dataFiles)
                            .withBucketPath(
                                    table.store().pathFactory().bucketPath(partition, 0).toString())
                            .rawConvertible(false)
                            .build();

            // Read data and write index
            ReadBuilder readBuilder = table.newReadBuilder().withReadType(readRowType);
            GlobalIndexSingletonWriter indexWriter =
                    (GlobalIndexSingletonWriter)
                            createIndexWriter(table, indexType, indexField, mergedOptions);

            InternalRow.FieldGetter getter =
                    InternalRow.createFieldGetter(
                            indexField.type(), readRowType.getFieldIndex(indexField.name()));

            try (RecordReader<InternalRow> reader = readBuilder.newRead().createReader(dataSplit);
                    CloseableIterator<InternalRow> iter = reader.toCloseableIterator()) {
                while (iter.hasNext()) {
                    InternalRow row = iter.next();
                    indexWriter.write(getter.getFieldOrNull(row));
                }
            }

            List<ResultEntry> resultEntries = indexWriter.finish();
            List<IndexFileMeta> indexFileMetas =
                    toIndexFileMetas(
                            table, partitionRange, indexField.id(), indexType, resultEntries);
            allCommitMessages.add(
                    new CommitMessageImpl(
                            partition, 0, null, indexIncrement(indexFileMetas), emptyIncrement()));
        }

        // Commit all index files
        try (TableCommitImpl commit = table.newCommit("global-index-create-" + UUID.randomUUID())) {
            commit.commit(allCommitMessages);
        }
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
