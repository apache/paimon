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

import org.apache.paimon.flink.btree.BTreeIndexTopoBuilder;
import org.apache.paimon.options.Options;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.ParameterUtils;

import org.apache.flink.table.annotation.ArgumentHint;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.ProcedureHint;
import org.apache.flink.table.procedure.ProcedureContext;

import java.util.List;
import java.util.Map;

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
            throw new UnsupportedOperationException(
                    "Unsupported index type: " + indexType + ". Supported types: btree, bitmap");
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
