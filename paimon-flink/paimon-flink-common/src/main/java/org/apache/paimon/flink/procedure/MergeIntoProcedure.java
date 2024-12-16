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

import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.flink.action.MergeIntoAction;

import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.annotation.ArgumentHint;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.ProcedureHint;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.procedure.ProcedureContext;

import java.util.Map;

import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.paimon.utils.Preconditions.checkNotNull;

/**
 * Merge Into procedure. Usage:
 *
 * <pre><code>
 *  -- NOTE: use '' as placeholder for optional arguments
 *  -- IMPORTANT: Use 'TRUE' if you want to delete data without filter condition.
 *  -- If matchedDeleteCondition='', it will ignore matched-delete action!
 *  CALL sys.merge_into(
 *      'targetTableId',
 *      'targetAlias',
 *      'sourceSqls',
 *      'sourceTable',
 *      'mergeCondition',
 *      'matchedUpsertCondition',
 *      'matchedUpsertSetting',
 *      'notMatchedInsertCondition',
 *      'notMatchedInsertValues',
 *      'matchedDeleteCondition'
 *  )
 * </code></pre>
 *
 * <p>This procedure will be forced to use batch environments. Compared to {@link MergeIntoAction},
 * this procedure doesn't provide arguments to control not-matched-by-source behavior because they
 * are not commonly used and will make the methods too complex to use.
 */
public class MergeIntoProcedure extends ProcedureBase {

    public static final String IDENTIFIER = "merge_into";

    @ProcedureHint(
            argument = {
                @ArgumentHint(name = "target_table", type = @DataTypeHint("STRING")),
                @ArgumentHint(
                        name = "target_alias",
                        type = @DataTypeHint("STRING"),
                        isOptional = true),
                @ArgumentHint(
                        name = "source_sqls",
                        type = @DataTypeHint("STRING"),
                        isOptional = true),
                @ArgumentHint(
                        name = "source_table",
                        type = @DataTypeHint("STRING"),
                        isOptional = true),
                @ArgumentHint(
                        name = "merge_condition",
                        type = @DataTypeHint("STRING"),
                        isOptional = true),
                @ArgumentHint(
                        name = "matched_upsert_condition",
                        type = @DataTypeHint("STRING"),
                        isOptional = true),
                @ArgumentHint(
                        name = "matched_upsert_setting",
                        type = @DataTypeHint("STRING"),
                        isOptional = true),
                @ArgumentHint(
                        name = "not_matched_insert_condition",
                        type = @DataTypeHint("STRING"),
                        isOptional = true),
                @ArgumentHint(
                        name = "not_matched_insert_values",
                        type = @DataTypeHint("STRING"),
                        isOptional = true),
                @ArgumentHint(
                        name = "matched_delete_condition",
                        type = @DataTypeHint("STRING"),
                        isOptional = true),
                @ArgumentHint(
                        name = "not_matched_by_source_upsert_condition",
                        type = @DataTypeHint("STRING"),
                        isOptional = true),
                @ArgumentHint(
                        name = "not_matched_by_source_upsert_setting",
                        type = @DataTypeHint("STRING"),
                        isOptional = true),
                @ArgumentHint(
                        name = "not_matched_by_source_delete_condition",
                        type = @DataTypeHint("STRING"),
                        isOptional = true),
            })
    public String[] call(
            ProcedureContext procedureContext,
            String targetTableId,
            String targetAlias,
            String sourceSqls,
            String sourceTable,
            String mergeCondition,
            String matchedUpsertCondition,
            String matchedUpsertSetting,
            String notMatchedInsertCondition,
            String notMatchedInsertValues,
            String matchedDeleteCondition,
            String notMatchedBySourceUpsertCondition,
            String notMatchedBySourceUpsertSetting,
            String notMatchedBySourceDeleteCondition) {
        targetAlias = notnull(targetAlias);
        sourceSqls = notnull(sourceSqls);
        sourceTable = notnull(sourceTable);
        mergeCondition = notnull(mergeCondition);
        matchedUpsertCondition = notnull(matchedUpsertCondition);
        matchedUpsertSetting = notnull(matchedUpsertSetting);
        notMatchedInsertCondition = notnull(notMatchedInsertCondition);
        notMatchedInsertValues = notnull(notMatchedInsertValues);
        matchedDeleteCondition = notnull(matchedDeleteCondition);
        notMatchedBySourceUpsertCondition = notnull(notMatchedBySourceUpsertCondition);
        notMatchedBySourceUpsertSetting = notnull(notMatchedBySourceUpsertSetting);
        notMatchedBySourceDeleteCondition = notnull(notMatchedBySourceDeleteCondition);

        String warehouse = catalog.warehouse();
        Map<String, String> catalogOptions = catalog.options();
        Identifier identifier = Identifier.fromString(targetTableId);
        MergeIntoAction action =
                new MergeIntoAction(
                        warehouse,
                        identifier.getDatabaseName(),
                        identifier.getObjectName(),
                        catalogOptions);
        action.withTargetAlias(nullable(targetAlias));

        if (!sourceSqls.isEmpty()) {
            action.withSourceSqls(sourceSqls.split(";"));
        }

        checkArgument(!sourceTable.isEmpty(), "Must specify source table.");
        action.withSourceTable(sourceTable);

        checkArgument(!mergeCondition.isEmpty(), "Must specify merge condition.");
        action.withMergeCondition(mergeCondition);

        if (!matchedUpsertCondition.isEmpty() || !matchedUpsertSetting.isEmpty()) {
            String condition = nullable(matchedUpsertCondition);
            String setting = nullable(matchedUpsertSetting);
            checkNotNull(setting, "matched-upsert must set the 'matchedUpsertSetting' argument");
            action.withMatchedUpsert(condition, setting);
        }

        if (!notMatchedInsertCondition.isEmpty() || !notMatchedInsertValues.isEmpty()) {
            String condition = nullable(notMatchedInsertCondition);
            String values = nullable(notMatchedInsertValues);
            checkNotNull(
                    values, "not-matched-insert must set the 'notMatchedInsertValues' argument");
            action.withNotMatchedInsert(condition, values);
        }

        if (!matchedDeleteCondition.isEmpty()) {
            action.withMatchedDelete(matchedDeleteCondition);
        }

        if (!notMatchedBySourceUpsertCondition.isEmpty()
                || !notMatchedBySourceUpsertSetting.isEmpty()) {
            String condition = nullable(notMatchedBySourceUpsertCondition);
            String values = nullable(notMatchedBySourceUpsertSetting);
            checkArgument(
                    !"*".equals(values),
                    "not-matched-by-source-upsert does not support setting notMatchedBySourceUpsertSetting to *.");
            action.withNotMatchedBySourceUpsert(condition, values);
        }

        if (!notMatchedBySourceDeleteCondition.isEmpty()) {
            action.withNotMatchedBySourceDelete(notMatchedBySourceDeleteCondition);
        }

        action.withStreamExecutionEnvironment(procedureContext.getExecutionEnvironment());
        action.validate();

        DataStream<RowData> dataStream = action.buildDataStream();
        TableResult tableResult = action.batchSink(dataStream);
        JobClient jobClient = tableResult.getJobClient().get();

        return execute(procedureContext, jobClient);
    }

    @Override
    public String identifier() {
        return IDENTIFIER;
    }
}
