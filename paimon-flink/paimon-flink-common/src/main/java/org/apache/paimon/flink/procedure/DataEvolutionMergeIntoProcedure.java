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
import org.apache.paimon.flink.action.DataEvolutionMergeIntoAction;
import org.apache.paimon.utils.Preconditions;

import org.apache.flink.core.execution.JobClient;
import org.apache.flink.table.annotation.ArgumentHint;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.ProcedureHint;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.procedure.ProcedureContext;

import java.util.Map;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * The MergeInto Procedure specially implemented for data evolution table. Please see {@code
 * DataEvolutionMergeIntoAction} for more information.
 *
 * <pre><code>
 *  -- NOTE: use '' as placeholder for optional arguments
 *  CALL sys.data_evolution_merge_into(
 *      'targetTableId',     --required
 *      'targetAlias',       --optional
 *      'sourceSqls',        --optional
 *      'sourceTable',       --required
 *      'mergeCondition',    --required
 *      'matchedUpdateSet',  --required
 *      'sinkParallelism'    --required
 *  )
 * </code></pre>
 *
 * <p>This procedure will be forced to use batch environments.
 */
public class DataEvolutionMergeIntoProcedure extends ProcedureBase {

    public static final String IDENTIFIER = "data_evolution_merge_into";

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
                        name = "matched_update_set",
                        type = @DataTypeHint("STRING"),
                        isOptional = true),
                @ArgumentHint(
                        name = "sink_parallelism",
                        type = @DataTypeHint("INTEGER"),
                        isOptional = true)
            })
    public String[] call(
            ProcedureContext procedureContext,
            String targetTableId,
            String targetAlias,
            String sourceSqls,
            String sourceTable,
            String mergeCondition,
            String matched_update_set,
            Integer sinkParallelism) {
        targetTableId = notnull(targetTableId);
        targetAlias = notnull(targetAlias);
        sourceSqls = notnull(sourceSqls);
        sourceTable = notnull(sourceTable);
        mergeCondition = notnull(mergeCondition);
        matched_update_set = notnull(matched_update_set);
        Preconditions.checkArgument(sinkParallelism != null && sinkParallelism > 0);

        Map<String, String> catalogOptions = catalog.options();
        Identifier identifier = Identifier.fromString(targetTableId);
        DataEvolutionMergeIntoAction action =
                new DataEvolutionMergeIntoAction(
                        identifier.getDatabaseName(), identifier.getObjectName(), catalogOptions);

        action.withTargetAlias(nullable(targetAlias));

        if (!sourceSqls.isEmpty()) {
            action.withSourceSqls(sourceSqls.split(";"));
        }

        checkArgument(!sourceTable.isEmpty(), "Must specify source table.");
        action.withSourceTable(sourceTable);

        checkArgument(!mergeCondition.isEmpty(), "Must specify merge condition.");
        action.withMergeCondition(mergeCondition);

        checkArgument(!matched_update_set.isEmpty(), "Must specify matched update set.");
        action.withMatchedUpdateSet(matched_update_set);

        action.withSinkParallelism(sinkParallelism);

        action.withStreamExecutionEnvironment(procedureContext.getExecutionEnvironment());

        TableResult result = action.runInternal();
        JobClient jobClient = result.getJobClient().get();

        return execute(procedureContext, jobClient);
    }

    @Override
    public String identifier() {
        return IDENTIFIER;
    }
}
