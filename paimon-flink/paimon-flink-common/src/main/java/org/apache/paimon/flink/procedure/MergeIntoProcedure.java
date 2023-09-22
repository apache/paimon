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

import org.apache.paimon.catalog.AbstractCatalog;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.flink.action.MergeIntoAction;
import org.apache.paimon.flink.action.MergeIntoActionFactory;

import org.apache.flink.table.procedure.ProcedureContext;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.paimon.flink.action.MergeIntoActionFactory.MATCHED_DELETE;
import static org.apache.paimon.flink.action.MergeIntoActionFactory.MATCHED_UPSERT;
import static org.apache.paimon.flink.action.MergeIntoActionFactory.NOT_MATCHED_BY_SOURCE_DELETE;
import static org.apache.paimon.flink.action.MergeIntoActionFactory.NOT_MATCHED_BY_SOURCE_UPSERT;
import static org.apache.paimon.flink.action.MergeIntoActionFactory.NOT_MATCHED_INSERT;
import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.paimon.utils.Preconditions.checkNotNull;

/**
 * Merge Into procedure. Usage:
 *
 * <pre><code>
 *  CALL merge_into(
 *      'targetTableId',
 *      'targetAlias',
 *      'sourceSqls', -- separate with ';'
 *      'sourceTable',
 *      'mergeCondition',
 *      'mergeActions',
 *      // arguments for merge actions
 *      ...
 *      )
 *
 *  -- merge actions and corresponding arguments
 *  matched-upsert: condition, set
 *  not-matched-by-source-upsert: condition, set
 *  matched-delete: condition
 *  not-matched-by-source-delete: condition
 *  not-matched-insert: condition, insertValues
 *
 *  -- NOTE: the arguments should be in the order of merge actions
 *  -- and use '' as placeholder for optional arguments
 * </code></pre>
 *
 * <p>This procedure will be forced to use batch environments
 */
public class MergeIntoProcedure extends ProcedureBase {

    public static final String NAME = "merge_into";

    public MergeIntoProcedure(Catalog catalog) {
        super(catalog);
    }

    public String[] call(
            ProcedureContext procedureContext,
            String targetTableId,
            String targetAlias,
            String sourceSqls,
            String sourceTable,
            String mergeCondition,
            String mergeActions,
            String... mergeActionArguments)
            throws Exception {
        String warehouse = ((AbstractCatalog) catalog).warehouse();
        Map<String, String> catalogOptions = ((AbstractCatalog) catalog).options();
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

        checkArgument(!mergeActions.isEmpty(), "Must specify at least one merge action.");
        List<String> actions =
                Arrays.stream(mergeActions.split(","))
                        .map(String::trim)
                        .collect(Collectors.toList());
        validateActions(actions, mergeActionArguments.length);

        int index = 0;
        String condition, setting;
        for (String mergeAction : actions) {
            switch (mergeAction) {
                case MATCHED_UPSERT:
                case NOT_MATCHED_BY_SOURCE_UPSERT:
                case NOT_MATCHED_INSERT:
                    condition = nullable(mergeActionArguments[index++]);
                    setting = nullable(mergeActionArguments[index++]);
                    checkNotNull(setting, "%s must set the second argument", mergeAction);
                    setMergeAction(action, mergeAction, condition, setting);
                    break;
                case MATCHED_DELETE:
                case NOT_MATCHED_BY_SOURCE_DELETE:
                    condition = nullable(mergeActionArguments[index++]);
                    setMergeAction(action, mergeAction, condition);
                    break;
                default:
                    throw new UnsupportedOperationException("Unknown merge action: " + action);
            }
        }

        MergeIntoActionFactory.validate(action);

        // TODO set dml-sync argument to action
        action.run();

        return new String[] {"Success"};
    }

    private void validateActions(List<String> mergeActions, int argumentLength) {
        int expectedArguments = 0;
        for (String action : mergeActions) {
            switch (action) {
                case MATCHED_UPSERT:
                case NOT_MATCHED_BY_SOURCE_UPSERT:
                case NOT_MATCHED_INSERT:
                    expectedArguments += 2;
                    break;
                case MATCHED_DELETE:
                case NOT_MATCHED_BY_SOURCE_DELETE:
                    expectedArguments += 1;
                    break;
                default:
                    throw new UnsupportedOperationException("Unknown merge action: " + action);
            }
        }

        checkArgument(
                expectedArguments == argumentLength,
                "Expected %s action arguments but given '%s'",
                expectedArguments,
                argumentLength);
    }

    private void setMergeAction(MergeIntoAction action, String mergeAction, String... arguments) {
        switch (mergeAction) {
            case MATCHED_UPSERT:
                action.withMatchedUpsert(arguments[0], arguments[1]);
                return;
            case NOT_MATCHED_BY_SOURCE_UPSERT:
                action.withNotMatchedBySourceUpsert(arguments[0], arguments[1]);
                return;
            case NOT_MATCHED_INSERT:
                action.withNotMatchedInsert(arguments[0], arguments[1]);
                return;
            case MATCHED_DELETE:
                action.withMatchedDelete(arguments[0]);
                return;
            case NOT_MATCHED_BY_SOURCE_DELETE:
                action.withNotMatchedBySourceDelete(arguments[0]);
                return;
            default:
                throw new UnsupportedOperationException("Unknown merge action: " + mergeAction);
        }
    }
}
