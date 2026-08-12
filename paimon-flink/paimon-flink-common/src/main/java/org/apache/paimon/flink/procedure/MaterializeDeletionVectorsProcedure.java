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
import org.apache.paimon.flink.action.MaterializeDeletionVectorsAction;

import org.apache.flink.table.annotation.ArgumentHint;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.ProcedureHint;
import org.apache.flink.table.procedure.ProcedureContext;

import java.util.Collections;
import java.util.Map;

import static org.apache.paimon.utils.ParameterUtils.getPartitions;
import static org.apache.paimon.utils.ParameterUtils.parseCommaSeparatedKeyValues;
import static org.apache.paimon.utils.StringUtils.isNullOrWhitespaceOnly;

/** Procedure which physically applies deletion vectors and assigns new row IDs. */
public class MaterializeDeletionVectorsProcedure extends ProcedureBase {

    public static final String IDENTIFIER = "materialize_deletion_vectors";

    @ProcedureHint(
            argument = {
                @ArgumentHint(name = "table", type = @DataTypeHint("STRING")),
                @ArgumentHint(
                        name = "partitions",
                        type = @DataTypeHint("STRING"),
                        isOptional = true),
                @ArgumentHint(name = "options", type = @DataTypeHint("STRING"), isOptional = true),
                @ArgumentHint(name = "where", type = @DataTypeHint("STRING"), isOptional = true)
            })
    public String[] call(
            ProcedureContext procedureContext,
            String tableId,
            String partitions,
            String tableOptions,
            String where)
            throws Exception {
        Map<String, String> tableConf =
                isNullOrWhitespaceOnly(tableOptions)
                        ? Collections.emptyMap()
                        : parseCommaSeparatedKeyValues(tableOptions);
        Identifier identifier = Identifier.fromString(tableId);
        MaterializeDeletionVectorsAction action =
                new MaterializeDeletionVectorsAction(
                        identifier.getDatabaseName(),
                        identifier.getObjectName(),
                        catalog.options(),
                        tableConf);
        if (!isNullOrWhitespaceOnly(partitions)) {
            action.withPartitions(getPartitions(partitions.split(";")));
        }
        if (!isNullOrWhitespaceOnly(where)) {
            action.withWhereSql(where);
        }
        return execute(
                procedureContext,
                action,
                "Materialize Deletion Vectors : " + identifier.getFullName());
    }

    @Override
    public String identifier() {
        return IDENTIFIER;
    }
}
