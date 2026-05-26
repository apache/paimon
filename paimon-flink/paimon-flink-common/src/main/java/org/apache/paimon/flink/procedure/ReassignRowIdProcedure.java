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

import org.apache.paimon.append.dataevolution.DataEvolutionRowIdReassigner;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.flink.action.ReassignRowIdAction;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.utils.ParameterUtils;

import org.apache.flink.table.annotation.ArgumentHint;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.ProcedureHint;
import org.apache.flink.table.procedure.ProcedureContext;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Procedure to reassign row IDs for data evolution tables. */
public class ReassignRowIdProcedure extends ProcedureBase {

    public static final String IDENTIFIER = "reassign_row_id";

    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    @ProcedureHint(argument = {@ArgumentHint(name = "table", type = @DataTypeHint("STRING"))})
    public String[] call(ProcedureContext procedureContext, String tableId)
            throws Catalog.TableNotExistException {
        return reassign(tableId, Collections.emptyList());
    }

    @ProcedureHint(
            argument = {
                @ArgumentHint(name = "table", type = @DataTypeHint("STRING")),
                @ArgumentHint(name = "partitions", type = @DataTypeHint("STRING"))
            })
    public String[] call(ProcedureContext procedureContext, String tableId, String partitions)
            throws Catalog.TableNotExistException {
        return reassign(tableId, ParameterUtils.getPartitions(partitions.split(";")));
    }

    private String[] reassign(String tableId, List<Map<String, String>> partitionSpecs)
            throws Catalog.TableNotExistException {
        Table table = table(tableId);
        checkArgument(
                table instanceof FileStoreTable,
                "Procedure '%s' only supports file store table, but table '%s' is %s.",
                IDENTIFIER,
                tableId,
                table.getClass().getName());

        DataEvolutionRowIdReassigner.Result result =
                new DataEvolutionRowIdReassigner(
                                (FileStoreTable) table,
                                ReassignRowIdAction.toPartitionPredicate(
                                        (FileStoreTable) table, partitionSpecs))
                        .reassign();
        return new String[] {ReassignRowIdAction.formatResult(tableId, result)};
    }
}
