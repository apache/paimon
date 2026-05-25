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

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.table.Table;

import org.apache.flink.table.annotation.ArgumentHint;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.ProcedureHint;
import org.apache.flink.table.procedure.ProcedureContext;

import javax.annotation.Nullable;

/**
 * Procedure for merging branches. Usage:
 *
 * <pre><code>
 *  CALL sys.merge_branch('tableId', 'sourceBranch', 'targetBranch')
 * </code></pre>
 */
public class MergeBranchProcedure extends ProcedureBase {

    public static final String IDENTIFIER = "merge_branch";

    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    @ProcedureHint(
            argument = {
                @ArgumentHint(name = "table", type = @DataTypeHint("STRING")),
                @ArgumentHint(name = "source_branch", type = @DataTypeHint("STRING")),
                @ArgumentHint(
                        name = "target_branch",
                        type = @DataTypeHint("STRING"),
                        isOptional = true)
            })
    public String[] call(
            ProcedureContext procedureContext,
            String tableId,
            String sourceBranch,
            @Nullable String targetBranch)
            throws Catalog.TableNotExistException {
        if (targetBranch == null) {
            targetBranch = Identifier.DEFAULT_MAIN_BRANCH;
        }
        Table table = catalog.getTable(Identifier.fromString(tableId));
        table.mergeBranch(sourceBranch, targetBranch);
        return new String[] {"Success"};
    }
}
