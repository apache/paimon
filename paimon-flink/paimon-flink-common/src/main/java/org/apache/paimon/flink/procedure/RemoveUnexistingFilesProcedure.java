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
import org.apache.paimon.flink.action.RemoveUnexistingFilesAction;

import org.apache.flink.table.annotation.ArgumentHint;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.ProcedureHint;
import org.apache.flink.table.procedure.ProcedureContext;
import org.apache.flink.util.CloseableIterator;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;

/**
 * Procedure to remove unexisting data files from manifest entries. See {@link
 * RemoveUnexistingFilesAction} for detailed use cases.
 *
 * <pre><code>
 *  -- remove unexisting data files in table `mydb.myt`
 *  CALL sys.remove_unexisting_files(`table` => 'mydb.myt')
 *
 *  -- only check what files will be removed, but not really remove them (dry run)
 *  CALL sys.remove_unexisting_files(`table` => 'mydb.myt', `dry_run` = true)
 * </code></pre>
 *
 * <p>Note that user is on his own risk using this procedure, which may cause data loss when used
 * outside from the use cases above.
 */
public class RemoveUnexistingFilesProcedure extends ProcedureBase {

    public static final String IDENTIFIER = "remove_unexisting_files";

    @ProcedureHint(
            argument = {
                @ArgumentHint(name = "table", type = @DataTypeHint("STRING")),
                @ArgumentHint(name = "dry_run", type = @DataTypeHint("BOOLEAN"), isOptional = true),
                @ArgumentHint(name = "parallelism", type = @DataTypeHint("INT"), isOptional = true)
            })
    public String[] call(
            ProcedureContext procedureContext,
            String tableId,
            @Nullable Boolean dryRun,
            @Nullable Integer parallelism)
            throws Exception {
        Identifier identifier = Identifier.fromString(tableId);
        String databaseName = identifier.getDatabaseName();
        String tableName = identifier.getObjectName();

        RemoveUnexistingFilesAction action =
                new RemoveUnexistingFilesAction(databaseName, tableName, catalog.options());
        if (Boolean.TRUE.equals(dryRun)) {
            action.dryRun();
        }
        if (parallelism != null) {
            action.withParallelism(parallelism);
        }
        action.withStreamExecutionEnvironment(procedureContext.getExecutionEnvironment());

        List<String> result = new ArrayList<>();
        try (CloseableIterator<String> it =
                action.buildDataStream()
                        .executeAndCollect("Remove Unexisting Files : " + tableName)) {
            it.forEachRemaining(result::add);
        }
        return result.toArray(new String[0]);
    }

    @Override
    public String identifier() {
        return IDENTIFIER;
    }
}
