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
import org.apache.paimon.flink.action.RemoveUnexistingManifestsAction;

import org.apache.flink.table.annotation.ArgumentHint;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.ProcedureHint;
import org.apache.flink.table.procedure.ProcedureContext;

/**
 * Procedure to remove unexisting manifest files from manifest list. See {@link
 * RemoveUnexistingManifestsAction} for detailed use cases.
 *
 * <pre><code>
 *  -- remove unexisting data files in table `mydb.myt`
 *  CALL sys.remove_unexisting_manifests(`table` => 'mydb.myt$branch_rt')
 * </code></pre>
 *
 * <p>Note that user is on his own risk using this procedure, which may cause data loss when used
 * outside from the use cases above.
 */
public class RemoveUnexistingManifestsProcedure extends ProcedureBase {

    public static final String IDENTIFIER = "remove_unexisting_manifests";

    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    @ProcedureHint(
            argument = {
                @ArgumentHint(name = "tableId", type = @DataTypeHint("STRING")),
            })
    public String[] call(ProcedureContext procedureContext, String tableId)
            throws Catalog.TableNotExistException {
        Identifier identifier = Identifier.fromString(tableId);
        String databaseName = identifier.getDatabaseName();
        String tableName = identifier.getObjectName();
        try {
            RemoveUnexistingManifestsAction unexistingManifestsAction =
                    new RemoveUnexistingManifestsAction(databaseName, tableName, catalog.options());
            unexistingManifestsAction.run();
            return new String[] {"Success"};
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
