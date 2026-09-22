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

import org.apache.paimon.append.dataevolution.DataEvolutionEnabler;
import org.apache.paimon.catalog.Identifier;

import org.apache.flink.table.annotation.ArgumentHint;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.ProcedureHint;
import org.apache.flink.table.procedure.ProcedureContext;

/**
 * Enables data evolution on an existing append table without rewriting its data files. See {@link
 * DataEvolutionEnabler}.
 *
 * <pre><code>
 *  CALL sys.enable_data_evolution('default.T')
 *  CALL sys.enable_data_evolution(`table` => 'default.T', dry_run => true)
 * </code></pre>
 */
public class EnableDataEvolutionProcedure extends ProcedureBase {

    public static final String IDENTIFIER = "enable_data_evolution";

    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    @ProcedureHint(
            argument = {
                @ArgumentHint(name = "table", type = @DataTypeHint("STRING")),
                @ArgumentHint(name = "dry_run", type = @DataTypeHint("BOOLEAN"), isOptional = true)
            })
    public String[] call(ProcedureContext procedureContext, String tableId, Boolean dryRun)
            throws Exception {
        Identifier identifier = Identifier.fromString(tableId);
        DataEvolutionEnabler.Result result =
                new DataEvolutionEnabler(catalog, identifier).run(dryRun != null && dryRun);
        return new String[] {result.describe(identifier)};
    }
}
