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

import org.apache.paimon.migrate.ExternalFileImporter;

import org.apache.flink.table.annotation.ArgumentHint;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.ProcedureHint;
import org.apache.flink.table.procedure.ProcedureContext;

import javax.annotation.Nullable;

/** Imports files from a directory into a partition using external paths. */
public class ImportFilesProcedure extends ProcedureBase {

    @Override
    public String identifier() {
        return "import_files";
    }

    @ProcedureHint(
            argument = {
                @ArgumentHint(name = "table", type = @DataTypeHint("STRING")),
                @ArgumentHint(name = "location", type = @DataTypeHint("STRING")),
                @ArgumentHint(name = "partition", type = @DataTypeHint("STRING"), isOptional = true)
            },
            output = @DataTypeHint("BIGINT"))
    public Long[] call(
            ProcedureContext context, String tableId, String location, @Nullable String partition)
            throws Exception {
        return new Long[] {ExternalFileImporter.importFiles(table(tableId), location, partition)};
    }
}
