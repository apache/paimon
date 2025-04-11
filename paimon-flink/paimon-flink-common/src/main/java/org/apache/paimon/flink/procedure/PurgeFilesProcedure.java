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
import org.apache.paimon.table.FileStoreTable;

import org.apache.flink.table.annotation.ArgumentHint;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.ProcedureHint;
import org.apache.flink.table.procedure.ProcedureContext;
import org.apache.flink.types.Row;

import static org.apache.paimon.table.FileStoreTableUtils.purgeFiles;

/**
 * A procedure to purge files for a table. Usage:
 *
 * <pre><code>
 *  -- rollback to the snapshot which earlier or equal than watermark.
 *  CALL sys.purge_files(`table` => 'tableId')
 * </code></pre>
 */
public class PurgeFilesProcedure extends ProcedureBase {

    public static final String IDENTIFIER = "purge_files";

    @ProcedureHint(argument = {@ArgumentHint(name = "table", type = @DataTypeHint("STRING"))})
    public @DataTypeHint("ROW<result STRING>") Row[] call(
            ProcedureContext procedureContext, String tableId) throws Exception {
        FileStoreTable table = (FileStoreTable) catalog.getTable(Identifier.fromString(tableId));
        purgeFiles(table);
        return new Row[] {Row.of("Success")};
    }

    @Override
    public String identifier() {
        return IDENTIFIER;
    }
}
