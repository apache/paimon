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
import org.apache.paimon.table.object.ObjectTable;

import org.apache.flink.table.annotation.ArgumentHint;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.ProcedureHint;
import org.apache.flink.table.procedure.ProcedureContext;
import org.apache.flink.types.Row;

/**
 * Refresh Object Table procedure. Usage:
 *
 * <pre><code>
 *  CALL sys.refresh_object_table('tableId')
 * </code></pre>
 */
public class RefreshObjectTableProcedure extends ProcedureBase {

    private static final String IDENTIFIER = "refresh_object_table";

    @ProcedureHint(argument = {@ArgumentHint(name = "table", type = @DataTypeHint("STRING"))})
    public @DataTypeHint("ROW<file_number BIGINT>") Row[] call(
            ProcedureContext procedureContext, String tableId)
            throws Catalog.TableNotExistException {
        ObjectTable table = (ObjectTable) table(tableId);
        long fileNumber = table.refresh();
        return new Row[] {Row.of(fileNumber)};
    }

    @Override
    public String identifier() {
        return IDENTIFIER;
    }
}
