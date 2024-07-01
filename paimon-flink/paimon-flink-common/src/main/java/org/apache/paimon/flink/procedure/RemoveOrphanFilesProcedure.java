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
import org.apache.paimon.operation.OrphanFilesClean;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.utils.StringUtils;

import org.apache.flink.table.procedure.ProcedureContext;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * Remove orphan files procedure. Usage:
 *
 * <pre><code>
 *  -- use the default file delete interval
 *  CALL sys.remove_orphan_files('tableId')
 *
 *  -- use custom file delete interval
 *  CALL sys.remove_orphan_files('tableId', '2023-12-31 23:59:59')
 * </code></pre>
 */
public class RemoveOrphanFilesProcedure extends ProcedureBase {

    public static final String IDENTIFIER = "remove_orphan_files";

    public String[] call(ProcedureContext procedureContext, String tableId) throws Exception {
        return call(procedureContext, tableId, "");
    }

    public String[] call(ProcedureContext procedureContext, String tableId, String olderThan)
            throws Exception {
        return call(procedureContext, tableId, olderThan, false);
    }

    public String[] call(
            ProcedureContext procedureContext, String tableId, String olderThan, boolean dryRun)
            throws Exception {
        Identifier identifier = Identifier.fromString(tableId);
        Table table = catalog.getTable(identifier);

        checkArgument(
                table instanceof FileStoreTable,
                "Only FileStoreTable supports remove-orphan-files action. The table type is '%s'.",
                table.getClass().getName());

        OrphanFilesClean orphanFilesClean = new OrphanFilesClean((FileStoreTable) table);
        if (!StringUtils.isBlank(olderThan)) {
            orphanFilesClean.olderThan(olderThan);
        }

        orphanFilesClean.dryRun(dryRun);

        return orphanFilesClean.clean().stream()
                .map(filePath -> filePath.toUri().getPath())
                .toArray(String[]::new);
    }

    @Override
    public String identifier() {
        return IDENTIFIER;
    }
}
