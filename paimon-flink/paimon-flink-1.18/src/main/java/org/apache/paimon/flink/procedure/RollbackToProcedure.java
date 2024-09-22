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

import org.apache.flink.table.procedure.ProcedureContext;

/**
 * Rollback procedure. Usage:
 *
 * <pre><code>
 *  -- rollback to a snapshot
 *  CALL sys.rollback_to('tableId', snapshotId)
 *
 *  -- rollback to a tag
 *  CALL sys.rollback_to('tableId', 'tagName')
 * </code></pre>
 */
public class RollbackToProcedure extends ProcedureBase {

    public static final String IDENTIFIER = "rollback_to";

    public String[] call(ProcedureContext procedureContext, String tableId, long snapshotId)
            throws Catalog.TableNotExistException {
        Table table = catalog.getTable(Identifier.fromString(tableId));
        table.rollbackTo(snapshotId);

        return new String[] {"Success"};
    }

    public String[] call(ProcedureContext procedureContext, String tableId, String tagName)
            throws Catalog.TableNotExistException {
        Table table = catalog.getTable(Identifier.fromString(tableId));
        table.rollbackTo(tagName);

        return new String[] {"Success"};
    }

    @Override
    public String identifier() {
        return IDENTIFIER;
    }
}
