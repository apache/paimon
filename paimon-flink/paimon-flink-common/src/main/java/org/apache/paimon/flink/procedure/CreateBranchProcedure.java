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

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.table.procedure.ProcedureContext;

/**
 * Create branch procedure for given tag. Usage:
 *
 * <pre><code>
 *  CALL sys.create_branch('tableId', 'branchName', 'tagName')
 * </code></pre>
 */
public class CreateBranchProcedure extends ProcedureBase {

    public static final String IDENTIFIER = "create_branch";

    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    public String[] call(
            ProcedureContext procedureContext, String tableId, String branchName, String tagName)
            throws Catalog.TableNotExistException {
        return innerCall(tableId, branchName, tagName);
    }

    public String[] call(ProcedureContext procedureContext, String tableId, String branchName)
            throws Catalog.TableNotExistException {
        return innerCall(tableId, branchName, null);
    }

    private String[] innerCall(String tableId, String branchName, String tagName)
            throws Catalog.TableNotExistException {
        Table table = catalog.getTable(Identifier.fromString(tableId));
        if (!StringUtils.isBlank(tagName)) {
            table.createBranch(branchName, tagName);
        } else {
            table.createBranch(branchName);
        }
        return new String[] {"Success"};
    }
}
