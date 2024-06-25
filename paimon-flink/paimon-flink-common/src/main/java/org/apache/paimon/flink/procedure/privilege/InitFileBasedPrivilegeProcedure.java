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

package org.apache.paimon.flink.procedure.privilege;

import org.apache.paimon.flink.procedure.ProcedureBase;
import org.apache.paimon.options.Options;
import org.apache.paimon.privilege.FileBasedPrivilegeManager;
import org.apache.paimon.privilege.PrivilegeManager;
import org.apache.paimon.privilege.PrivilegedCatalog;

import org.apache.flink.table.procedure.ProcedureContext;

/**
 * Procedure to initialize file-based privilege system in warehouse. This procedure will
 * automatically create a root user with the provided password. Usage:
 *
 * <pre><code>
 *  CALL sys.init_file_based_privilege('rootPassword')
 * </code></pre>
 */
public class InitFileBasedPrivilegeProcedure extends ProcedureBase {

    public static final String IDENTIFIER = "init_file_based_privilege";

    public String[] call(ProcedureContext procedureContext, String rootPassword) {
        if (catalog instanceof PrivilegedCatalog) {
            throw new IllegalArgumentException("Catalog is already a PrivilegedCatalog");
        }

        Options options = new Options(catalog.options());
        PrivilegeManager privilegeManager =
                new FileBasedPrivilegeManager(
                        catalog.warehouse(),
                        catalog.fileIO(),
                        options.get(PrivilegedCatalog.USER),
                        options.get(PrivilegedCatalog.PASSWORD));
        privilegeManager.initializePrivilege(rootPassword);
        return new String[] {
            "Privilege system is successfully enabled. Please drop and re-create the catalog."
        };
    }

    @Override
    public String identifier() {
        return IDENTIFIER;
    }
}
