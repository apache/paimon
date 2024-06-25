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

import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.privilege.PrivilegeType;

import org.apache.flink.table.procedure.ProcedureContext;

/**
 * Procedure to grant privilege to a user. Privilege can be granted on the whole catalog, a database
 * or a table. Only users with {@link org.apache.paimon.privilege.PrivilegeType#ADMIN} privilege can
 * perform this operation. Usage:
 *
 * <pre><code>
 *  CALL sys.grant_privilege_to_user('username', 'privilege')
 *  CALL sys.grant_privilege_to_user('username', 'privilege', 'database')
 *  CALL sys.grant_privilege_to_user('username', 'privilege', 'database', 'table')
 * </code></pre>
 */
public class GrantPrivilegeToUserProcedure extends PrivilegeProcedureBase {

    public static final String IDENTIFIER = "grant_privilege_to_user";

    public String[] call(ProcedureContext procedureContext, String user, String privilege) {
        getPrivilegedCatalog().grantPrivilegeOnCatalog(user, PrivilegeType.valueOf(privilege));
        return new String[] {
            String.format("User %s is granted with privilege %s on the catalog.", user, privilege)
        };
    }

    public String[] call(
            ProcedureContext procedureContext, String user, String privilege, String database) {
        getPrivilegedCatalog()
                .grantPrivilegeOnDatabase(user, database, PrivilegeType.valueOf(privilege));
        return new String[] {
            String.format(
                    "User %s is granted with privilege %s on database %s.",
                    user, privilege, database)
        };
    }

    public String[] call(
            ProcedureContext procedureContext,
            String user,
            String privilege,
            String database,
            String table) {
        Identifier identifier = Identifier.create(database, table);
        getPrivilegedCatalog()
                .grantPrivilegeOnTable(user, identifier, PrivilegeType.valueOf(privilege));
        return new String[] {
            String.format(
                    "User %s is granted with privilege %s on table %s.",
                    user, privilege, identifier)
        };
    }

    @Override
    public String identifier() {
        return IDENTIFIER;
    }
}
