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
import org.apache.paimon.utils.Preconditions;

import org.apache.flink.table.annotation.ArgumentHint;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.ProcedureHint;
import org.apache.flink.table.procedure.ProcedureContext;

/**
 * Procedure to revoke privilege from a user. Privilege can be revoked from the whole catalog, a
 * database or a table. Only users with {@link org.apache.paimon.privilege.PrivilegeType#ADMIN}
 * privilege can perform this operation. Usage:
 *
 * <pre><code>
 *  CALL sys.revoke_privilege_from_user('username', 'privilege')
 *  CALL sys.revoke_privilege_from_user('username', 'privilege', 'database')
 *  CALL sys.revoke_privilege_from_user('username', 'privilege', 'database', 'table')
 * </code></pre>
 */
public class RevokePrivilegeFromUserProcedure extends PrivilegeProcedureBase {

    public static final String IDENTIFIER = "revoke_privilege_from_user";

    @ProcedureHint(
            argument = {
                @ArgumentHint(name = "username", type = @DataTypeHint("STRING")),
                @ArgumentHint(name = "privilege", type = @DataTypeHint("STRING")),
                @ArgumentHint(name = "database", type = @DataTypeHint("STRING"), isOptional = true),
                @ArgumentHint(name = "table", type = @DataTypeHint("STRING"), isOptional = true)
            })
    public String[] call(
            ProcedureContext procedureContext,
            String user,
            String privilege,
            String database,
            String table) {
        if (database == null) {
            Preconditions.checkState(
                    table == null,
                    "database must be set if privilege is granted at table's granularity.");
            int count =
                    getPrivilegedCatalog()
                            .revokePrivilegeOnCatalog(user, PrivilegeType.valueOf(privilege));
            return new String[] {
                String.format(
                        "User %s is revoked with privilege %s on the catalog.", user, privilege),
                "Number of privileges revoked: " + count
            };
        } else if (table == null) {
            int count =
                    getPrivilegedCatalog()
                            .revokePrivilegeOnDatabase(
                                    user, database, PrivilegeType.valueOf(privilege));
            return new String[] {
                String.format(
                        "User %s is revoked with privilege %s on database %s.",
                        user, privilege, database),
                "Number of privileges revoked: " + count
            };
        } else {
            Identifier identifier = Identifier.create(database, table);
            int count =
                    getPrivilegedCatalog()
                            .revokePrivilegeOnTable(
                                    user, identifier, PrivilegeType.valueOf(privilege));
            return new String[] {
                String.format(
                        "User %s is revoked with privilege %s on table %s.",
                        user, privilege, identifier),
                "Number of privileges revoked: " + count
            };
        }
    }

    @Override
    public String identifier() {
        return IDENTIFIER;
    }
}
