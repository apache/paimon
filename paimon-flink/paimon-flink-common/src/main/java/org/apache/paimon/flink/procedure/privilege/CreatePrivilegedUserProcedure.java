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

import org.apache.flink.table.procedure.ProcedureContext;

/**
 * Procedure to create a user for the privilege system. Only users with {@link
 * org.apache.paimon.privilege.PrivilegeType#ADMIN} privilege can perform this operation. Usage:
 *
 * <pre><code>
 *  CALL sys.create_privileged_user('username', 'password')
 * </code></pre>
 */
public class CreatePrivilegedUserProcedure extends PrivilegeProcedureBase {

    public static final String IDENTIFIER = "create_privileged_user";

    public String[] call(ProcedureContext procedureContext, String name, String password) {
        getPrivilegedCatalog().createPrivilegedUser(name, password);
        return new String[] {String.format("User %s is created without any privileges.", name)};
    }

    @Override
    public String identifier() {
        return IDENTIFIER;
    }
}
