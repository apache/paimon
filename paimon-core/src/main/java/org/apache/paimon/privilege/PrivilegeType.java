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

package org.apache.paimon.privilege;

import org.apache.paimon.annotation.Public;

/**
 * Types of privilege.
 *
 * @since 0.7.0
 */
@Public
public enum PrivilegeType {
    SELECT(PrivilegeTarget.TABLE),
    INSERT(PrivilegeTarget.TABLE),
    ALTER_TABLE(PrivilegeTarget.TABLE),
    DROP_TABLE(PrivilegeTarget.TABLE),

    CREATE_TABLE(PrivilegeTarget.DATABASE),
    DROP_DATABASE(PrivilegeTarget.DATABASE),
    ALTER_DATABASE(PrivilegeTarget.DATABASE),

    CREATE_DATABASE(PrivilegeTarget.CATALOG),
    // you can create and drop users, grant and revoke any privileges to or from others
    ADMIN(PrivilegeTarget.CATALOG);

    private final PrivilegeTarget target;

    PrivilegeType(PrivilegeTarget target) {
        this.target = target;
    }

    public boolean canGrantOnCatalog() {
        return PrivilegeTarget.CATALOG.equals(target) || canGrantOnDatabase();
    }

    public boolean canGrantOnDatabase() {
        return PrivilegeTarget.DATABASE.equals(target) || canGrantOnTable();
    }

    public boolean canGrantOnTable() {
        return PrivilegeTarget.TABLE.equals(target);
    }

    private enum PrivilegeTarget {
        CATALOG,
        DATABASE,
        TABLE
    }
}
