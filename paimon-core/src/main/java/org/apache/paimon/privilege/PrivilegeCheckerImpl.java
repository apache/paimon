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

import org.apache.paimon.catalog.Identifier;

import java.util.Map;
import java.util.Objects;
import java.util.Set;

/** Default implementation of {@link PrivilegeChecker}. */
public class PrivilegeCheckerImpl implements PrivilegeChecker {

    private static final long serialVersionUID = 1L;

    private final String user;
    private final Map<String, Set<PrivilegeType>> privileges;

    public PrivilegeCheckerImpl(String user, Map<String, Set<PrivilegeType>> privileges) {
        this.user = user;
        this.privileges = privileges;
    }

    @Override
    public void assertCanSelect(Identifier identifier) {
        if (!check(identifier.getFullName(), PrivilegeType.SELECT)) {
            throw new NoPrivilegeException(
                    user, "table", identifier.getFullName(), PrivilegeType.SELECT);
        }
    }

    @Override
    public void assertCanInsert(Identifier identifier) {
        if (!check(identifier.getFullName(), PrivilegeType.INSERT)) {
            throw new NoPrivilegeException(
                    user, "table", identifier.getFullName(), PrivilegeType.INSERT);
        }
    }

    @Override
    public void assertCanAlterTable(Identifier identifier) {
        if (!check(identifier.getFullName(), PrivilegeType.ALTER_TABLE)) {
            throw new NoPrivilegeException(
                    user, "table", identifier.getFullName(), PrivilegeType.ALTER_TABLE);
        }
    }

    @Override
    public void assertCanDropTable(Identifier identifier) {
        if (!check(identifier.getFullName(), PrivilegeType.DROP_TABLE)) {
            throw new NoPrivilegeException(
                    user, "table", identifier.getFullName(), PrivilegeType.DROP_TABLE);
        }
    }

    @Override
    public void assertCanCreateTable(String databaseName) {
        if (!check(databaseName, PrivilegeType.CREATE_TABLE)) {
            throw new NoPrivilegeException(
                    user, "database", databaseName, PrivilegeType.CREATE_TABLE);
        }
    }

    @Override
    public void assertCanDropDatabase(String databaseName) {
        if (!check(databaseName, PrivilegeType.DROP_DATABASE)) {
            throw new NoPrivilegeException(
                    user, "database", databaseName, PrivilegeType.DROP_DATABASE);
        }
    }

    @Override
    public void assertCanAlertDatabase(String databaseName) {
        if (!check(databaseName, PrivilegeType.ALERT_DATABASE)) {
            throw new NoPrivilegeException(
                    user, "database", databaseName, PrivilegeType.ALERT_DATABASE);
        }
    }

    @Override
    public void assertCanCreateDatabase() {
        if (!check(
                FileBasedPrivilegeManager.IDENTIFIER_WHOLE_CATALOG,
                PrivilegeType.CREATE_DATABASE)) {
            throw new NoPrivilegeException(
                    user,
                    "catalog",
                    FileBasedPrivilegeManager.IDENTIFIER_WHOLE_CATALOG,
                    PrivilegeType.DROP_DATABASE);
        }
    }

    @Override
    public void assertCanCreateUser() {
        assertHasAdmin();
    }

    @Override
    public void assertCanDropUser() {
        assertHasAdmin();
    }

    @Override
    public void assertCanGrant(String identifier, PrivilegeType privilege) {
        assertHasAdmin();
    }

    @Override
    public void assertCanRevoke() {
        assertHasAdmin();
    }

    private void assertHasAdmin() {
        if (!check(FileBasedPrivilegeManager.IDENTIFIER_WHOLE_CATALOG, PrivilegeType.ADMIN)) {
            throw new NoPrivilegeException(
                    user,
                    "catalog",
                    FileBasedPrivilegeManager.IDENTIFIER_WHOLE_CATALOG,
                    PrivilegeType.ADMIN);
        }
    }

    private boolean check(String identifier, PrivilegeType privilege) {
        Set<PrivilegeType> set = privileges.get(identifier);
        if (set != null && set.contains(privilege)) {
            return true;
        } else if (identifier.isEmpty()) {
            return false;
        } else {
            return check(
                    identifier.substring(0, Math.max(identifier.lastIndexOf('.'), 0)), privilege);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PrivilegeCheckerImpl that = (PrivilegeCheckerImpl) o;
        return Objects.equals(user, that.user) && Objects.equals(privileges, that.privileges);
    }
}
