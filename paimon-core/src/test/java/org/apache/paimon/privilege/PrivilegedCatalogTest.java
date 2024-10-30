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

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.FileSystemCatalogTest;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.table.FileStoreTable;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

import java.util.OptionalLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Tests for {@link PrivilegedCatalog}. */
public class PrivilegedCatalogTest extends FileSystemCatalogTest {
    private static final String PASSWORD_ROOT = "123456";
    private static final String USERNAME_TEST_USER = "test_user";
    private static final String PASSWORD_TEST_USER = "test_password";

    @BeforeEach
    public void setUp() throws Exception {
        super.setUp();
        getPrivilegeManager("anonymous", "anonymous").initializePrivilege(PASSWORD_ROOT);
        catalog = new PrivilegedCatalog(catalog, getPrivilegeManager("root", PASSWORD_ROOT));
    }

    @Override
    @Test
    public void testGetTable() throws Exception {
        super.testGetTable();
        Identifier identifier = Identifier.create("test_db", "test_table");

        PrivilegedCatalog rootCatalog = ((PrivilegedCatalog) catalog);
        rootCatalog.createPrivilegedUser(USERNAME_TEST_USER, PASSWORD_TEST_USER);
        Catalog userCatalog =
                new PrivilegedCatalog(
                        rootCatalog.wrapped(),
                        getPrivilegeManager(USERNAME_TEST_USER, PASSWORD_TEST_USER));
        FileStoreTable dataTable = (FileStoreTable) userCatalog.getTable(identifier);

        assertNoPrivilege(dataTable::snapshotManager);
        assertNoPrivilege(dataTable::latestSnapshotId);
        assertNoPrivilege(() -> dataTable.snapshot(0));

        rootCatalog.grantPrivilegeOnTable(USERNAME_TEST_USER, identifier, PrivilegeType.SELECT);
        userCatalog =
                new PrivilegedCatalog(
                        rootCatalog.wrapped(),
                        getPrivilegeManager(USERNAME_TEST_USER, PASSWORD_TEST_USER));
        FileStoreTable dataTable2 = (FileStoreTable) userCatalog.getTable(identifier);

        assertThat(dataTable2.snapshotManager().latestSnapshotId()).isNull();
        assertThat(dataTable2.latestSnapshotId()).isEqualTo(OptionalLong.empty());
        assertThatThrownBy(() -> dataTable2.snapshot(0)).isNotNull();
    }

    private FileBasedPrivilegeManager getPrivilegeManager(String user, String password) {
        return new FileBasedPrivilegeManager(warehouse, fileIO, user, password);
    }

    private void assertNoPrivilege(Executable executable) {
        Exception e = assertThrows(Exception.class, executable);
        if (e.getCause() != null) {
            assertThat(e).hasRootCauseInstanceOf(NoPrivilegeException.class);
        } else {
            assertThat(e).isInstanceOf(NoPrivilegeException.class);
        }
    }
}
