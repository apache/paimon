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

import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.utils.TraceableFileIO;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.function.Predicate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Tests for {@link FileBasedPrivilegeManager}. */
public class FileBasedPrivilegeManagerTest {

    private static final String PASSWORD_ROOT = "123456";

    @TempDir public java.nio.file.Path tempPath;
    private Path warehouse;

    @BeforeEach
    public void beforeEach() {
        warehouse = new Path(TraceableFileIO.SCHEME + "://" + tempPath.toString());
    }

    @AfterEach
    public void afterEach() {
        Predicate<Path> pathPredicate = path -> path.toString().contains(tempPath.toString());
        assertThat(TraceableFileIO.openInputStreams(pathPredicate)).isEmpty();
        assertThat(TraceableFileIO.openOutputStreams(pathPredicate)).isEmpty();
    }

    @Test
    public void testInitializeMultipleTimes() throws Exception {
        initPrivilege();
        assertThrows(IllegalStateException.class, this::initPrivilege);
    }

    @Test
    public void testUsers() throws Exception {
        initPrivilege();

        FileBasedPrivilegeManager rootManager = getPrivilegeManager("root", PASSWORD_ROOT);
        rootManager.createUser("test", "passwd");
        assertThrows(
                IllegalArgumentException.class, () -> rootManager.createUser("test", "changed"));

        FileBasedPrivilegeManager testManager = getPrivilegeManager("test", "passwd");
        assertThrows(
                IllegalArgumentException.class,
                () -> getPrivilegeManager("test", "wrong").getPrivilegeChecker());
        assertThrows(NoPrivilegeException.class, () -> testManager.createUser("test2", "passwd"));

        rootManager.dropUser("test");
        rootManager.dropUser("test");
        assertThrows(
                IllegalArgumentException.class,
                () -> getPrivilegeManager("test", "passwd").getPrivilegeChecker());
        assertThrows(IllegalArgumentException.class, () -> rootManager.dropUser("root"));
        assertThrows(IllegalArgumentException.class, () -> rootManager.dropUser("anonymous"));
    }

    @Test
    public void testGrantAndRevoke() throws Exception {
        initPrivilege();

        FileBasedPrivilegeManager rootManager = getPrivilegeManager("root", PASSWORD_ROOT);
        rootManager.createUser("test", "passwd");
        rootManager.grant("test", "my_db", PrivilegeType.SELECT);
        rootManager.grant("test", "another_db.my_tbl", PrivilegeType.SELECT);
        rootManager.grant("test", "another_db.my_tbl", PrivilegeType.INSERT);
        rootManager.grant("test", "another_db.another_tbl", PrivilegeType.INSERT);
        assertThrows(
                IllegalArgumentException.class,
                () -> rootManager.grant("test2", "my_db", PrivilegeType.SELECT));
        FileBasedPrivilegeManager testManager = getPrivilegeManager("test", "passwd");

        {
            PrivilegeChecker checker = testManager.getPrivilegeChecker();

            checker.assertCanSelect(Identifier.create("my_db", "my_tbl"));
            checker.assertCanSelect(Identifier.create("my_db", "another_tbl"));
            checker.assertCanSelect(Identifier.create("another_db", "my_tbl"));
            assertThrows(
                    NoPrivilegeException.class,
                    () -> checker.assertCanSelect(Identifier.create("another_db", "another_tbl")));

            assertThrows(
                    NoPrivilegeException.class,
                    () -> checker.assertCanInsert(Identifier.create("my_db", "my_tbl")));
            assertThrows(
                    NoPrivilegeException.class,
                    () -> checker.assertCanInsert(Identifier.create("my_db", "another_tbl")));
            checker.assertCanInsert(Identifier.create("another_db", "my_tbl"));
            checker.assertCanInsert(Identifier.create("another_db", "another_tbl"));

            assertThrows(
                    NoPrivilegeException.class,
                    () ->
                            testManager.grant(
                                    "test", "another_db.another_tbl", PrivilegeType.SELECT));
        }

        rootManager.revoke("test", "another_db", PrivilegeType.INSERT);
        assertThrows(
                IllegalArgumentException.class,
                () -> rootManager.revoke("test2", "another_db", PrivilegeType.INSERT));

        {
            PrivilegeChecker checker = testManager.getPrivilegeChecker();

            checker.assertCanSelect(Identifier.create("my_db", "my_tbl"));
            checker.assertCanSelect(Identifier.create("my_db", "another_tbl"));
            checker.assertCanSelect(Identifier.create("another_db", "my_tbl"));
            assertThrows(
                    NoPrivilegeException.class,
                    () -> checker.assertCanSelect(Identifier.create("another_db", "another_tbl")));

            assertThrows(
                    NoPrivilegeException.class,
                    () -> checker.assertCanInsert(Identifier.create("my_db", "my_tbl")));
            assertThrows(
                    NoPrivilegeException.class,
                    () -> checker.assertCanInsert(Identifier.create("my_db", "another_tbl")));
            assertThrows(
                    NoPrivilegeException.class,
                    () -> checker.assertCanInsert(Identifier.create("another_db", "my_tbl")));
            assertThrows(
                    NoPrivilegeException.class,
                    () -> checker.assertCanInsert(Identifier.create("another_db", "another_tbl")));
        }
    }

    private void initPrivilege() throws Exception {
        getPrivilegeManager("anonymous", "anonymous").initializePrivilege(PASSWORD_ROOT);
    }

    private FileBasedPrivilegeManager getPrivilegeManager(String user, String password)
            throws Exception {
        return new FileBasedPrivilegeManager(
                warehouse.toString(),
                FileIO.get(warehouse, CatalogContext.create(warehouse)),
                user,
                password);
    }
}
