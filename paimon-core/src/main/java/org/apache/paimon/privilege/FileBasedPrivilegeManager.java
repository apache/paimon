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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.FileSystemCatalog;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.reader.RecordReaderIterator;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.TableScan;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.CloseableIterator;
import org.apache.paimon.utils.Preconditions;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A {@link PrivilegeManager} based on user and privilege system tables. The directories of these
 * system tables are created at the root of warehouse.
 *
 * <p>User table is the table which stores all user information. The schema of user table is:
 *
 * <ul>
 *   <li>user (string): user name (primary key)
 *   <li>sha256 (bytes): sha256 of password
 * </ul>
 *
 * <p>Privilege table is the table storing what privileges each user have. Its schema is:
 *
 * <ul>
 *   <li>name (string): user or role name (primary key)
 *   <li>entity_type (string): user or role (primary key)
 *   <li>identifier (string): identifier of object (primary key)
 *   <li>privilege (string): name of privilege (primary key), see {@link PrivilegeType}
 * </ul>
 */
public class FileBasedPrivilegeManager implements PrivilegeManager {

    private static final String USER_TABLE_DIR = "user.sys";
    private static final RowType USER_TABLE_TYPE =
            RowType.of(
                    new DataType[] {DataTypes.STRING(), DataTypes.BYTES()},
                    new String[] {"user", "sha256"});

    private static final String PRIVILEGE_TABLE_DIR = "privilege.sys";
    private static final RowType PRIVILEGE_TABLE_TYPE =
            RowType.of(
                    new DataType[] {
                        DataTypes.STRING(),
                        DataTypes.STRING(),
                        DataTypes.STRING(),
                        DataTypes.STRING()
                    },
                    new String[] {"name", "entity_type", "identifier", "privilege"});

    private final String warehouse;
    private final FileIO fileIO;
    private final String user;
    private final byte[] sha256;

    private Table userTable;
    private Table privilegeTable;

    public FileBasedPrivilegeManager(
            String warehouse, FileIO fileIO, String user, String password) {
        this.warehouse = warehouse;
        this.fileIO = fileIO;
        this.user = user;
        this.sha256 = getSha256(password);
    }

    public static void assertSupported(Catalog catalog) {
        if (catalog instanceof PrivilegedCatalog) {
            throw new IllegalArgumentException("Catalog is already a PrivilegedCatalog");
        } else if (catalog instanceof FileSystemCatalog) {
            // supported
        } else {
            throw new IllegalArgumentException(
                    "File based privilege system does not support " + catalog.getClass().getName());
        }
    }

    @Override
    public boolean privilegeEnabled() {
        return getUserTable(false) != null && getPrivilegeTable(false) != null;
    }

    @Override
    public void initializePrivilege(String rootPassword) {
        if (privilegeEnabled()) {
            throw new IllegalStateException(
                    "Privilege system is already enabled in warehouse " + warehouse);
        }

        createUserTable();
        createUserImpl(USER_ROOT, rootPassword);
        createUserImpl(USER_ANONYMOUS, PASSWORD_ANONYMOUS);

        createPrivilegeTable();
    }

    @Override
    public void createUser(String user, String password) {
        getPrivilegeChecker().assertCanCreateUser();
        if (userExists(user)) {
            throw new IllegalArgumentException("User " + user + " already exists");
        }
        createUserImpl(user, password);
    }

    @Override
    public void dropUser(String user) {
        getPrivilegeChecker().assertCanDropUser();
        Preconditions.checkArgument(!USER_ROOT.equals(user), USER_ROOT + " cannot be dropped");
        Preconditions.checkArgument(
                !USER_ANONYMOUS.equals(user), USER_ANONYMOUS + " cannot be dropped");
        dropUserImpl(user);
    }

    @Override
    public void grant(String user, String identifier, PrivilegeType privilege) {
        getPrivilegeChecker().assertCanGrant(identifier, privilege);
        Preconditions.checkArgument(
                !USER_ROOT.equals(user), "Cannot change privilege for user " + USER_ROOT);
        if (!userExists(user)) {
            throw new IllegalArgumentException("User " + user + " does not exist");
        }
        grantImpl(
                Collections.singletonList(
                        new PrivilegeEntry(user, EntityType.USER, identifier, privilege)));
    }

    @Override
    public int revoke(String user, String identifier, PrivilegeType privilege) {
        getPrivilegeChecker().assertCanRevoke();
        Preconditions.checkArgument(
                !USER_ROOT.equals(user), "Cannot change privilege for user " + USER_ROOT);
        if (!userExists(user)) {
            throw new IllegalArgumentException("User " + user + " does not exist");
        }
        int count = revokeImpl(user, identifier, privilege);
        Preconditions.checkArgument(
                count > 0,
                String.format(
                        "User %s does not have privilege %s on %s. "
                                + "It's possible that the user has such privilege on a higher level. "
                                + "Please check the privilege table.",
                        user, privilege, identifier));
        return count;
    }

    @Override
    public void objectRenamed(String oldName, String newName) {
        Table privilegeTable = getPrivilegeTable(true);
        PredicateBuilder predicateBuilder = new PredicateBuilder(PRIVILEGE_TABLE_TYPE);
        Predicate predicate = predicateBuilder.equal(2, BinaryString.fromString(oldName));

        BatchWriteBuilder writeBuilder = privilegeTable.newBatchWriteBuilder();
        try (CloseableIterator<InternalRow> it = read(privilegeTable, predicate);
                BatchTableWrite write = writeBuilder.newWrite();
                BatchTableCommit commit = writeBuilder.newCommit()) {
            while (it.hasNext()) {
                InternalRow row = it.next();
                GenericRow replaced =
                        GenericRow.of(
                                row.getString(0),
                                row.getString(1),
                                BinaryString.fromString(newName),
                                row.getString(3));
                write.write(replaced);
            }
            commit.commit(write.prepareCommit());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void objectDropped(String identifier) {
        Table privilegeTable = getPrivilegeTable(true);
        PredicateBuilder predicateBuilder = new PredicateBuilder(PRIVILEGE_TABLE_TYPE);
        Predicate predicate = predicateBuilder.startsWith(2, BinaryString.fromString(identifier));
        deleteAll(privilegeTable, predicate);
    }

    @Override
    public PrivilegeChecker getPrivilegeChecker() {
        assertUserPassword();
        if (USER_ROOT.equals(user)) {
            return new AllGrantedPrivilegeChecker();
        }

        Table privilegeTable = getPrivilegeTable(true);
        PredicateBuilder predicateBuilder = new PredicateBuilder(PRIVILEGE_TABLE_TYPE);
        Predicate predicate =
                PredicateBuilder.and(
                        predicateBuilder.equal(0, BinaryString.fromString(user)),
                        predicateBuilder.equal(1, BinaryString.fromString(EntityType.USER.name())));

        Map<String, Set<PrivilegeType>> privileges = new HashMap<>();
        try (CloseableIterator<InternalRow> it = read(privilegeTable, predicate)) {
            while (it.hasNext()) {
                InternalRow row = it.next();
                privileges
                        .computeIfAbsent(row.getString(2).toString(), ignore -> new HashSet<>())
                        .add(PrivilegeType.valueOf(row.getString(3).toString()));
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return new PrivilegeCheckerImpl(user, privileges);
    }

    private void createUserImpl(String user, String password) {
        byte[] sha256 = getSha256(password);
        BatchWriteBuilder writeBuilder = getUserTable(true).newBatchWriteBuilder();
        try (BatchTableWrite write = writeBuilder.newWrite();
                BatchTableCommit commit = writeBuilder.newCommit()) {
            write.write(GenericRow.of(BinaryString.fromString(user), sha256));
            commit.commit(write.prepareCommit());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void dropUserImpl(String user) {
        BatchWriteBuilder writeBuilder = getUserTable(true).newBatchWriteBuilder();
        try (BatchTableWrite write = writeBuilder.newWrite();
                BatchTableCommit commit = writeBuilder.newCommit()) {
            write.write(
                    GenericRow.ofKind(RowKind.DELETE, BinaryString.fromString(user), new byte[0]));
            commit.commit(write.prepareCommit());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        PredicateBuilder predicateBuilder = new PredicateBuilder(PRIVILEGE_TABLE_TYPE);
        Predicate predicate =
                PredicateBuilder.and(
                        predicateBuilder.equal(0, BinaryString.fromString(user)),
                        predicateBuilder.equal(1, BinaryString.fromString(EntityType.USER.name())));
        deleteAll(getPrivilegeTable(true), predicate);
    }

    private void grantImpl(List<PrivilegeEntry> entries) {
        BatchWriteBuilder writeBuilder = getPrivilegeTable(true).newBatchWriteBuilder();
        try (BatchTableWrite write = writeBuilder.newWrite();
                BatchTableCommit commit = writeBuilder.newCommit()) {
            for (PrivilegeEntry entry : entries) {
                write.write(
                        GenericRow.of(
                                BinaryString.fromString(entry.name),
                                BinaryString.fromString(entry.entityType.name()),
                                BinaryString.fromString(entry.identifier),
                                BinaryString.fromString(entry.privilege.name())));
            }
            commit.commit(write.prepareCommit());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private int revokeImpl(String user, String identifier, PrivilegeType privilege) {
        PredicateBuilder predicateBuilder = new PredicateBuilder(PRIVILEGE_TABLE_TYPE);
        Predicate predicate =
                PredicateBuilder.and(
                        predicateBuilder.equal(0, BinaryString.fromString(user)),
                        predicateBuilder.equal(1, BinaryString.fromString(EntityType.USER.name())),
                        predicateBuilder.startsWith(2, BinaryString.fromString(identifier)),
                        predicateBuilder.equal(3, BinaryString.fromString(privilege.name())));
        return deleteAll(getPrivilegeTable(true), predicate);
    }

    private static class PrivilegeEntry {
        String name;
        EntityType entityType;
        String identifier;
        PrivilegeType privilege;

        private PrivilegeEntry(
                String name, EntityType entityType, String identifier, PrivilegeType privilege) {
            this.name = name;
            this.entityType = entityType;
            this.identifier = identifier;
            this.privilege = privilege;
        }
    }

    private boolean userExists(String user) {
        Table userTable = getUserTable(true);
        Predicate predicate =
                new PredicateBuilder(USER_TABLE_TYPE).equal(0, BinaryString.fromString(user));
        try (CloseableIterator<InternalRow> it = read(userTable, predicate)) {
            return it.hasNext();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void assertUserPassword() {
        Table userTable = getUserTable(true);
        PredicateBuilder predicateBuilder = new PredicateBuilder(USER_TABLE_TYPE);
        Predicate predicate =
                PredicateBuilder.and(
                        predicateBuilder.equal(0, BinaryString.fromString(user)),
                        predicateBuilder.equal(1, sha256));

        try (CloseableIterator<InternalRow> it = read(userTable, predicate)) {
            if (it.hasNext()) {
                return;
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        throw new IllegalArgumentException("User " + user + " not found, or password incorrect.");
    }

    private Table getUserTable(boolean assertExists) {
        userTable = getTable(userTable, USER_TABLE_DIR, assertExists);
        return userTable;
    }

    private Table getPrivilegeTable(boolean assertExists) {
        privilegeTable = getTable(privilegeTable, PRIVILEGE_TABLE_DIR, assertExists);
        return privilegeTable;
    }

    private Table getTable(Table lazy, String dir, boolean assertExists) {
        if (lazy != null) {
            return lazy;
        }

        Path tableRoot = new Path(warehouse, dir);
        boolean tableExists;
        try {
            tableExists = fileIO.exists(tableRoot);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        if (tableExists) {
            return FileStoreTableFactory.create(fileIO, tableRoot);
        } else if (assertExists) {
            throw new RuntimeException(
                    "Privilege system is not enabled in warehouse " + warehouse + ".");
        } else {
            return null;
        }
    }

    private void createUserTable() {
        Options options = new Options();
        options.set(CoreOptions.BUCKET, 1);
        Path tableRoot = new Path(warehouse, USER_TABLE_DIR);
        SchemaManager schemaManager = new SchemaManager(fileIO, tableRoot);
        try {
            schemaManager.createTable(
                    new Schema(
                            USER_TABLE_TYPE.getFields(),
                            Collections.emptyList(),
                            Collections.singletonList("user"),
                            options.toMap(),
                            ""));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void createPrivilegeTable() {
        Options options = new Options();
        options.set(CoreOptions.BUCKET, 1);
        Path tableRoot = new Path(warehouse, PRIVILEGE_TABLE_DIR);
        SchemaManager schemaManager = new SchemaManager(fileIO, tableRoot);
        try {
            schemaManager.createTable(
                    new Schema(
                            PRIVILEGE_TABLE_TYPE.getFields(),
                            Collections.emptyList(),
                            Arrays.asList("name", "entity_type", "privilege", "identifier"),
                            options.toMap(),
                            ""));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private CloseableIterator<InternalRow> read(Table table, Predicate predicate) {
        ReadBuilder readBuilder = table.newReadBuilder().withFilter(predicate);
        TableScan.Plan plan = readBuilder.newScan().plan();
        try {
            return new RecordReaderIterator<>(
                    readBuilder.newRead().executeFilter().createReader(plan));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private int deleteAll(Table table, Predicate predicate) {
        int count = 0;
        BatchWriteBuilder writeBuilder = table.newBatchWriteBuilder();
        try (CloseableIterator<InternalRow> it = read(table, predicate);
                BatchTableWrite write = writeBuilder.newWrite();
                BatchTableCommit commit = writeBuilder.newCommit()) {
            while (it.hasNext()) {
                InternalRow row = it.next();
                row.setRowKind(RowKind.DELETE);
                write.write(row);
                count++;
            }
            commit.commit(write.prepareCommit());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return count;
    }

    private byte[] getSha256(String s) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(s.getBytes(StandardCharsets.UTF_8));
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }
}
