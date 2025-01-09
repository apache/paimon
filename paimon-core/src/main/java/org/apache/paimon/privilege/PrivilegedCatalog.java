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
import org.apache.paimon.catalog.CatalogLoader;
import org.apache.paimon.catalog.DelegateCatalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.catalog.PropertyChange;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.options.ConfigOption;
import org.apache.paimon.options.ConfigOptions;
import org.apache.paimon.options.Options;
import org.apache.paimon.partition.Partition;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.utils.Preconditions;

import java.util.List;
import java.util.Map;

/** {@link Catalog} which supports privilege system. */
public class PrivilegedCatalog extends DelegateCatalog {

    public static final ConfigOption<String> USER =
            ConfigOptions.key("user").stringType().defaultValue(PrivilegeManager.USER_ANONYMOUS);
    public static final ConfigOption<String> PASSWORD =
            ConfigOptions.key("password")
                    .stringType()
                    .defaultValue(PrivilegeManager.PASSWORD_ANONYMOUS);

    private final String warehouse;
    private final FileIO fileIO;
    private final String user;
    private final String password;

    private final PrivilegeManager privilegeManager;

    public PrivilegedCatalog(
            Catalog wrapped, String warehouse, FileIO fileIO, String user, String password) {
        super(wrapped);
        this.warehouse = warehouse;
        this.fileIO = fileIO;
        this.user = user;
        this.password = password;
        this.privilegeManager = new FileBasedPrivilegeManager(warehouse, fileIO, user, password);
    }

    public static Catalog tryToCreate(Catalog catalog, Options options) {
        if (new FileBasedPrivilegeManager(
                        catalog.warehouse(),
                        catalog.fileIO(),
                        options.get(PrivilegedCatalog.USER),
                        options.get(PrivilegedCatalog.PASSWORD))
                .privilegeEnabled()) {
            catalog =
                    new PrivilegedCatalog(
                            catalog,
                            catalog.warehouse(),
                            catalog.fileIO(),
                            options.get(PrivilegedCatalog.USER),
                            options.get(PrivilegedCatalog.PASSWORD));
        }
        return catalog;
    }

    public PrivilegeManager privilegeManager() {
        return privilegeManager;
    }

    @Override
    public CatalogLoader catalogLoader() {
        return new PrivilegedCatalogLoader(
                wrapped.catalogLoader(), warehouse, fileIO, user, password);
    }

    @Override
    public void createDatabase(String name, boolean ignoreIfExists, Map<String, String> properties)
            throws DatabaseAlreadyExistException {
        privilegeManager.getPrivilegeChecker().assertCanCreateDatabase();
        wrapped.createDatabase(name, ignoreIfExists, properties);
    }

    @Override
    public void dropDatabase(String name, boolean ignoreIfNotExists, boolean cascade)
            throws DatabaseNotExistException, DatabaseNotEmptyException {
        privilegeManager.getPrivilegeChecker().assertCanDropDatabase(name);
        wrapped.dropDatabase(name, ignoreIfNotExists, cascade);
        privilegeManager.objectDropped(name);
    }

    @Override
    public void alterDatabase(String name, List<PropertyChange> changes, boolean ignoreIfNotExists)
            throws DatabaseNotExistException {
        privilegeManager.getPrivilegeChecker().assertCanAlterDatabase(name);
        super.alterDatabase(name, changes, ignoreIfNotExists);
    }

    @Override
    public void dropTable(Identifier identifier, boolean ignoreIfNotExists)
            throws TableNotExistException {
        privilegeManager.getPrivilegeChecker().assertCanDropTable(identifier);
        wrapped.dropTable(identifier, ignoreIfNotExists);
        privilegeManager.objectDropped(identifier.getFullName());
    }

    @Override
    public void createTable(Identifier identifier, Schema schema, boolean ignoreIfExists)
            throws TableAlreadyExistException, DatabaseNotExistException {
        privilegeManager.getPrivilegeChecker().assertCanCreateTable(identifier.getDatabaseName());
        wrapped.createTable(identifier, schema, ignoreIfExists);
    }

    @Override
    public void renameTable(Identifier fromTable, Identifier toTable, boolean ignoreIfNotExists)
            throws TableNotExistException, TableAlreadyExistException {
        privilegeManager.getPrivilegeChecker().assertCanAlterTable(fromTable);
        wrapped.renameTable(fromTable, toTable, ignoreIfNotExists);

        try {
            getTable(toTable);
        } catch (TableNotExistException e) {
            throw new IllegalStateException(
                    "Table "
                            + toTable
                            + " does not exist. There might be concurrent renaming. "
                            + "Aborting updates in privilege system.");
        }
        privilegeManager.objectRenamed(fromTable.getFullName(), toTable.getFullName());
    }

    @Override
    public void alterTable(
            Identifier identifier, List<SchemaChange> changes, boolean ignoreIfNotExists)
            throws TableNotExistException, ColumnAlreadyExistException, ColumnNotExistException {
        privilegeManager.getPrivilegeChecker().assertCanAlterTable(identifier);
        wrapped.alterTable(identifier, changes, ignoreIfNotExists);
    }

    @Override
    public Table getTable(Identifier identifier) throws TableNotExistException {
        Table table = wrapped.getTable(identifier);
        if (table instanceof FileStoreTable) {
            return PrivilegedFileStoreTable.wrap(
                    (FileStoreTable) table, privilegeManager.getPrivilegeChecker(), identifier);
        } else {
            return table;
        }
    }

    @Override
    public void createPartitions(Identifier identifier, List<Map<String, String>> partitions)
            throws TableNotExistException {
        privilegeManager.getPrivilegeChecker().assertCanInsert(identifier);
        wrapped.createPartitions(identifier, partitions);
    }

    @Override
    public void dropPartitions(Identifier identifier, List<Map<String, String>> partitions)
            throws TableNotExistException {
        privilegeManager.getPrivilegeChecker().assertCanInsert(identifier);
        wrapped.dropPartitions(identifier, partitions);
    }

    @Override
    public void alterPartitions(Identifier identifier, List<Partition> partitions)
            throws TableNotExistException {
        privilegeManager.getPrivilegeChecker().assertCanInsert(identifier);
        wrapped.alterPartitions(identifier, partitions);
    }

    @Override
    public void markDonePartitions(Identifier identifier, List<Map<String, String>> partitions)
            throws TableNotExistException {
        privilegeManager.getPrivilegeChecker().assertCanInsert(identifier);
        wrapped.markDonePartitions(identifier, partitions);
    }

    public void createPrivilegedUser(String user, String password) {
        privilegeManager.createUser(user, password);
    }

    public void dropPrivilegedUser(String user) {
        privilegeManager.dropUser(user);
    }

    public void grantPrivilegeOnCatalog(String user, PrivilegeType privilege) {
        Preconditions.checkArgument(
                privilege.canGrantOnCatalog(),
                "Privilege " + privilege + " can't be granted on a catalog");
        privilegeManager.grant(user, PrivilegeManager.IDENTIFIER_WHOLE_CATALOG, privilege);
    }

    public void grantPrivilegeOnDatabase(
            String user, String databaseName, PrivilegeType privilege) {
        Preconditions.checkArgument(
                privilege.canGrantOnDatabase(),
                "Privilege " + privilege + " can't be granted on a database");
        try {
            getDatabase(databaseName);
        } catch (DatabaseNotExistException e) {
            throw new IllegalArgumentException("Database " + databaseName + " does not exist");
        }
        privilegeManager.grant(user, databaseName, privilege);
    }

    public void grantPrivilegeOnTable(String user, Identifier identifier, PrivilegeType privilege) {
        Preconditions.checkArgument(
                privilege.canGrantOnTable(),
                "Privilege " + privilege + " can't be granted on a table");

        try {
            getTable(identifier);
        } catch (TableNotExistException e) {
            throw new IllegalArgumentException("Table " + identifier + " does not exist");
        }
        privilegeManager.grant(user, identifier.getFullName(), privilege);
    }

    /** Returns the number of privilege revoked. */
    public int revokePrivilegeOnCatalog(String user, PrivilegeType privilege) {
        return privilegeManager.revoke(user, PrivilegeManager.IDENTIFIER_WHOLE_CATALOG, privilege);
    }

    /** Returns the number of privilege revoked. */
    public int revokePrivilegeOnDatabase(
            String user, String databaseName, PrivilegeType privilege) {
        return privilegeManager.revoke(user, databaseName, privilege);
    }

    /** Returns the number of privilege revoked. */
    public int revokePrivilegeOnTable(String user, Identifier identifier, PrivilegeType privilege) {
        return privilegeManager.revoke(user, identifier.getFullName(), privilege);
    }
}
