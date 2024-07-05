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
import org.apache.paimon.catalog.CatalogLockContext;
import org.apache.paimon.catalog.CatalogLockFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.metastore.MetastoreClient;
import org.apache.paimon.options.ConfigOption;
import org.apache.paimon.options.ConfigOptions;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.utils.Preconditions;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/** {@link Catalog} which supports privilege system. */
public class PrivilegedCatalog implements Catalog {

    public static final ConfigOption<String> USER =
            ConfigOptions.key("user").stringType().defaultValue(PrivilegeManager.USER_ANONYMOUS);
    public static final ConfigOption<String> PASSWORD =
            ConfigOptions.key("password")
                    .stringType()
                    .defaultValue(PrivilegeManager.PASSWORD_ANONYMOUS);

    private final Catalog wrapped;
    private final PrivilegeManager privilegeManager;

    public PrivilegedCatalog(Catalog wrapped, PrivilegeManager privilegeManager) {
        this.wrapped = wrapped;
        this.privilegeManager = privilegeManager;
    }

    public Catalog wrapped() {
        return wrapped;
    }

    public PrivilegeManager privilegeManager() {
        return privilegeManager;
    }

    @Override
    public boolean caseSensitive() {
        return wrapped.caseSensitive();
    }

    @Override
    public String warehouse() {
        return wrapped.warehouse();
    }

    @Override
    public Map<String, String> options() {
        return wrapped.options();
    }

    @Override
    public FileIO fileIO() {
        return wrapped.fileIO();
    }

    @Override
    public Optional<CatalogLockFactory> lockFactory() {
        return wrapped.lockFactory();
    }

    @Override
    public Optional<CatalogLockContext> lockContext() {
        return wrapped.lockContext();
    }

    @Override
    public Optional<MetastoreClient.Factory> metastoreClientFactory(Identifier identifier) {
        return wrapped.metastoreClientFactory(identifier);
    }

    @Override
    public List<String> listDatabases() {
        return wrapped.listDatabases();
    }

    @Override
    public boolean databaseExists(String databaseName) {
        return wrapped.databaseExists(databaseName);
    }

    @Override
    public void createDatabase(String name, boolean ignoreIfExists)
            throws DatabaseAlreadyExistException {
        privilegeManager.getPrivilegeChecker().assertCanCreateDatabase();
        wrapped.createDatabase(name, ignoreIfExists);
    }

    @Override
    public void createDatabase(String name, boolean ignoreIfExists, Map<String, String> properties)
            throws DatabaseAlreadyExistException {
        privilegeManager.getPrivilegeChecker().assertCanCreateDatabase();
        wrapped.createDatabase(name, ignoreIfExists, properties);
    }

    @Override
    public Map<String, String> loadDatabaseProperties(String name)
            throws DatabaseNotExistException {
        return wrapped.loadDatabaseProperties(name);
    }

    @Override
    public void dropDatabase(String name, boolean ignoreIfNotExists, boolean cascade)
            throws DatabaseNotExistException, DatabaseNotEmptyException {
        privilegeManager.getPrivilegeChecker().assertCanDropDatabase(name);
        wrapped.dropDatabase(name, ignoreIfNotExists, cascade);
        privilegeManager.objectDropped(name);
    }

    @Override
    public List<String> listTables(String databaseName) throws DatabaseNotExistException {
        return wrapped.listTables(databaseName);
    }

    @Override
    public boolean tableExists(Identifier identifier) {
        return wrapped.tableExists(identifier);
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
        Preconditions.checkState(
                wrapped.tableExists(toTable),
                "Table "
                        + toTable
                        + " does not exist. There might be concurrent renaming. "
                        + "Aborting updates in privilege system.");
        privilegeManager.objectRenamed(fromTable.getFullName(), toTable.getFullName());
    }

    @Override
    public void alterTable(Identifier identifier, SchemaChange change, boolean ignoreIfNotExists)
            throws TableNotExistException, ColumnAlreadyExistException, ColumnNotExistException {
        privilegeManager.getPrivilegeChecker().assertCanAlterTable(identifier);
        wrapped.alterTable(identifier, change, ignoreIfNotExists);
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
            return new PrivilegedFileStoreTable(
                    (FileStoreTable) table, privilegeManager.getPrivilegeChecker(), identifier);
        } else {
            return table;
        }
    }

    @Override
    public void dropPartition(Identifier identifier, Map<String, String> partitions)
            throws TableNotExistException, PartitionNotExistException {
        privilegeManager.getPrivilegeChecker().assertCanInsert(identifier);
        wrapped.dropPartition(identifier, partitions);
    }

    @Override
    public void close() throws Exception {
        wrapped.close();
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
        Preconditions.checkArgument(
                databaseExists(databaseName), "Database " + databaseName + " does not exist");
        privilegeManager.grant(user, databaseName, privilege);
    }

    public void grantPrivilegeOnTable(String user, Identifier identifier, PrivilegeType privilege) {
        Preconditions.checkArgument(
                privilege.canGrantOnTable(),
                "Privilege " + privilege + " can't be granted on a table");
        Preconditions.checkArgument(
                tableExists(identifier), "Table " + identifier + " does not exist");
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
