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

package org.apache.paimon.catalog;

import org.apache.paimon.PagedList;
import org.apache.paimon.Snapshot;
import org.apache.paimon.function.Function;
import org.apache.paimon.function.FunctionChange;
import org.apache.paimon.partition.Partition;
import org.apache.paimon.partition.PartitionStatistics;
import org.apache.paimon.rest.responses.GetTagResponse;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.table.Instant;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.TableSnapshot;
import org.apache.paimon.utils.SnapshotNotExistException;
import org.apache.paimon.view.View;
import org.apache.paimon.view.ViewChange;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/** A {@link Catalog} to delegate all operations to another {@link Catalog}. */
public abstract class DelegateCatalog implements Catalog {

    protected final Catalog wrapped;

    public DelegateCatalog(Catalog wrapped) {
        this.wrapped = wrapped;
    }

    public Catalog wrapped() {
        return wrapped;
    }

    @Override
    public boolean caseSensitive() {
        return wrapped.caseSensitive();
    }

    @Override
    public Map<String, String> options() {
        return wrapped.options();
    }

    @Override
    public List<String> listDatabases() {
        return wrapped.listDatabases();
    }

    @Override
    public PagedList<String> listDatabasesPaged(
            Integer maxResults, String pageToken, String databaseNamePattern) {
        return wrapped.listDatabasesPaged(maxResults, pageToken, databaseNamePattern);
    }

    @Override
    public void createDatabase(String name, boolean ignoreIfExists, Map<String, String> properties)
            throws DatabaseAlreadyExistException {
        wrapped.createDatabase(name, ignoreIfExists, properties);
    }

    @Override
    public Database getDatabase(String name) throws DatabaseNotExistException {
        return wrapped.getDatabase(name);
    }

    @Override
    public void dropDatabase(String name, boolean ignoreIfNotExists, boolean cascade)
            throws DatabaseNotExistException, DatabaseNotEmptyException {
        wrapped.dropDatabase(name, ignoreIfNotExists, cascade);
    }

    @Override
    public void alterDatabase(String name, List<PropertyChange> changes, boolean ignoreIfNotExists)
            throws DatabaseNotExistException {
        wrapped.alterDatabase(name, changes, ignoreIfNotExists);
    }

    @Override
    public List<String> listTables(String databaseName) throws DatabaseNotExistException {
        return wrapped.listTables(databaseName);
    }

    @Override
    public PagedList<String> listTablesPaged(
            String databaseName,
            Integer maxResults,
            String pageToken,
            String tableNamePattern,
            String tableType)
            throws DatabaseNotExistException {
        return wrapped.listTablesPaged(
                databaseName, maxResults, pageToken, tableNamePattern, tableType);
    }

    @Override
    public PagedList<Table> listTableDetailsPaged(
            String databaseName,
            Integer maxResults,
            String pageToken,
            String tableNamePattern,
            String tableType)
            throws DatabaseNotExistException {
        return wrapped.listTableDetailsPaged(
                databaseName, maxResults, pageToken, tableNamePattern, tableType);
    }

    @Override
    public PagedList<Identifier> listTablesPagedGlobally(
            String databaseNamePattern,
            String tableNamePattern,
            Integer maxResults,
            String pageToken) {
        return wrapped.listTablesPagedGlobally(
                databaseNamePattern, tableNamePattern, maxResults, pageToken);
    }

    @Override
    public void dropTable(Identifier identifier, boolean ignoreIfNotExists)
            throws TableNotExistException {
        wrapped.dropTable(identifier, ignoreIfNotExists);
    }

    @Override
    public void createTable(Identifier identifier, Schema schema, boolean ignoreIfExists)
            throws TableAlreadyExistException, DatabaseNotExistException {
        wrapped.createTable(identifier, schema, ignoreIfExists);
    }

    @Override
    public void renameTable(Identifier fromTable, Identifier toTable, boolean ignoreIfNotExists)
            throws TableNotExistException, TableAlreadyExistException {
        wrapped.renameTable(fromTable, toTable, ignoreIfNotExists);
    }

    @Override
    public void alterTable(
            Identifier identifier, List<SchemaChange> changes, boolean ignoreIfNotExists)
            throws TableNotExistException, ColumnAlreadyExistException, ColumnNotExistException {
        wrapped.alterTable(identifier, changes, ignoreIfNotExists);
    }

    @Override
    public void registerTable(Identifier identifier, String path)
            throws TableAlreadyExistException {
        wrapped.registerTable(identifier, path);
    }

    @Override
    public boolean supportsListObjectsPaged() {
        return wrapped.supportsListObjectsPaged();
    }

    @Override
    public boolean supportsListByPattern() {
        return wrapped.supportsListByPattern();
    }

    @Override
    public boolean supportsListTableByType() {
        return wrapped.supportsListTableByType();
    }

    @Override
    public boolean supportsVersionManagement() {
        return wrapped.supportsVersionManagement();
    }

    @Override
    public Optional<TableSnapshot> loadSnapshot(Identifier identifier)
            throws TableNotExistException {
        return wrapped.loadSnapshot(identifier);
    }

    @Override
    public Optional<Snapshot> loadSnapshot(Identifier identifier, String version)
            throws TableNotExistException {
        return wrapped.loadSnapshot(identifier, version);
    }

    @Override
    public PagedList<Snapshot> listSnapshotsPaged(
            Identifier identifier, @Nullable Integer maxResults, @Nullable String pageToken)
            throws TableNotExistException {
        return wrapped.listSnapshotsPaged(identifier, maxResults, pageToken);
    }

    @Override
    public void rollbackTo(Identifier identifier, Instant instant, @Nullable Long fromSnapshot)
            throws Catalog.TableNotExistException {
        wrapped.rollbackTo(identifier, instant, fromSnapshot);
    }

    @Override
    public void createBranch(Identifier identifier, String branch, @Nullable String fromTag)
            throws TableNotExistException, BranchAlreadyExistException, TagNotExistException {
        wrapped.createBranch(identifier, branch, fromTag);
    }

    @Override
    public void dropBranch(Identifier identifier, String branch) throws BranchNotExistException {
        wrapped.dropBranch(identifier, branch);
    }

    @Override
    public void fastForward(Identifier identifier, String branch) throws BranchNotExistException {
        wrapped.fastForward(identifier, branch);
    }

    @Override
    public List<String> listBranches(Identifier identifier) throws TableNotExistException {
        return wrapped.listBranches(identifier);
    }

    @Override
    public GetTagResponse getTag(Identifier identifier, String tagName)
            throws TableNotExistException, TagNotExistException {
        return wrapped.getTag(identifier, tagName);
    }

    @Override
    public void createTag(
            Identifier identifier,
            String tagName,
            @Nullable Long snapshotId,
            @Nullable String timeRetained,
            boolean ignoreIfExists)
            throws TableNotExistException, SnapshotNotExistException, TagAlreadyExistException {
        wrapped.createTag(identifier, tagName, snapshotId, timeRetained, ignoreIfExists);
    }

    @Override
    public PagedList<String> listTagsPaged(
            Identifier identifier, @Nullable Integer maxResults, @Nullable String pageToken)
            throws TableNotExistException {
        return wrapped.listTagsPaged(identifier, maxResults, pageToken);
    }

    @Override
    public void deleteTag(Identifier identifier, String tagName)
            throws TableNotExistException, TagNotExistException {
        wrapped.deleteTag(identifier, tagName);
    }

    @Override
    public boolean commitSnapshot(
            Identifier identifier,
            @Nullable String tableUuid,
            Snapshot snapshot,
            List<PartitionStatistics> statistics)
            throws TableNotExistException {
        return wrapped.commitSnapshot(identifier, tableUuid, snapshot, statistics);
    }

    @Override
    public void createPartitions(Identifier identifier, List<Map<String, String>> partitions)
            throws TableNotExistException {
        wrapped.createPartitions(identifier, partitions);
    }

    @Override
    public void dropPartitions(Identifier identifier, List<Map<String, String>> partitions)
            throws TableNotExistException {
        wrapped.dropPartitions(identifier, partitions);
    }

    @Override
    public void alterPartitions(Identifier identifier, List<PartitionStatistics> partitions)
            throws TableNotExistException {
        wrapped.alterPartitions(identifier, partitions);
    }

    @Override
    public List<String> listFunctions(String databaseName) throws DatabaseNotExistException {
        return wrapped.listFunctions(databaseName);
    }

    @Override
    public Function getFunction(Identifier identifier) throws FunctionNotExistException {
        return wrapped.getFunction(identifier);
    }

    @Override
    public void createFunction(Identifier identifier, Function function, boolean ignoreIfExists)
            throws FunctionAlreadyExistException, DatabaseNotExistException {
        wrapped.createFunction(identifier, function, ignoreIfExists);
    }

    @Override
    public void dropFunction(Identifier identifier, boolean ignoreIfNotExists)
            throws FunctionNotExistException {
        wrapped.dropFunction(identifier, ignoreIfNotExists);
    }

    @Override
    public void alterFunction(
            Identifier identifier, List<FunctionChange> changes, boolean ignoreIfNotExists)
            throws FunctionNotExistException, DefinitionAlreadyExistException,
                    DefinitionNotExistException {
        wrapped.alterFunction(identifier, changes, ignoreIfNotExists);
    }

    @Override
    public void markDonePartitions(Identifier identifier, List<Map<String, String>> partitions)
            throws TableNotExistException {
        wrapped.markDonePartitions(identifier, partitions);
    }

    @Override
    public Table getTable(Identifier identifier) throws TableNotExistException {
        return wrapped.getTable(identifier);
    }

    @Override
    public View getView(Identifier identifier) throws ViewNotExistException {
        return wrapped.getView(identifier);
    }

    @Override
    public void dropView(Identifier identifier, boolean ignoreIfNotExists)
            throws ViewNotExistException {
        wrapped.dropView(identifier, ignoreIfNotExists);
    }

    @Override
    public void createView(Identifier identifier, View view, boolean ignoreIfExists)
            throws ViewAlreadyExistException, DatabaseNotExistException {
        wrapped.createView(identifier, view, ignoreIfExists);
    }

    @Override
    public List<String> listViews(String databaseName) throws DatabaseNotExistException {
        return wrapped.listViews(databaseName);
    }

    @Override
    public PagedList<String> listViewsPaged(
            String databaseName, Integer maxResults, String pageToken, String tableNamePattern)
            throws DatabaseNotExistException {
        return wrapped.listViewsPaged(databaseName, maxResults, pageToken, tableNamePattern);
    }

    @Override
    public PagedList<View> listViewDetailsPaged(
            String databaseName, Integer maxResults, String pageToken, String tableNamePattern)
            throws DatabaseNotExistException {
        return wrapped.listViewDetailsPaged(databaseName, maxResults, pageToken, tableNamePattern);
    }

    @Override
    public PagedList<Identifier> listViewsPagedGlobally(
            String databaseNamePattern,
            String viewNamePattern,
            Integer maxResults,
            String pageToken) {
        return wrapped.listViewsPagedGlobally(
                databaseNamePattern, viewNamePattern, maxResults, pageToken);
    }

    @Override
    public void renameView(Identifier fromView, Identifier toView, boolean ignoreIfNotExists)
            throws ViewNotExistException, ViewAlreadyExistException {
        wrapped.renameView(fromView, toView, ignoreIfNotExists);
    }

    @Override
    public void alterView(Identifier view, List<ViewChange> viewChanges, boolean ignoreIfNotExists)
            throws ViewNotExistException, DialectAlreadyExistException, DialectNotExistException {
        wrapped.alterView(view, viewChanges, ignoreIfNotExists);
    }

    @Override
    public List<Partition> listPartitions(Identifier identifier) throws TableNotExistException {
        return wrapped.listPartitions(identifier);
    }

    @Override
    public PagedList<Partition> listPartitionsPaged(
            Identifier identifier,
            Integer maxResults,
            String pageToken,
            String partitionNamePattern)
            throws TableNotExistException {
        return wrapped.listPartitionsPaged(identifier, maxResults, pageToken, partitionNamePattern);
    }

    @Override
    public List<String> authTableQuery(Identifier identifier, @Nullable List<String> select)
            throws TableNotExistException {
        return wrapped.authTableQuery(identifier, select);
    }

    @Override
    public void repairCatalog() {
        wrapped.repairCatalog();
    }

    @Override
    public void repairDatabase(String databaseName) {
        wrapped.repairDatabase(databaseName);
    }

    @Override
    public void repairTable(Identifier identifier) throws TableNotExistException {
        wrapped.repairTable(identifier);
    }

    @Override
    public void close() throws Exception {
        wrapped.close();
    }

    public static Catalog rootCatalog(Catalog catalog) {
        while (catalog instanceof DelegateCatalog) {
            catalog = ((DelegateCatalog) catalog).wrapped();
        }
        return catalog;
    }
}
