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

package org.apache.paimon.flink.lakestream;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogDatabase;
import org.apache.flink.table.catalog.CatalogFunction;
import org.apache.flink.table.catalog.CatalogPartition;
import org.apache.flink.table.catalog.CatalogPartitionSpec;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.FunctionNotExistException;
import org.apache.flink.table.catalog.exceptions.PartitionNotExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.catalog.stats.CatalogColumnStatistics;
import org.apache.flink.table.catalog.stats.CatalogTableStatistics;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.factories.Factory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * A fake lake-stream store {@link org.apache.flink.table.catalog.Catalog} for tests. It records
 * {@link #getTable(ObjectPath)} calls and lifecycle state so tests can verify how {@link
 * FlinkCatalog} discovers, opens, delegates to and closes the store catalog.
 *
 * <p>{@link #getFactory()} exposes the fake store's table factories; the catalog option {@code
 * factory.mode} ({@code both} (default), {@code source-only}, {@code sink-only}, {@code none})
 * controls which kind is exposed so Paimon's factory-type validation can be tested.
 */
public class FakeStoreCatalog implements org.apache.flink.table.catalog.Catalog {

    public static final String SENTINEL_COMMENT = "fake store table";

    public static final String FACTORY_MODE_OPTION = "factory.mode";

    private static final List<ObjectPath> GET_TABLE_CALLS = new ArrayList<>();

    /** Paths {@link #getTable(ObjectPath)} reports as absent, so the error paths are reachable. */
    private static final List<ObjectPath> MISSING_TABLES =
            Collections.synchronizedList(new ArrayList<ObjectPath>());

    private final String name;
    private final Map<String, String> options;

    private boolean opened;
    private boolean closed;

    public FakeStoreCatalog(String name, Map<String, String> options) {
        this.name = name;
        this.options = options;
    }

    public static List<ObjectPath> getTableCalls() {
        return GET_TABLE_CALLS;
    }

    public static void resetGetTableCalls() {
        GET_TABLE_CALLS.clear();
        MISSING_TABLES.clear();
    }

    public static void markTableMissing(ObjectPath tablePath) {
        MISSING_TABLES.add(tablePath);
    }

    public boolean isOpened() {
        return opened;
    }

    public boolean isClosed() {
        return closed;
    }

    @Override
    public void open() {
        opened = true;
    }

    @Override
    public void close() {
        closed = true;
    }

    @Override
    public Optional<Factory> getFactory() {
        String mode = options.get(FACTORY_MODE_OPTION);
        if (mode == null || "both".equals(mode)) {
            return Optional.<Factory>of(new FakeStoreTableFactory());
        } else if ("source-only".equals(mode)) {
            return Optional.<Factory>of(new FakeStoreTableFactory.FakeStoreSourceFactory());
        } else if ("sink-only".equals(mode)) {
            return Optional.<Factory>of(new FakeStoreTableFactory.FakeStoreSinkFactory());
        } else if ("none".equals(mode)) {
            return Optional.empty();
        }
        throw new IllegalArgumentException("Unknown " + FACTORY_MODE_OPTION + ": " + mode);
    }

    @Override
    public String getDefaultDatabase() {
        return "default";
    }

    @Override
    public List<String> listDatabases() {
        return Collections.singletonList("default");
    }

    @Override
    public CatalogDatabase getDatabase(String databaseName) throws DatabaseNotExistException {
        throw new DatabaseNotExistException(name, databaseName);
    }

    @Override
    public boolean databaseExists(String databaseName) {
        return "default".equals(databaseName);
    }

    @Override
    public void createDatabase(
            String databaseName, CatalogDatabase database, boolean ignoreIfExists) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void dropDatabase(String databaseName, boolean ignoreIfNotExists, boolean cascade) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void alterDatabase(
            String databaseName, CatalogDatabase newDatabase, boolean ignoreIfNotExists) {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<String> listTables(String databaseName) throws DatabaseNotExistException {
        if (!databaseExists(databaseName)) {
            throw new DatabaseNotExistException(name, databaseName);
        }
        return Collections.emptyList();
    }

    @Override
    public List<String> listViews(String databaseName) throws DatabaseNotExistException {
        if (!databaseExists(databaseName)) {
            throw new DatabaseNotExistException(name, databaseName);
        }
        return Collections.emptyList();
    }

    @Override
    public CatalogBaseTable getTable(ObjectPath tablePath) throws TableNotExistException {
        GET_TABLE_CALLS.add(tablePath);
        if (MISSING_TABLES.contains(tablePath)) {
            throw new TableNotExistException(name, tablePath);
        }
        Map<String, String> tableOptions = new HashMap<>();
        tableOptions.put("connector", FakeStoreCatalogFactory.IDENTIFIER);
        // CatalogTable#of was removed in Flink 2.x, so build it the way both versions accept
        return CatalogTable.newBuilder()
                .schema(Schema.newBuilder().column("id", DataTypes.INT()).build())
                .comment(SENTINEL_COMMENT)
                .partitionKeys(Collections.<String>emptyList())
                .options(tableOptions)
                .build();
    }

    @Override
    public boolean tableExists(ObjectPath tablePath) {
        return false;
    }

    @Override
    public void dropTable(ObjectPath tablePath, boolean ignoreIfNotExists) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void renameTable(ObjectPath tablePath, String newTableName, boolean ignoreIfNotExists) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void createTable(ObjectPath tablePath, CatalogBaseTable table, boolean ignoreIfExists) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void alterTable(
            ObjectPath tablePath, CatalogBaseTable newTable, boolean ignoreIfNotExists) {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<CatalogPartitionSpec> listPartitions(ObjectPath tablePath) {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<CatalogPartitionSpec> listPartitions(
            ObjectPath tablePath, CatalogPartitionSpec partitionSpec) {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<CatalogPartitionSpec> listPartitionsByFilter(
            ObjectPath tablePath, List<Expression> filters) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CatalogPartition getPartition(ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
            throws PartitionNotExistException {
        throw new PartitionNotExistException(name, tablePath, partitionSpec);
    }

    @Override
    public boolean partitionExists(ObjectPath tablePath, CatalogPartitionSpec partitionSpec) {
        return false;
    }

    @Override
    public void createPartition(
            ObjectPath tablePath,
            CatalogPartitionSpec partitionSpec,
            CatalogPartition partition,
            boolean ignoreIfExists) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void dropPartition(
            ObjectPath tablePath, CatalogPartitionSpec partitionSpec, boolean ignoreIfNotExists) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void alterPartition(
            ObjectPath tablePath,
            CatalogPartitionSpec partitionSpec,
            CatalogPartition newPartition,
            boolean ignoreIfNotExists) {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<String> listFunctions(String databaseName) throws DatabaseNotExistException {
        if (!databaseExists(databaseName)) {
            throw new DatabaseNotExistException(name, databaseName);
        }
        return Collections.emptyList();
    }

    @Override
    public CatalogFunction getFunction(ObjectPath functionPath) throws FunctionNotExistException {
        throw new FunctionNotExistException(name, functionPath);
    }

    @Override
    public boolean functionExists(ObjectPath functionPath) {
        return false;
    }

    @Override
    public void createFunction(
            ObjectPath functionPath, CatalogFunction function, boolean ignoreIfExists) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void alterFunction(
            ObjectPath functionPath, CatalogFunction newFunction, boolean ignoreIfNotExists) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void dropFunction(ObjectPath functionPath, boolean ignoreIfNotExists) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CatalogTableStatistics getTableStatistics(ObjectPath tablePath) {
        return CatalogTableStatistics.UNKNOWN;
    }

    @Override
    public CatalogColumnStatistics getTableColumnStatistics(ObjectPath tablePath) {
        return CatalogColumnStatistics.UNKNOWN;
    }

    @Override
    public CatalogTableStatistics getPartitionStatistics(
            ObjectPath tablePath, CatalogPartitionSpec partitionSpec) {
        return CatalogTableStatistics.UNKNOWN;
    }

    @Override
    public CatalogColumnStatistics getPartitionColumnStatistics(
            ObjectPath tablePath, CatalogPartitionSpec partitionSpec) {
        return CatalogColumnStatistics.UNKNOWN;
    }

    @Override
    public void alterTableStatistics(
            ObjectPath tablePath,
            CatalogTableStatistics tableStatistics,
            boolean ignoreIfNotExists) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void alterTableColumnStatistics(
            ObjectPath tablePath,
            CatalogColumnStatistics columnStatistics,
            boolean ignoreIfNotExists) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void alterPartitionStatistics(
            ObjectPath tablePath,
            CatalogPartitionSpec partitionSpec,
            CatalogTableStatistics partitionStatistics,
            boolean ignoreIfNotExists) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void alterPartitionColumnStatistics(
            ObjectPath tablePath,
            CatalogPartitionSpec partitionSpec,
            CatalogColumnStatistics columnStatistics,
            boolean ignoreIfNotExists) {
        throw new UnsupportedOperationException();
    }
}
