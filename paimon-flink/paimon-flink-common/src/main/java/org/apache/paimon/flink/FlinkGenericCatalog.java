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

package org.apache.paimon.flink;

import org.apache.paimon.flink.procedure.ProcedureUtil;

import org.apache.flink.table.catalog.AbstractCatalog;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogDatabase;
import org.apache.flink.table.catalog.CatalogFunction;
import org.apache.flink.table.catalog.CatalogPartition;
import org.apache.flink.table.catalog.CatalogPartitionSpec;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.TableChange;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.DatabaseAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotEmptyException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.FunctionAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.FunctionNotExistException;
import org.apache.flink.table.catalog.exceptions.PartitionAlreadyExistsException;
import org.apache.flink.table.catalog.exceptions.PartitionNotExistException;
import org.apache.flink.table.catalog.exceptions.PartitionSpecInvalidException;
import org.apache.flink.table.catalog.exceptions.ProcedureNotExistException;
import org.apache.flink.table.catalog.exceptions.TableAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.catalog.exceptions.TableNotPartitionedException;
import org.apache.flink.table.catalog.exceptions.TablePartitionedException;
import org.apache.flink.table.catalog.stats.CatalogColumnStatistics;
import org.apache.flink.table.catalog.stats.CatalogTableStatistics;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.factories.Factory;
import org.apache.flink.table.factories.FunctionDefinitionFactory;
import org.apache.flink.table.procedures.Procedure;

import java.util.List;
import java.util.Optional;

import static org.apache.flink.table.factories.FactoryUtil.CONNECTOR;

/** A Flink catalog that can also load non-Paimon tables. */
public class FlinkGenericCatalog extends AbstractCatalog {

    private final FlinkCatalog paimon;
    private final Catalog flink;

    public FlinkGenericCatalog(FlinkCatalog paimon, Catalog flink) {
        super(paimon.getName(), paimon.getDefaultDatabase());
        this.paimon = paimon;
        this.flink = flink;
    }

    @Override
    public void open() throws CatalogException {
        paimon.open();
        flink.open();
    }

    @Override
    public void close() throws CatalogException {
        paimon.close();
        flink.close();
    }

    @Override
    public Optional<Factory> getFactory() {
        return Optional.of(
                new FlinkGenericTableFactory(paimon.getFactory().get(), flink.getFactory().get()));
    }

    @Override
    public Optional<FunctionDefinitionFactory> getFunctionDefinitionFactory() {
        return flink.getFunctionDefinitionFactory();
    }

    @Override
    public List<String> listDatabases() throws CatalogException {
        return flink.listDatabases();
    }

    @Override
    public CatalogDatabase getDatabase(String databaseName)
            throws DatabaseNotExistException, CatalogException {
        return flink.getDatabase(databaseName);
    }

    @Override
    public boolean databaseExists(String databaseName) throws CatalogException {
        return flink.databaseExists(databaseName);
    }

    @Override
    public void createDatabase(String name, CatalogDatabase database, boolean ignoreIfExists)
            throws DatabaseAlreadyExistException, CatalogException {
        flink.createDatabase(name, database, ignoreIfExists);
    }

    @Override
    public void dropDatabase(String name, boolean ignoreIfNotExists, boolean cascade)
            throws DatabaseNotExistException, DatabaseNotEmptyException, CatalogException {
        flink.dropDatabase(name, ignoreIfNotExists, cascade);
    }

    @Override
    public void alterDatabase(String name, CatalogDatabase newDatabase, boolean ignoreIfNotExists)
            throws DatabaseNotExistException, CatalogException {
        flink.alterDatabase(name, newDatabase, ignoreIfNotExists);
    }

    @Override
    public List<String> listTables(String databaseName)
            throws DatabaseNotExistException, CatalogException {
        // flink list tables contains all paimon tables
        return flink.listTables(databaseName);
    }

    @Override
    public List<String> listViews(String databaseName)
            throws DatabaseNotExistException, CatalogException {
        return flink.listViews(databaseName);
    }

    @Override
    public CatalogBaseTable getTable(ObjectPath tablePath)
            throws TableNotExistException, CatalogException {
        try {
            return paimon.getTable(tablePath);
        } catch (TableNotExistException e) {
            return flink.getTable(tablePath);
        }
    }

    @Override
    public boolean tableExists(ObjectPath tablePath) throws CatalogException {
        if (isPaimonTable(tablePath)) {
            return true;
        } else {
            return flink.tableExists(tablePath);
        }
    }

    @Override
    public void dropTable(ObjectPath tablePath, boolean ignoreIfNotExists)
            throws TableNotExistException, CatalogException {
        try {
            paimon.dropTable(tablePath, false);
        } catch (TableNotExistException e) {
            flink.dropTable(tablePath, ignoreIfNotExists);
        }
    }

    @Override
    public void renameTable(ObjectPath tablePath, String newTableName, boolean ignoreIfNotExists)
            throws TableNotExistException, TableAlreadyExistException, CatalogException {
        try {
            paimon.renameTable(tablePath, newTableName, false);
        } catch (TableNotExistException e) {
            flink.renameTable(tablePath, newTableName, ignoreIfNotExists);
        }
    }

    @Override
    public void createTable(ObjectPath tablePath, CatalogBaseTable table, boolean ignoreIfExists)
            throws TableAlreadyExistException, DatabaseNotExistException, CatalogException {
        String connector = table.getOptions().get(CONNECTOR.key());
        if (connector == null) {
            throw new RuntimeException(
                    "FlinkGenericCatalog can not create table without 'connector' key.");
        }
        if (FlinkCatalogFactory.IDENTIFIER.equals(connector)) {
            paimon.createTable(tablePath, table, ignoreIfExists);
        } else {
            flink.createTable(tablePath, table, ignoreIfExists);
        }
    }

    private boolean isPaimonTable(ObjectPath tablePath) {
        try {
            paimon.getTable(tablePath);
            return true;
        } catch (TableNotExistException e) {
            return false;
        }
    }

    @Override
    public void alterTable(
            ObjectPath tablePath, CatalogBaseTable newTable, boolean ignoreIfNotExists)
            throws TableNotExistException, CatalogException {
        if (isPaimonTable(tablePath)) {
            paimon.alterTable(tablePath, newTable, ignoreIfNotExists);
        } else {
            flink.alterTable(tablePath, newTable, ignoreIfNotExists);
        }
    }

    @Override
    public void alterTable(
            ObjectPath tablePath,
            CatalogBaseTable newTable,
            List<TableChange> tableChanges,
            boolean ignoreIfNotExists)
            throws TableNotExistException, CatalogException {
        if (isPaimonTable(tablePath)) {
            paimon.alterTable(tablePath, newTable, tableChanges, ignoreIfNotExists);
        } else {
            flink.alterTable(tablePath, newTable, tableChanges, ignoreIfNotExists);
        }
    }

    @Override
    public List<CatalogPartitionSpec> listPartitions(ObjectPath tablePath)
            throws TableNotExistException, TableNotPartitionedException, CatalogException {
        if (isPaimonTable(tablePath)) {
            return paimon.listPartitions(tablePath);
        } else {
            return flink.listPartitions(tablePath);
        }
    }

    @Override
    public List<CatalogPartitionSpec> listPartitions(
            ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
            throws TableNotExistException, TableNotPartitionedException,
                    PartitionSpecInvalidException, CatalogException {
        if (isPaimonTable(tablePath)) {
            return paimon.listPartitions(tablePath, partitionSpec);
        } else {
            return flink.listPartitions(tablePath, partitionSpec);
        }
    }

    @Override
    public List<CatalogPartitionSpec> listPartitionsByFilter(
            ObjectPath tablePath, List<Expression> filters)
            throws TableNotExistException, TableNotPartitionedException, CatalogException {
        if (isPaimonTable(tablePath)) {
            return paimon.listPartitionsByFilter(tablePath, filters);
        } else {
            return flink.listPartitionsByFilter(tablePath, filters);
        }
    }

    @Override
    public CatalogPartition getPartition(ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
            throws PartitionNotExistException, CatalogException {
        if (isPaimonTable(tablePath)) {
            return paimon.getPartition(tablePath, partitionSpec);
        } else {
            return flink.getPartition(tablePath, partitionSpec);
        }
    }

    @Override
    public boolean partitionExists(ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
            throws CatalogException {
        if (isPaimonTable(tablePath)) {
            return paimon.partitionExists(tablePath, partitionSpec);
        } else {
            return flink.partitionExists(tablePath, partitionSpec);
        }
    }

    @Override
    public void createPartition(
            ObjectPath tablePath,
            CatalogPartitionSpec partitionSpec,
            CatalogPartition partition,
            boolean ignoreIfExists)
            throws TableNotExistException, TableNotPartitionedException,
                    PartitionSpecInvalidException, PartitionAlreadyExistsException,
                    CatalogException {
        if (isPaimonTable(tablePath)) {
            paimon.createPartition(tablePath, partitionSpec, partition, ignoreIfExists);
        } else {
            flink.createPartition(tablePath, partitionSpec, partition, ignoreIfExists);
        }
    }

    @Override
    public void dropPartition(
            ObjectPath tablePath, CatalogPartitionSpec partitionSpec, boolean ignoreIfNotExists)
            throws PartitionNotExistException, CatalogException {
        if (isPaimonTable(tablePath)) {
            paimon.dropPartition(tablePath, partitionSpec, ignoreIfNotExists);
        } else {
            flink.dropPartition(tablePath, partitionSpec, ignoreIfNotExists);
        }
    }

    @Override
    public void alterPartition(
            ObjectPath tablePath,
            CatalogPartitionSpec partitionSpec,
            CatalogPartition newPartition,
            boolean ignoreIfNotExists)
            throws PartitionNotExistException, CatalogException {
        if (isPaimonTable(tablePath)) {
            paimon.alterPartition(tablePath, partitionSpec, newPartition, ignoreIfNotExists);
        } else {
            flink.alterPartition(tablePath, partitionSpec, newPartition, ignoreIfNotExists);
        }
    }

    @Override
    public List<String> listFunctions(String dbName)
            throws DatabaseNotExistException, CatalogException {
        return flink.listFunctions(dbName);
    }

    @Override
    public CatalogFunction getFunction(ObjectPath functionPath)
            throws FunctionNotExistException, CatalogException {
        return flink.getFunction(functionPath);
    }

    @Override
    public boolean functionExists(ObjectPath functionPath) throws CatalogException {
        return flink.functionExists(functionPath);
    }

    @Override
    public void createFunction(
            ObjectPath functionPath, CatalogFunction function, boolean ignoreIfExists)
            throws FunctionAlreadyExistException, DatabaseNotExistException, CatalogException {
        flink.createFunction(functionPath, function, ignoreIfExists);
    }

    @Override
    public void alterFunction(
            ObjectPath functionPath, CatalogFunction newFunction, boolean ignoreIfNotExists)
            throws FunctionNotExistException, CatalogException {
        flink.alterFunction(functionPath, newFunction, ignoreIfNotExists);
    }

    @Override
    public void dropFunction(ObjectPath functionPath, boolean ignoreIfNotExists)
            throws FunctionNotExistException, CatalogException {
        flink.dropFunction(functionPath, ignoreIfNotExists);
    }

    @Override
    public CatalogTableStatistics getTableStatistics(ObjectPath tablePath)
            throws TableNotExistException, CatalogException {
        if (isPaimonTable(tablePath)) {
            return paimon.getTableStatistics(tablePath);
        } else {
            return flink.getTableStatistics(tablePath);
        }
    }

    @Override
    public CatalogColumnStatistics getTableColumnStatistics(ObjectPath tablePath)
            throws TableNotExistException, CatalogException {
        if (isPaimonTable(tablePath)) {
            return paimon.getTableColumnStatistics(tablePath);
        } else {
            return flink.getTableColumnStatistics(tablePath);
        }
    }

    @Override
    public CatalogTableStatistics getPartitionStatistics(
            ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
            throws PartitionNotExistException, CatalogException {
        if (isPaimonTable(tablePath)) {
            return paimon.getPartitionStatistics(tablePath, partitionSpec);
        } else {
            return flink.getPartitionStatistics(tablePath, partitionSpec);
        }
    }

    @Override
    public CatalogColumnStatistics getPartitionColumnStatistics(
            ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
            throws PartitionNotExistException, CatalogException {
        if (isPaimonTable(tablePath)) {
            return paimon.getPartitionColumnStatistics(tablePath, partitionSpec);
        } else {
            return flink.getPartitionColumnStatistics(tablePath, partitionSpec);
        }
    }

    @Override
    public void alterTableStatistics(
            ObjectPath tablePath, CatalogTableStatistics tableStatistics, boolean ignoreIfNotExists)
            throws TableNotExistException, CatalogException {
        if (isPaimonTable(tablePath)) {
            paimon.alterTableStatistics(tablePath, tableStatistics, ignoreIfNotExists);
        } else {
            flink.alterTableStatistics(tablePath, tableStatistics, ignoreIfNotExists);
        }
    }

    @Override
    public void alterTableColumnStatistics(
            ObjectPath tablePath,
            CatalogColumnStatistics columnStatistics,
            boolean ignoreIfNotExists)
            throws TableNotExistException, CatalogException, TablePartitionedException {
        if (isPaimonTable(tablePath)) {
            paimon.alterTableColumnStatistics(tablePath, columnStatistics, ignoreIfNotExists);
        } else {
            flink.alterTableColumnStatistics(tablePath, columnStatistics, ignoreIfNotExists);
        }
    }

    @Override
    public void alterPartitionStatistics(
            ObjectPath tablePath,
            CatalogPartitionSpec partitionSpec,
            CatalogTableStatistics partitionStatistics,
            boolean ignoreIfNotExists)
            throws PartitionNotExistException, CatalogException {
        if (isPaimonTable(tablePath)) {
            paimon.alterPartitionStatistics(
                    tablePath, partitionSpec, partitionStatistics, ignoreIfNotExists);
        } else {
            flink.alterPartitionStatistics(
                    tablePath, partitionSpec, partitionStatistics, ignoreIfNotExists);
        }
    }

    @Override
    public void alterPartitionColumnStatistics(
            ObjectPath tablePath,
            CatalogPartitionSpec partitionSpec,
            CatalogColumnStatistics columnStatistics,
            boolean ignoreIfNotExists)
            throws PartitionNotExistException, CatalogException {
        if (isPaimonTable(tablePath)) {
            paimon.alterPartitionColumnStatistics(
                    tablePath, partitionSpec, columnStatistics, ignoreIfNotExists);
        } else {
            flink.alterPartitionColumnStatistics(
                    tablePath, partitionSpec, columnStatistics, ignoreIfNotExists);
        }
    }

    @Override
    public List<CatalogTableStatistics> bulkGetPartitionStatistics(
            ObjectPath tablePath, List<CatalogPartitionSpec> partitionSpecs)
            throws PartitionNotExistException, CatalogException {
        if (isPaimonTable(tablePath)) {
            return paimon.bulkGetPartitionStatistics(tablePath, partitionSpecs);
        } else {
            return flink.bulkGetPartitionStatistics(tablePath, partitionSpecs);
        }
    }

    @Override
    public List<CatalogColumnStatistics> bulkGetPartitionColumnStatistics(
            ObjectPath tablePath, List<CatalogPartitionSpec> partitionSpecs)
            throws PartitionNotExistException, CatalogException {
        if (isPaimonTable(tablePath)) {
            return paimon.bulkGetPartitionColumnStatistics(tablePath, partitionSpecs);
        } else {
            return flink.bulkGetPartitionColumnStatistics(tablePath, partitionSpecs);
        }
    }

    /**
     * Do not annotate with <code>@override</code> here to maintain compatibility with Flink 1.17-.
     */
    public List<String> listProcedures(String dbName)
            throws DatabaseNotExistException, CatalogException {
        if (paimon.databaseExists(dbName)) {
            return ProcedureUtil.listProcedures();
        }
        return flink.listProcedures(dbName);
    }

    /**
     * Do not annotate with <code>@override</code> here to maintain compatibility with Flink 1.17-.
     */
    public Procedure getProcedure(ObjectPath procedurePath)
            throws ProcedureNotExistException, CatalogException {
        Optional<Procedure> procedure = ProcedureUtil.getProcedure(paimon.catalog(), procedurePath);
        if (procedure.isPresent()) {
            return procedure.get();
        } else {
            return flink.getProcedure(procedurePath);
        }
    }
}
