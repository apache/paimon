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

package org.apache.flink.table.store.file.catalog;

import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.CatalogDatabase;
import org.apache.flink.table.catalog.CatalogDatabaseImpl;
import org.apache.flink.table.catalog.CatalogFunction;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.CatalogTestBase;
import org.apache.flink.table.catalog.CatalogTestUtil;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.DatabaseAlreadyExistException;

import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link TableStoreCatalog}. */
public abstract class TableStoreCatalogTest extends CatalogTestBase {

    @Override
    protected boolean isGeneric() {
        return false;
    }

    @Override
    public CatalogDatabase createDb() {
        return new CatalogDatabaseImpl(Collections.emptyMap(), "");
    }

    @Override
    public CatalogTable createAnotherTable() {
        // TODO support change schema, modify it to createAnotherSchema
        ResolvedSchema resolvedSchema = this.createSchema();
        CatalogTable origin =
                CatalogTable.of(
                        Schema.newBuilder().fromResolvedSchema(resolvedSchema).build(),
                        "test comment",
                        Collections.emptyList(),
                        this.getBatchTableProperties());
        return new ResolvedCatalogTable(origin, resolvedSchema);
    }

    @Override
    public CatalogTable createAnotherPartitionedTable() {
        // TODO support change schema, modify it to createAnotherSchema
        ResolvedSchema resolvedSchema = this.createSchema();
        CatalogTable origin =
                CatalogTable.of(
                        Schema.newBuilder().fromResolvedSchema(resolvedSchema).build(),
                        "test comment",
                        this.createPartitionKeys(),
                        this.getBatchTableProperties());
        return new ResolvedCatalogTable(origin, resolvedSchema);
    }

    @Override
    public CatalogDatabase createAnotherDb() {
        // Not support database with properties or comment
        return new CatalogDatabaseImpl(new HashMap<String, String>() {}, null);
    }

    @Override
    public void testAlterTable() throws Exception {
        catalog.createDatabase("db1", this.createDb(), false);
        CatalogTable table = this.createTable();
        catalog.createTable(this.path1, table, false);
        CatalogTestUtil.checkEquals(table, (CatalogTable) catalog.getTable(this.path1));
        CatalogTable newTable = this.createAnotherTable();
        catalog.alterTable(this.path1, newTable, false);
        Assert.assertNotEquals(table, catalog.getTable(this.path1));
        CatalogTestUtil.checkEquals(newTable, (CatalogTable) catalog.getTable(this.path1));
        catalog.dropTable(this.path1, false);

        // Not support views
    }

    @Test
    public void testListTables() throws Exception {
        catalog.createDatabase("db1", this.createDb(), false);
        catalog.createTable(this.path1, this.createTable(), false);
        catalog.createTable(this.path3, this.createTable(), false);
        Assert.assertEquals(2L, catalog.listTables("db1").size());

        // Not support views
    }

    @Override
    public void testAlterTable_differentTypedTable() {
        // TODO support this
    }

    @Test
    public void testCreateFlinkTable() throws DatabaseAlreadyExistException {
        // create a flink table
        CatalogTable table = createTable();
        HashMap<String, String> newOptions = new HashMap<>(table.getOptions());
        newOptions.put("connector", "filesystem");
        CatalogTable newTable = table.copy(newOptions);

        catalog.createDatabase("db1", this.createDb(), false);

        assertThatThrownBy(() -> catalog.createTable(this.path1, newTable, false))
                .isInstanceOf(CatalogException.class)
                .hasMessageContaining(
                        "Table Store Catalog only supports table store tables, not Flink connector: filesystem");
    }

    // --------------------- unsupported methods ----------------------------

    @Override
    protected CatalogFunction createFunction() {
        throw new UnsupportedOperationException();
    }

    @Override
    protected CatalogFunction createPythonFunction() {
        throw new UnsupportedOperationException();
    }

    @Override
    protected CatalogFunction createAnotherFunction() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void testCreateView() {}

    @Override
    public void testAlterDb() {}

    @Override
    public void testAlterDb_DatabaseNotExistException() {}

    @Override
    public void testAlterDb_DatabaseNotExist_ignored() {}

    @Override
    public void testRenameTable_nonPartitionedTable() {}

    @Override
    public void testRenameTable_TableNotExistException_ignored() {}

    @Override
    public void testRenameTable_TableNotExistException() {}

    @Override
    public void testRenameTable_TableAlreadyExistException() {}

    @Override
    public void testCreateView_DatabaseNotExistException() {}

    @Override
    public void testCreateView_TableAlreadyExistException() {}

    @Override
    public void testCreateView_TableAlreadyExist_ignored() {}

    @Override
    public void testDropView() {}

    @Override
    public void testAlterView() {}

    @Override
    public void testAlterView_TableNotExistException() {}

    @Override
    public void testAlterView_TableNotExist_ignored() {}

    @Override
    public void testListView() {}

    @Override
    public void testRenameView() {}

    @Override
    public void testCreateFunction() {}

    @Override
    public void testCreatePythonFunction() {}

    @Override
    public void testCreateFunction_DatabaseNotExistException() {}

    @Override
    public void testCreateFunction_FunctionAlreadyExistException() {}

    @Override
    public void testAlterFunction() {}

    @Override
    public void testAlterPythonFunction() {}

    @Override
    public void testAlterFunction_FunctionNotExistException() {}

    @Override
    public void testAlterFunction_FunctionNotExist_ignored() {}

    @Override
    public void testListFunctions() {}

    @Override
    public void testListFunctions_DatabaseNotExistException() {}

    @Override
    public void testGetFunction_FunctionNotExistException() {}

    @Override
    public void testGetFunction_FunctionNotExistException_NoDb() {}

    @Override
    public void testDropFunction() {}

    @Override
    public void testDropFunction_FunctionNotExistException() {}

    @Override
    public void testDropFunction_FunctionNotExist_ignored() {}

    @Override
    public void testCreatePartition() {}

    @Override
    public void testCreatePartition_TableNotExistException() {}

    @Override
    public void testCreatePartition_TableNotPartitionedException() {}

    @Override
    public void testCreatePartition_PartitionSpecInvalidException() {}

    @Override
    public void testCreatePartition_PartitionAlreadyExistsException() {}

    @Override
    public void testCreatePartition_PartitionAlreadyExists_ignored() {}

    @Override
    public void testDropPartition() {}

    @Override
    public void testDropPartition_TableNotExist() {}

    @Override
    public void testDropPartition_TableNotPartitioned() {}

    @Override
    public void testDropPartition_PartitionSpecInvalid() {}

    @Override
    public void testDropPartition_PartitionNotExist() {}

    @Override
    public void testDropPartition_PartitionNotExist_ignored() {}

    @Override
    public void testAlterPartition() {}

    @Override
    public void testAlterPartition_TableNotExist() {}

    @Override
    public void testAlterPartition_TableNotPartitioned() {}

    @Override
    public void testAlterPartition_PartitionSpecInvalid() {}

    @Override
    public void testAlterPartition_PartitionNotExist() {}

    @Override
    public void testAlterPartition_PartitionNotExist_ignored() {}

    @Override
    public void testGetPartition_TableNotExist() {}

    @Override
    public void testGetPartition_TableNotPartitioned() {}

    @Override
    public void testGetPartition_PartitionSpecInvalid_invalidPartitionSpec() {}

    @Override
    public void testGetPartition_PartitionSpecInvalid_sizeNotEqual() {}

    @Override
    public void testGetPartition_PartitionNotExist() {}

    @Override
    public void testPartitionExists() {}

    @Override
    public void testListPartitionPartialSpec() {}

    @Override
    public void testGetTableStats_TableNotExistException() {}

    @Override
    public void testGetPartitionStats() {}

    @Override
    public void testAlterTableStats() {}

    @Override
    public void testAlterTableStats_partitionedTable() {}

    @Override
    public void testAlterPartitionTableStats() {}

    @Override
    public void testAlterTableStats_TableNotExistException() {}

    @Override
    public void testAlterTableStats_TableNotExistException_ignore() {}
}
