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

package org.apache.flink.table.store.connector;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

import org.apache.flink.shaded.guava30.com.google.common.collect.ImmutableList;

import org.junit.Before;

import java.io.IOException;
import java.util.List;

/** ITCase for catalog. */
public abstract class CatalogITCaseBase extends AbstractTestBase {

    protected TableEnvironment tEnv;

    @Before
    public void before() throws IOException {
        tEnv = TableEnvironment.create(EnvironmentSettings.newInstance().inBatchMode().build());
        tEnv.executeSql(
                String.format(
                        "CREATE CATALOG TABLE_STORE WITH ("
                                + "'type'='table-store', 'warehouse'='%s')",
                        TEMPORARY_FOLDER.newFolder().toURI()));
        tEnv.useCatalog("TABLE_STORE");
    }

    protected List<Row> sql(String query, Object... args) throws Exception {
        try (CloseableIterator<Row> iter = tEnv.executeSql(String.format(query, args)).collect()) {
            return ImmutableList.copyOf(iter);
        }
    }

    protected CatalogTable table(String tableName) throws TableNotExistException {
        Catalog catalog = tEnv.getCatalog(tEnv.getCurrentCatalog()).get();
        CatalogBaseTable table =
                catalog.getTable(new ObjectPath(catalog.getDefaultDatabase(), tableName));
        return (CatalogTable) table;
    }
}
