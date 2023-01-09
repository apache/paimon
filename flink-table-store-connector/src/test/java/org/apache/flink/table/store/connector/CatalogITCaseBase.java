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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
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
import java.time.Duration;
import java.util.Collections;
import java.util.List;

import static org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions.CHECKPOINTING_INTERVAL;

/** ITCase for catalog. */
public abstract class CatalogITCaseBase extends AbstractTestBase {

    protected TableEnvironment tEnv;
    protected TableEnvironment sEnv;

    @Before
    public void before() throws IOException {
        tEnv = TableEnvironment.create(EnvironmentSettings.newInstance().inBatchMode().build());
        String catalog = "TABLE_STORE";
        tEnv.executeSql(
                String.format(
                        "CREATE CATALOG %s WITH (" + "'type'='table-store', 'warehouse'='%s')",
                        catalog, TEMPORARY_FOLDER.newFolder().toURI()));
        tEnv.useCatalog(catalog);

        sEnv = TableEnvironment.create(EnvironmentSettings.newInstance().inStreamingMode().build());
        sEnv.getConfig().getConfiguration().set(CHECKPOINTING_INTERVAL, Duration.ofMillis(100));
        sEnv.registerCatalog(catalog, tEnv.getCatalog(catalog).get());
        sEnv.useCatalog(catalog);

        prepareConfiguration(tEnv);
        prepareConfiguration(sEnv);

        for (String ddl : ddl()) {
            tEnv.executeSql(ddl);
        }
    }

    private void prepareConfiguration(TableEnvironment env) {
        Configuration config = env.getConfig().getConfiguration();
        config.set(
                ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM,
                defaultParallelism());
    }

    protected int defaultParallelism() {
        return 2;
    }

    protected List<String> ddl() {
        return Collections.emptyList();
    }

    protected List<Row> batchSql(String query, Object... args) {
        return sql(query, args);
    }

    protected List<Row> sql(String query, Object... args) {
        try (CloseableIterator<Row> iter = tEnv.executeSql(String.format(query, args)).collect()) {
            return ImmutableList.copyOf(iter);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    protected CloseableIterator<Row> streamSqlIter(String query, Object... args) {
        return sEnv.executeSql(String.format(query, args)).collect();
    }

    protected CatalogTable table(String tableName) throws TableNotExistException {
        Catalog catalog = tEnv.getCatalog(tEnv.getCurrentCatalog()).get();
        CatalogBaseTable table =
                catalog.getTable(new ObjectPath(catalog.getDefaultDatabase(), tableName));
        return (CatalogTable) table;
    }
}
