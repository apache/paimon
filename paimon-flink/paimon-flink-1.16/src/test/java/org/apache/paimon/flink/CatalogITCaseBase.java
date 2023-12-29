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

import org.apache.paimon.shade.guava30.com.google.common.collect.ImmutableList;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.delegation.Parser;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.ddl.CreateCatalogOperation;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.BeforeEach;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.List;

import static org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions.CHECKPOINTING_INTERVAL;

/** ITCase for catalog. */
public abstract class CatalogITCaseBase extends AbstractTestBase {

    protected TableEnvironment tEnv;
    protected TableEnvironment sEnv;
    protected String path;

    @BeforeEach
    public void before() throws IOException {
        tEnv = TableEnvironment.create(EnvironmentSettings.newInstance().inBatchMode().build());
        String catalog = "PAIMON";
        path = getTempDirPath("paimon");
        tEnv.executeSql(
                String.format(
                        "CREATE CATALOG %s WITH (" + "'type'='paimon', 'warehouse'='%s')",
                        catalog, path));
        tEnv.useCatalog(catalog);

        sEnv = TableEnvironment.create(EnvironmentSettings.newInstance().inStreamingMode().build());
        sEnv.getConfig().getConfiguration().set(CHECKPOINTING_INTERVAL, Duration.ofMillis(100));
        sEnv.registerCatalog(catalog, tEnv.getCatalog(catalog).get());
        sEnv.useCatalog(catalog);

        setParallelism(defaultParallelism());
        prepareEnv();
    }

    private void prepareEnv() {
        Parser parser = ((TableEnvironmentImpl) tEnv).getParser();
        for (String ddl : ddl()) {
            tEnv.executeSql(ddl);
            List<Operation> operations = parser.parse(ddl);
            if (operations.size() == 1) {
                Operation operation = operations.get(0);
                if (operation instanceof CreateCatalogOperation) {
                    String name = ((CreateCatalogOperation) operation).getCatalogName();
                    sEnv.registerCatalog(name, tEnv.getCatalog(name).orElse(null));
                }
            }
        }
    }

    protected void setParallelism(int parallelism) {
        tEnv.getConfig()
                .getConfiguration()
                .set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, parallelism);
        sEnv.getConfig()
                .getConfiguration()
                .set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, parallelism);
    }

    protected int defaultParallelism() {
        return 2;
    }

    protected List<String> ddl() {
        return Collections.emptyList();
    }

    protected List<Row> sql(String query, Object... args) {
        try (CloseableIterator<Row> iter = tEnv.executeSql(String.format(query, args)).collect()) {
            return ImmutableList.copyOf(iter);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    protected CatalogTable table(String tableName) throws TableNotExistException {
        Catalog catalog = tEnv.getCatalog(tEnv.getCurrentCatalog()).get();
        CatalogBaseTable table =
                catalog.getTable(new ObjectPath(catalog.getDefaultDatabase(), tableName));
        return (CatalogTable) table;
    }
}
