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

import org.apache.paimon.Snapshot;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.flink.util.AbstractTestBase;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.utils.BlockingIterator;
import org.apache.paimon.utils.SnapshotManager;

import org.apache.paimon.shade.guava30.com.google.common.collect.ImmutableList;

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
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.ListAssert;
import org.junit.jupiter.api.BeforeEach;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/** ITCase for catalog. */
public abstract class CatalogITCaseBase extends AbstractTestBase {

    protected TableEnvironment tEnv;
    protected TableEnvironment sEnv;
    protected String path;

    @BeforeEach
    public void before() throws IOException {
        tEnv = tableEnvironmentBuilder().batchMode().build();
        String catalog = "PAIMON";
        path = getTempDirPath();
        String inferScan =
                !inferScanParallelism() ? ",\n'table-default.scan.infer-parallelism'='false'" : "";

        Map<String, String> options = new HashMap<>(catalogOptions());
        options.put("type", "paimon");
        options.put("warehouse", toWarehouse(path));
        tEnv.executeSql(
                String.format(
                        "CREATE CATALOG %s WITH (" + "%s" + inferScan + ")",
                        catalog,
                        options.entrySet().stream()
                                .map(e -> String.format("'%s'='%s'", e.getKey(), e.getValue()))
                                .collect(Collectors.joining(","))));
        tEnv.useCatalog(catalog);

        sEnv = tableEnvironmentBuilder().streamingMode().checkpointIntervalMs(100).build();
        sEnv.registerCatalog(catalog, tEnv.getCatalog(catalog).get());
        sEnv.useCatalog(catalog);

        setParallelism(defaultParallelism());
        prepareEnv();
    }

    protected Map<String, String> catalogOptions() {
        return Collections.emptyMap();
    }

    protected boolean inferScanParallelism() {
        return false;
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
                .set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, parallelism);
        sEnv.getConfig()
                .set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, parallelism);
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

    protected void sqlAssertWithRetry(
            String query, Consumer<ListAssert<Row>> checker, Object... args) {
        long start = System.currentTimeMillis();
        while (true) {
            try (CloseableIterator<Row> iter =
                    tEnv.executeSql(String.format(query, args)).collect()) {
                try {
                    checker.accept(Assertions.assertThat(ImmutableList.copyOf(iter)));
                    return;
                } catch (AssertionError e) {
                    if (System.currentTimeMillis() - start >= 3 * 60 * 1000) {
                        throw new RuntimeException(e);
                    }
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    protected CloseableIterator<Row> streamSqlIter(String query, Object... args) {
        return sEnv.executeSql(String.format(query, args)).collect();
    }

    protected BlockingIterator<Row, Row> streamSqlBlockIter(String query, Object... args) {
        return BlockingIterator.of(sEnv.executeSql(String.format(query, args)).collect());
    }

    protected CatalogTable table(String tableName) throws TableNotExistException {
        Catalog catalog = flinkCatalog();
        CatalogBaseTable table =
                catalog.getTable(new ObjectPath(catalog.getDefaultDatabase(), tableName));
        return (CatalogTable) table;
    }

    protected FileStoreTable paimonTable(String tableName)
            throws org.apache.paimon.catalog.Catalog.TableNotExistException {
        org.apache.paimon.catalog.Catalog catalog = flinkCatalog().catalog();
        return (FileStoreTable)
                catalog.getTable(Identifier.create(tEnv.getCurrentDatabase(), tableName));
    }

    protected FileStoreTable paimonTable(String database, String tableName)
            throws org.apache.paimon.catalog.Catalog.TableNotExistException {
        org.apache.paimon.catalog.Catalog catalog = flinkCatalog().catalog();
        return (FileStoreTable) catalog.getTable(Identifier.create(database, tableName));
    }

    private FlinkCatalog flinkCatalog() {
        return (FlinkCatalog) tEnv.getCatalog(tEnv.getCurrentCatalog()).get();
    }

    protected Path getTableDirectory(String tableName) {
        return new Path(
                new File(path, String.format("%s.db/%s", tEnv.getCurrentDatabase(), tableName))
                        .toString());
    }

    @Nullable
    protected Snapshot findLatestSnapshot(String tableName) {
        SnapshotManager snapshotManager =
                new SnapshotManager(LocalFileIO.create(), getTableDirectory(tableName));
        Long id = snapshotManager.latestSnapshotId();
        return id == null ? null : snapshotManager.snapshot(id);
    }

    @Nullable
    protected Snapshot findSnapshot(String tableName, long snapshotId) {
        SnapshotManager snapshotManager =
                new SnapshotManager(LocalFileIO.create(), getTableDirectory(tableName));
        Long id = snapshotManager.latestSnapshotId();
        return id == null ? null : id >= snapshotId ? snapshotManager.snapshot(snapshotId) : null;
    }

    protected String toWarehouse(String path) {
        return path;
    }

    protected List<Row> queryAndSort(String sql) {
        return sql(sql).stream()
                .sorted(Comparator.comparingInt(r -> r.getFieldAs(0)))
                .collect(Collectors.toList());
    }
}
