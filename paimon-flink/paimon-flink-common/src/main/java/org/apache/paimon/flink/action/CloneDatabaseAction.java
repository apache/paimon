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

package org.apache.paimon.flink.action;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.clone.TableCloneTypeAction;
import org.apache.paimon.flink.FlinkCatalogFactory;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.utils.Pair;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.apache.paimon.utils.Preconditions.checkState;

/** Snapshot/Tag/Table clone action for Flink. */
public class CloneDatabaseAction extends AbstractCloneAction {

    private final List<Pair<FileStoreTable, FileStoreTable>> sourceAndTargetFileStoreTables;

    public CloneDatabaseAction(
            String sourceWarehouse,
            String sourceDatabase,
            Map<String, String> sourceCatalogConfig,
            String targetWarehouse,
            String targetDatabase,
            Map<String, String> targetCatalogConfig,
            String parallelismStr) {
        super(sourceWarehouse, sourceCatalogConfig, parallelismStr);
        this.sourceAndTargetFileStoreTables = new ArrayList<>();
        this.cloneTypeAction = new TableCloneTypeAction();

        initTargetTable(sourceDatabase, targetWarehouse, targetDatabase, targetCatalogConfig);
    }

    private void initTargetTable(
            String sourceDatabase,
            String targetWarehouse,
            String targetDatabase,
            Map<String, String> targetCatalogConfig) {
        Options targetTableCatalogOptions = Options.fromMap(targetCatalogConfig);
        targetTableCatalogOptions.set(CatalogOptions.WAREHOUSE, targetWarehouse);
        Catalog targetCatalog = FlinkCatalogFactory.createPaimonCatalog(targetTableCatalogOptions);
        checkState(
                !targetCatalog.databaseExists(targetDatabase),
                "Clone target database should not exist.");
        try {
            targetCatalog.createDatabase(targetDatabase, false);
            List<String> sourceTableNames = catalog.listTables(sourceDatabase);
            checkState(sourceTableNames.size() > 0, "Can not clone a empty database.");
            for (String sourceTableName : sourceTableNames) {
                FileStoreTable sourceTable =
                        checkTableAndCast(
                                catalog.getTable(new Identifier(sourceDatabase, sourceTableName)));
                targetCatalog.createTable(
                        new Identifier(targetDatabase, sourceTableName),
                        Schema.fromTableSchema(sourceTable.schema()),
                        false);
                FileStoreTable targetTable =
                        checkTableAndCast(
                                targetCatalog.getTable(
                                        new Identifier(targetDatabase, sourceTableName)));
                sourceAndTargetFileStoreTables.add(Pair.of(sourceTable, targetTable));
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    // ------------------------------------------------------------------------
    //  Java API
    // ------------------------------------------------------------------------

    @Override
    public void build() {
        checkFlinkRuntimeParameters();
        sourceAndTargetFileStoreTables.forEach(
                tablePair -> buildFlinkBatchJob(tablePair.getLeft(), tablePair.getRight()));
    }

    @Override
    public void run() throws Exception {
        build();
        execute("clone database job");
    }
}
