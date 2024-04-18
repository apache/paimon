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
import org.apache.paimon.clone.CloneType;
import org.apache.paimon.flink.FlinkCatalogFactory;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;

import java.util.Map;

import static org.apache.paimon.utils.Preconditions.checkState;

/** Snapshot/Tag/Table clone action for Flink. */
public class CloneAction extends AbstractCloneAction {

    private final FileStoreTable sourceFileStoreTable;
    private FileStoreTable targetFileStoreTable;

    public CloneAction(
            String sourceWarehouse,
            String sourceDatabase,
            String sourceTableName,
            Map<String, String> sourceCatalogConfig,
            String targetWarehouse,
            String targetDatabase,
            String targetTableName,
            Map<String, String> targetCatalogConfig,
            String parallelismStr,
            CloneType cloneType,
            Long snapshotId,
            String tagName,
            Long timestamp) {
        super(
                sourceWarehouse,
                sourceDatabase,
                sourceTableName,
                sourceCatalogConfig,
                parallelismStr);
        this.sourceFileStoreTable = checkTableAndCast(table);
        this.cloneTypeAction = cloneType.generateCloneTypeAction(snapshotId, tagName, timestamp);

        initTargetTable(targetWarehouse, targetDatabase, targetTableName, targetCatalogConfig);
    }

    private void initTargetTable(
            String targetWarehouse,
            String targetDatabase,
            String targetTableName,
            Map<String, String> targetCatalogConfig) {
        Options targetTableCatalogOptions = Options.fromMap(targetCatalogConfig);
        targetTableCatalogOptions.set(CatalogOptions.WAREHOUSE, targetWarehouse);
        Catalog targetCatalog = FlinkCatalogFactory.createPaimonCatalog(targetTableCatalogOptions);
        try {
            if (!targetCatalog.databaseExists(targetDatabase)) {
                targetCatalog.createDatabase(targetDatabase, true);
            }
            Identifier targetIdentifier = new Identifier(targetDatabase, targetTableName);
            Table targetTable;
            checkState(
                    !targetCatalog.tableExists(targetIdentifier),
                    "Clone target table should not exist.");

            targetCatalog.createTable(
                    targetIdentifier, Schema.fromTableSchema(sourceFileStoreTable.schema()), false);
            targetTable = targetCatalog.getTable(targetIdentifier);
            this.targetFileStoreTable = checkTableAndCast(targetTable);
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
        buildFlinkBatchJob(sourceFileStoreTable, targetFileStoreTable);
    }

    @Override
    public void run() throws Exception {
        build();
        execute("clone snapshot/tag/table job");
    }
}
