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

import org.apache.paimon.clone.CloneType;

import org.apache.flink.api.java.tuple.Tuple3;

import java.util.Map;
import java.util.Optional;

import static org.apache.paimon.utils.Preconditions.checkNotNull;

/** Factory to create {@link CloneAction}. */
public class CloneActionFactory implements ActionFactory {

    private static final String IDENTIFIER = "clone";
    private static final String PARALLELISM = "parallelism";
    private static final String CLONE_TYPE = "clone_type";
    private static final String SNAPSHOT_ID = "snapshot_id";
    private static final String TAG_NAME = "tag_name";
    private static final String TIMESTAMP = "timestamp";
    private static final String TARGET_WAREHOUSE = "target_warehouse";
    private static final String TARGET_DATABASE = "target_database";
    private static final String TARGET_TABLE = "target_table";
    private static final String TARGET_PATH = "target_path";
    private static final String TARGET_CATALOG_CONF = "target_catalog_conf";

    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    @Override
    public Optional<Action> create(MultipleParameterToolAdapter params) {
        Tuple3<String, String, String> sourceTablePath = getTablePath(params);
        Map<String, String> catalogConfig = optionalConfigMap(params, CATALOG_CONF);

        String targetWarehouse = params.get(TARGET_WAREHOUSE);
        String targetDatabase = params.get(TARGET_DATABASE);
        String targetTable = params.get(TARGET_TABLE);
        String targetPath = params.get(TARGET_PATH);
        Tuple3<String, String, String> targetTablePath =
                getTablePath(targetWarehouse, targetDatabase, targetTable, targetPath);
        Map<String, String> targetCatalogConfig = optionalConfigMap(params, TARGET_CATALOG_CONF);

        String cloneTypeStr = params.get(CLONE_TYPE);
        checkNotNull(cloneTypeStr, "clone_type should not be null.");
        CloneType cloneType = CloneType.valueOf(cloneTypeStr);

        long snapshotId = 0;
        String tagName = null;
        long timestamp = 0;
        String parallelismStr = params.get(PARALLELISM);
        switch (cloneType) {
            case SpecificSnapshot:
                String snapshotIdStr = params.get(SNAPSHOT_ID);
                checkNotNull(
                        snapshotIdStr,
                        "snapshot_id should not be null when clone_type is SPECIFIC_SNAPSHOT.");
                snapshotId = Long.parseLong(snapshotIdStr);
                break;

            case FromTimestamp:
                String timestampStr = params.get(TIMESTAMP);
                checkNotNull(
                        timestampStr,
                        "timestamp should not be null when clone_type is FROM_TIMESTAMP.");
                timestamp = Long.parseLong(timestampStr);
                break;

            case Tag:
                tagName = params.get(TAG_NAME);
                checkNotNull(tagName, "tag_name should not be null when clone_type is TAG.");
                break;

            case LatestSnapshot:
            case Table:
                break;
            default:
                throw new UnsupportedOperationException("Unknown cloneType : " + cloneType);
        }

        CloneAction cloneAction =
                new CloneAction(
                        sourceTablePath.f0,
                        sourceTablePath.f1,
                        sourceTablePath.f2,
                        catalogConfig,
                        targetTablePath.f0,
                        targetTablePath.f1,
                        targetTablePath.f2,
                        targetCatalogConfig,
                        parallelismStr,
                        cloneType,
                        snapshotId,
                        tagName,
                        timestamp);

        return Optional.of(cloneAction);
    }

    @Override
    public void printHelp() {
        System.out.println(
                "Action \"clone\" runs a batch job for clone specified Snapshot/Tag/Table.");
        System.out.println();

        System.out.println("Syntax:");
        System.out.println(
                "  clone --warehouse <warehouse_path>"
                        + "--database <database_name> "
                        + "--table <table_name> "
                        + "[--catalog_conf <source-paimon-catalog-conf> [--catalog_conf <source-paimon-catalog-conf> ...]]"
                        + "--target_warehouse <target_warehouse_path>"
                        + "--target_database <target_database_name> "
                        + "--target_table <target_table_name> "
                        + "[--target_catalog_conf <target-paimon-catalog-conf> [--target_catalog_conf <target-paimon-catalog-conf> ...]]"
                        + "--clone_type LatestSnapshot");

        System.out.println();
        System.out.println(
                "clone_type support : LatestSnapshot, SpecificSnapshot, FromTimestamp, Tag, Table.");

        System.out.println("Examples:");
        System.out.println(
                "  clone --warehouse hdfs:///path/from/warehouse --database test_db --table test_table"
                        + " --target_warehouse hdfs:///path/to/warehouse --target_database test_db"
                        + " --target_table test_table_copy --clone_type LatestSnapshot");
        System.out.println(
                "  clone --warehouse hdfs:///path/from/warehouse --database test_db --table test_table"
                        + " --target_warehouse hdfs:///path/to/warehouse --target_database test_db"
                        + " --target_table test_table_copy --clone_type SPECIFIC_SNAPSHOT --clone_snapshot_id 10");
        System.out.println(
                "  clone --warehouse hdfs:///path/from/warehouse --database test_db --table test_table"
                        + " --target_warehouse hdfs:///path/to/warehouse --target_database test_db"
                        + " --target_table test_table_copy --clone_type FROM_TIMESTAMP --clone_timestamp 1672599841000");
        System.out.println(
                "  clone --warehouse hdfs:///path/from/warehouse --database test_db --table test_table"
                        + " --target_warehouse hdfs:///path/to/warehouse --target_database test_db"
                        + " --target_table test_table_copy --clone_type Tag --tag_name 20240318");
        System.out.println(
                "  clone --warehouse s3:///path1/from/warehouse "
                        + "--database test_db "
                        + "--table test_table "
                        + "--catalog_conf s3.endpoint=https://****.com "
                        + "--catalog_conf s3.access-key=***** "
                        + "--catalog_conf s3.secret-key=***** "
                        + "--target_warehouse s3:///path2/to/warehouse "
                        + "--target_database test_db_copy "
                        + "--target_table test_table_copy "
                        + "--target_catalog_conf s3.endpoint=https://****.com "
                        + "--target_catalog_conf s3.access-key=***** "
                        + "--target_catalog_conf s3.access-key=***** "
                        + "--clone_type Table");
    }
}
