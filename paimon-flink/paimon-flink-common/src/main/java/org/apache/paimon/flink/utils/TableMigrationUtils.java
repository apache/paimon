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

package org.apache.paimon.flink.utils;

import org.apache.paimon.catalog.CachingCatalog;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.fs.Path;
import org.apache.paimon.hive.HiveCatalog;
import org.apache.paimon.hive.migrate.HiveMigrator;
import org.apache.paimon.iceberg.IcebergMigrator;
import org.apache.paimon.migrate.Migrator;
import org.apache.paimon.options.Options;
import org.apache.paimon.utils.Preconditions;

import java.util.List;
import java.util.Map;

/** Migration util to choose importer according to connector. */
public class TableMigrationUtils {

    public static Migrator getImporter(
            String connector,
            Catalog catalog,
            String sourceIdentifier,
            String targetDatabase,
            String targetTableName,
            Integer parallelism,
            Map<String, String> options) {
        switch (connector) {
            case "hive":
                Identifier identifier = Identifier.fromString(sourceIdentifier);
                if (catalog instanceof CachingCatalog) {
                    catalog = ((CachingCatalog) catalog).wrapped();
                }
                if (!(catalog instanceof HiveCatalog)) {
                    throw new IllegalArgumentException("Only support Hive Catalog");
                }
                return new HiveMigrator(
                        (HiveCatalog) catalog,
                        identifier.getDatabaseName(),
                        identifier.getTableName(),
                        targetDatabase,
                        targetTableName,
                        parallelism,
                        options);
            default:
                throw new UnsupportedOperationException("Don't support connector " + connector);
        }
    }

    public static Migrator getIcebergImporter(
            Catalog catalog,
            Integer parallelism,
            Map<String, String> options,
            Map<String, String> icebergConf) {
        checkIcebergRequiredConf(icebergConf);
        return new IcebergMigrator(
                catalog,
                new Path(icebergConf.get("iceberg-meta-path")),
                icebergConf.get("target-database"),
                icebergConf.get("target-table"),
                new Options(icebergConf).getBoolean("ignore-delete-file", false),
                parallelism);
    }

    public static List<Migrator> getImporters(
            String connector,
            Catalog catalog,
            String sourceDatabase,
            Integer parallelism,
            Map<String, String> options) {
        switch (connector) {
            case "hive":
                if (catalog instanceof CachingCatalog) {
                    catalog = ((CachingCatalog) catalog).wrapped();
                }
                if (!(catalog instanceof HiveCatalog)) {
                    throw new IllegalArgumentException("Only support Hive Catalog");
                }
                return HiveMigrator.databaseMigrators(
                        (HiveCatalog) catalog, sourceDatabase, options, parallelism);
            default:
                throw new UnsupportedOperationException("Don't support connector " + connector);
        }
    }

    private static void checkIcebergRequiredConf(Map<String, String> icebergConf) {
        Preconditions.checkArgument(
                icebergConf.containsKey("iceberg-meta-path"),
                "please set required iceberg argument 'iceberg-meta-path'.");
        Preconditions.checkArgument(
                icebergConf.containsKey("target-database"),
                "please set required iceberg argument 'target-database'.");
        Preconditions.checkArgument(
                icebergConf.containsKey("target-table"),
                "please set required iceberg argument 'target-table'.");
    }
}
