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

package org.apache.paimon.spark.utils;

import org.apache.paimon.catalog.CachingCatalog;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.hive.HiveCatalog;
import org.apache.paimon.hive.migrate.HiveMigrator;
import org.apache.paimon.migrate.Migrator;

import java.util.Map;

/** Migration util to choose importer according to connector. */
public class TableMigrationUtils {

    public static Migrator getImporter(
            String connector,
            Catalog catalog,
            String sourceDatabase,
            String sourceTableName,
            String targetDatabase,
            String targetTableName,
            Map<String, String> options) {
        switch (connector) {
            case "hive":
                if (catalog instanceof CachingCatalog) {
                    catalog = ((CachingCatalog) catalog).wrapped();
                }
                if (!(catalog instanceof HiveCatalog)) {
                    throw new IllegalArgumentException("Only support Hive Catalog");
                }
                return new HiveMigrator(
                        (HiveCatalog) catalog,
                        sourceDatabase,
                        sourceTableName,
                        targetDatabase,
                        targetTableName,
                        options);
            default:
                throw new UnsupportedOperationException("Unsupported connector " + connector);
        }
    }
}
