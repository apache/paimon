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

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.hive.HiveCatalog;
import org.apache.paimon.table.AbstractFileStoreTable;

import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.migrate.Importer;
import org.apache.migrate.hive.HiveImporter;

/** Test. */
public class TableMigrationUtils {

    public static Importer getImporter(
            CatalogBaseTable sourceFlinkTable,
            Catalog paimonCatalog,
            String sourceDatabase,
            String souceTableName,
            AbstractFileStoreTable targetPaimonTable) {
        String connector = sourceFlinkTable.getOptions().get("connector");
        switch (connector) {
            case "hive":
                {
                    HiveCatalog paimonHiveCatalog = (HiveCatalog) paimonCatalog;
                    FileIO fileIO = paimonHiveCatalog.fileIO();
                    IMetaStoreClient client = paimonHiveCatalog.getHmsClient();
                    return new HiveImporter(
                            fileIO, client, sourceDatabase, souceTableName, targetPaimonTable);
                }
            default:
                throw new UnsupportedOperationException("Don't support connector " + connector);
        }
    }
}
