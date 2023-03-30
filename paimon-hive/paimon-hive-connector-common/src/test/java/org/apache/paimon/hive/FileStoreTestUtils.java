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

package org.apache.paimon.hive;

import org.apache.paimon.FileStore;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.Table;
import org.apache.paimon.types.RowType;

import java.util.List;

/** Test utils related to {@link FileStore}. */
public class FileStoreTestUtils {

    private static final String TABLE_NAME = "hive_test_table";

    private static final String DATABASE_NAME = "default";

    private static final Identifier TABLE_IDENTIFIER = Identifier.create(DATABASE_NAME, TABLE_NAME);

    public static Table createFileStoreTable(
            Options conf, RowType rowType, List<String> partitionKeys, List<String> primaryKeys)
            throws Exception {
        // create CatalogContext using the options
        CatalogContext catalogContext = CatalogContext.create(conf);
        Catalog catalog = CatalogFactory.createCatalog(catalogContext);
        // create database
        catalog.createDatabase(DATABASE_NAME, false);
        // create table
        catalog.createTable(
                TABLE_IDENTIFIER,
                new Schema(rowType.getFields(), partitionKeys, primaryKeys, conf.toMap(), ""),
                false);
        Table table = catalog.getTable(TABLE_IDENTIFIER);
        catalog.close();
        return table;
    }
}
