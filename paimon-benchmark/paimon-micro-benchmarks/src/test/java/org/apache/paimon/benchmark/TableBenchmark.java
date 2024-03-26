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

package org.apache.paimon.benchmark;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.Table;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.IntType;

import org.apache.commons.math3.random.RandomDataGenerator;
import org.junit.jupiter.api.io.TempDir;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Base class for table benchmark. */
public class TableBenchmark {

    private static final int VALUE_COUNT = 20;

    @TempDir java.nio.file.Path tempFile;

    private final RandomDataGenerator random = new RandomDataGenerator();

    protected Table createTable(Options tableOptions, String tableName) throws Exception {
        return createTable(tableOptions, tableName, Collections.singletonList("k"));
    }

    protected Table createTable(Options tableOptions, String tableName, List<String> primaryKeys)
            throws Exception {
        Options catalogOptions = new Options();
        catalogOptions.set(CatalogOptions.WAREHOUSE, tempFile.toUri().toString());
        Catalog catalog = CatalogFactory.createCatalog(CatalogContext.create(catalogOptions));
        String database = "default";
        catalog.createDatabase(database, true);

        List<DataField> fields = new ArrayList<>();
        fields.add(new DataField(0, "k", new IntType()));
        for (int i = 1; i <= VALUE_COUNT; i++) {
            fields.add(new DataField(i, "f" + i, DataTypes.STRING()));
        }
        tableOptions.set(CoreOptions.SNAPSHOT_NUM_RETAINED_MAX, 10);
        Schema schema =
                new Schema(fields, Collections.emptyList(), primaryKeys, tableOptions.toMap(), "");
        Identifier identifier = Identifier.create(database, tableName);
        catalog.createTable(identifier, schema, false);
        return catalog.getTable(identifier);
    }

    protected InternalRow newRandomRow() {
        GenericRow row = new GenericRow(1 + VALUE_COUNT);
        row.setField(0, random.nextInt(0, Integer.MAX_VALUE));
        for (int i = 1; i <= VALUE_COUNT; i++) {
            row.setField(i, BinaryString.fromString(random.nextHexString(10)));
        }
        return row;
    }
}
