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

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;

import org.apache.paimon.shade.guava30.com.google.common.collect.Lists;
import org.apache.paimon.shade.guava30.com.google.common.collect.Maps;

import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Table;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.Collections;
import java.util.UUID;

import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.METASTORECONNECTURLKEY;
import static org.assertj.core.api.Assertions.assertThat;

/** Verify that table stats has been updated. */
public class HiveTableStatsTest {
    @TempDir java.nio.file.Path tempFile;
    protected Catalog catalog;

    @BeforeEach
    public void setUp() throws Exception {
        String warehouse = tempFile.toUri().toString();
        HiveConf hiveConf = new HiveConf();
        String jdoConnectionURL = "jdbc:derby:memory:" + UUID.randomUUID();
        hiveConf.setVar(METASTORECONNECTURLKEY, jdoConnectionURL + ";create=true");
        String metastoreClientClass = "org.apache.hadoop.hive.metastore.HiveMetaStoreClient";
        Options catalogOptions = new Options();
        catalogOptions.set(StatsSetupConst.DO_NOT_UPDATE_STATS, "true");
        catalogOptions.set(CatalogOptions.WAREHOUSE, warehouse);
        CatalogContext catalogContext = CatalogContext.create(catalogOptions);
        FileIO fileIO = FileIO.get(new Path(warehouse), catalogContext);
        catalog =
                new HiveCatalog(fileIO, hiveConf, metastoreClientClass, catalogOptions, warehouse);
    }

    @Test
    public void testAlterTable() throws Exception {
        catalog.createDatabase("test_db", false);
        // Alter table adds a new column to an existing table,but do not update stats
        Identifier identifier = Identifier.create("test_db", "test_table");
        catalog.createTable(
                identifier,
                new Schema(
                        Lists.newArrayList(new DataField(0, "col1", DataTypes.STRING())),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        Maps.newHashMap(),
                        ""),
                false);
        catalog.alterTable(
                identifier,
                Lists.newArrayList(
                        SchemaChange.addColumn("col2", DataTypes.DATE()),
                        SchemaChange.addColumn("col3", DataTypes.STRING(), "col3 field")),
                false);
        HiveCatalog hiveCatalog = (HiveCatalog) catalog;
        Table table = hiveCatalog.getHmsTable(identifier);
        assertThat(table.getParameters().get("COLUMN_STATS_ACCURATE")).isEqualTo(null);
    }
}
