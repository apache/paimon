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

import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.options.Options;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogMaterializedTable;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.IntervalFreshness;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.ResolvedCatalogMaterializedTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.nio.file.Path;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests that {@link FlinkCatalog} stores and loads a materialized table on the Flink version of
 * this module. The catalog is compiled against a newer Flink version and reaches the classes that
 * changed since then through MaterializedTableUtils, which this module replaces with its own copy.
 */
public class FlinkCatalogMaterializedTableTest {

    private static final String DEFINITION_QUERY = "SELECT id FROM T";

    @TempDir Path warehouse;

    @ParameterizedTest
    @EnumSource(IntervalFreshness.TimeUnit.class)
    void testCreateAndGetMaterializedTable(IntervalFreshness.TimeUnit timeUnit) throws Exception {
        Options options = new Options();
        options.setString("warehouse", warehouse.toString());
        Catalog catalog =
                FlinkCatalogFactory.createCatalog(
                        "test-catalog",
                        CatalogContext.create(options),
                        FlinkCatalogMaterializedTableTest.class.getClassLoader());
        ObjectPath tablePath = new ObjectPath("db1", "mt");
        ResolvedSchema schema = ResolvedSchema.of(Column.physical("id", DataTypes.INT()));
        IntervalFreshness freshness = IntervalFreshness.of("5", timeUnit);
        CatalogMaterializedTable table =
                CatalogMaterializedTable.newBuilder()
                        .schema(Schema.newBuilder().fromResolvedSchema(schema).build())
                        .comment("")
                        .partitionKeys(Collections.emptyList())
                        .options(Collections.emptyMap())
                        .definitionQuery(DEFINITION_QUERY)
                        .freshness(freshness)
                        .logicalRefreshMode(CatalogMaterializedTable.LogicalRefreshMode.AUTOMATIC)
                        .refreshMode(CatalogMaterializedTable.RefreshMode.CONTINUOUS)
                        .refreshStatus(CatalogMaterializedTable.RefreshStatus.INITIALIZING)
                        .build();
        catalog.createDatabase(tablePath.getDatabaseName(), null, false);
        catalog.createTable(tablePath, new ResolvedCatalogMaterializedTable(table, schema), false);

        CatalogMaterializedTable actual = (CatalogMaterializedTable) catalog.getTable(tablePath);
        assertThat(actual.getDefinitionQuery()).isEqualTo(DEFINITION_QUERY);
        assertThat(actual.getDefinitionFreshness()).isEqualTo(freshness);
    }
}
