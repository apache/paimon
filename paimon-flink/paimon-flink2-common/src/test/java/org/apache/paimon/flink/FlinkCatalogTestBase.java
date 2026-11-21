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

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogMaterializedTable;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.Interval;
import org.apache.flink.table.catalog.IntervalFreshness;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.ResolvedCatalogMaterializedTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.StartMode;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.Collections;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Base class for Flink2 catalog tests. */
public abstract class FlinkCatalogTestBase {

    private static final String DEFINITION_QUERY = "SELECT id, region, county FROM T";

    private static final IntervalFreshness FRESHNESS = IntervalFreshness.ofMinute("3");

    /** The catalog that the tests of this class run against. */
    protected abstract Catalog catalog();

    protected CatalogMaterializedTable createMaterializedTable(
            ResolvedSchema resolvedSchema, Map<String, String> options) {
        return createMaterializedTable(resolvedSchema, options, FRESHNESS);
    }

    private CatalogMaterializedTable createMaterializedTable(
            ResolvedSchema resolvedSchema,
            Map<String, String> options,
            IntervalFreshness freshness) {
        return new ResolvedCatalogMaterializedTable(
                CatalogMaterializedTable.newBuilder()
                        .schema(Schema.newBuilder().fromResolvedSchema(resolvedSchema).build())
                        .comment("test materialized table comment")
                        .partitionKeys(Collections.emptyList())
                        .options(options)
                        .originalQuery(DEFINITION_QUERY)
                        .expandedQuery(DEFINITION_QUERY)
                        .freshness(freshness)
                        .logicalRefreshMode(CatalogMaterializedTable.LogicalRefreshMode.AUTOMATIC)
                        .refreshMode(CatalogMaterializedTable.RefreshMode.CONTINUOUS)
                        .refreshStatus(CatalogMaterializedTable.RefreshStatus.INITIALIZING)
                        .build(),
                resolvedSchema,
                CatalogMaterializedTable.RefreshMode.CONTINUOUS,
                freshness,
                StartMode.of(StartMode.StartModeKind.FROM_BEGINNING));
    }

    /**
     * Since Flink 2.3 a freshness has more time units than the four that Paimon stores, and the
     * planner hands WEEK to the catalog for a continuous materialized table. Flink 1.x has no such
     * unit, so this test is here and not in FlinkCatalogTest.
     */
    @ParameterizedTest
    @EnumSource(
            value = Interval.TimeUnit.class,
            mode = EnumSource.Mode.EXCLUDE,
            names = {"SECOND", "MINUTE", "HOUR", "DAY"})
    public void testCreateMaterializedTableWithUnsupportedFreshnessTimeUnit(
            Interval.TimeUnit timeUnit) throws Exception {
        ObjectPath tablePath = new ObjectPath("db1", "mt");
        CatalogMaterializedTable materializedTable =
                createMaterializedTable(
                        ResolvedSchema.of(Column.physical("id", DataTypes.INT())),
                        Collections.emptyMap(),
                        IntervalFreshness.of("1", timeUnit));
        catalog().createDatabase(tablePath.getDatabaseName(), null, false);

        assertThatThrownBy(() -> catalog().createTable(tablePath, materializedTable, false))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessage(
                        "Materialized table freshness in "
                                + timeUnit
                                + " is not supported, use SECOND, MINUTE, HOUR or DAY.");
        assertThat(catalog().tableExists(tablePath)).isFalse();
    }
}
