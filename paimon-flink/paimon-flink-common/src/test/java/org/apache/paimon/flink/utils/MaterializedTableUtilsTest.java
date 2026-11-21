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

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.CatalogMaterializedTable;
import org.apache.flink.table.catalog.IntervalFreshness;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for the {@link MaterializedTableUtils} of the Flink profile that this module is built with.
 * They only use calls that Flink 1.20 and Flink 2.x have. The connector modules of Flink 2.x test
 * the class of their bundle against their own Flink version.
 */
public class MaterializedTableUtilsTest {

    private static final String DEFINITION_QUERY = "SELECT id, region FROM T";

    @Test
    void testStoredFreshnessTimeUnits() {
        assertFreshness(IntervalFreshness.ofSecond("5"), "SECOND");
        assertFreshness(IntervalFreshness.ofMinute("5"), "MINUTE");
        assertFreshness(IntervalFreshness.ofHour("5"), "HOUR");
        assertFreshness(IntervalFreshness.ofDay("5"), "DAY");
    }

    @Test
    void testDefinitionQuery() {
        CatalogMaterializedTable table =
                MaterializedTableUtils.definitionQuery(
                                CatalogMaterializedTable.newBuilder(), DEFINITION_QUERY)
                        .schema(Schema.newBuilder().column("id", DataTypes.INT()).build())
                        .freshness(IntervalFreshness.ofMinute("3"))
                        .logicalRefreshMode(CatalogMaterializedTable.LogicalRefreshMode.AUTOMATIC)
                        .refreshMode(CatalogMaterializedTable.RefreshMode.CONTINUOUS)
                        .refreshStatus(CatalogMaterializedTable.RefreshStatus.INITIALIZING)
                        .build();

        assertThat(table.getDefinitionQuery()).isEqualTo(DEFINITION_QUERY);
    }

    private static void assertFreshness(IntervalFreshness freshness, String timeUnit) {
        assertThat(MaterializedTableUtils.freshnessTimeUnit(freshness)).isEqualTo(timeUnit);
        assertThat(MaterializedTableUtils.intervalFreshness("5", timeUnit)).isEqualTo(freshness);
    }
}
