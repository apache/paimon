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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link MaterializedTableUtils}. This module has its own copy of the class, which
 * replaces the one of paimon-flink2-common in the bundle, and the tests run it against the Flink
 * version of this module.
 */
public class MaterializedTableUtilsTest {

    private static final String DEFINITION_QUERY = "SELECT id, region FROM T";

    @ParameterizedTest
    @EnumSource(IntervalFreshness.TimeUnit.class)
    void testFreshnessTimeUnit(IntervalFreshness.TimeUnit timeUnit) {
        IntervalFreshness freshness = IntervalFreshness.of("3", timeUnit);

        assertThat(MaterializedTableUtils.freshnessTimeUnit(freshness)).isEqualTo(timeUnit.name());
        assertThat(MaterializedTableUtils.intervalFreshness("3", timeUnit.name()))
                .isEqualTo(freshness);
    }

    @Test
    void testDefinitionQuery() {
        CatalogMaterializedTable table =
                MaterializedTableUtils.definitionQuery(
                                CatalogMaterializedTable.newBuilder(), DEFINITION_QUERY)
                        .schema(Schema.newBuilder().column("id", DataTypes.INT()).build())
                        .freshness(MaterializedTableUtils.intervalFreshness("3", "MINUTE"))
                        .logicalRefreshMode(CatalogMaterializedTable.LogicalRefreshMode.AUTOMATIC)
                        .refreshMode(CatalogMaterializedTable.RefreshMode.CONTINUOUS)
                        .refreshStatus(CatalogMaterializedTable.RefreshStatus.INITIALIZING)
                        .build();

        assertThat(table.getDefinitionQuery()).isEqualTo(DEFINITION_QUERY);
        assertThat(table.getDefinitionFreshness()).isEqualTo(IntervalFreshness.ofMinute("3"));
    }
}
