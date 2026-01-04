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

import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.CatalogMaterializedTable;
import org.apache.flink.table.catalog.IntervalFreshness;
import org.apache.flink.table.catalog.ResolvedCatalogMaterializedTable;
import org.apache.flink.table.catalog.ResolvedSchema;

import java.util.Collections;
import java.util.Map;

/** Base class for Flink1 catalog tests. */
public class FlinkCatalogTestBase {

    private static final String DEFINITION_QUERY = "SELECT id, region, county FROM T";

    private static final IntervalFreshness FRESHNESS = IntervalFreshness.ofMinute("3");

    protected CatalogMaterializedTable createMaterializedTable(
            ResolvedSchema resolvedSchema, Map<String, String> options) {
        return new ResolvedCatalogMaterializedTable(
                CatalogMaterializedTable.newBuilder()
                        .schema(Schema.newBuilder().fromResolvedSchema(resolvedSchema).build())
                        .comment("test materialized table comment")
                        .partitionKeys(Collections.emptyList())
                        .options(options)
                        .definitionQuery(DEFINITION_QUERY)
                        .freshness(FRESHNESS)
                        .logicalRefreshMode(CatalogMaterializedTable.LogicalRefreshMode.AUTOMATIC)
                        .refreshMode(CatalogMaterializedTable.RefreshMode.CONTINUOUS)
                        .refreshStatus(CatalogMaterializedTable.RefreshStatus.INITIALIZING)
                        .build(),
                resolvedSchema);
    }
}
