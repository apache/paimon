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

import org.apache.flink.table.catalog.CatalogMaterializedTable;
import org.apache.flink.table.catalog.Interval;
import org.apache.flink.table.catalog.IntervalFreshness;

/** Utility methods about Flink {@link CatalogMaterializedTable} to resolve compatibility issues. */
public class MaterializedTableUtils {

    public static String freshnessTimeUnit(IntervalFreshness freshness) {
        return freshness.getTimeUnit().name();
    }

    /** Since Flink 2.3 the time unit of a freshness is {@link Interval.TimeUnit}. */
    public static IntervalFreshness intervalFreshness(String interval, String timeUnit) {
        return IntervalFreshness.of(interval, Interval.TimeUnit.valueOf(timeUnit));
    }

    /**
     * Since Flink 2.3 a materialized table keeps the original and the expanded query, and both are
     * required. Paimon stores one definition query, so it is used for both.
     */
    public static CatalogMaterializedTable.Builder definitionQuery(
            CatalogMaterializedTable.Builder builder, String definitionQuery) {
        return builder.originalQuery(definitionQuery).expandedQuery(definitionQuery);
    }
}
