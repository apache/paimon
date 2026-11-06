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

package org.apache.paimon.globalindex;

import org.apache.paimon.globalindex.EsIndexSearch.Hit;
import org.apache.paimon.utils.JsonSerdeUtil;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/** Bounded HIT/PAGE_END records shared by Spark, Flink and the Java SDK. */
public final class EsSearchResult {
    private static final String[] COLUMNS = {
        "record_kind",
        "query_id",
        "query_ordinal",
        "execution_query_id",
        "target",
        "table_name",
        "branch",
        "snapshot_id",
        "row_id",
        "score",
        "rank",
        "sort_values_json",
        "source_schema_json",
        "source_binary",
        "source_encoding",
        "metadata_json",
        "index_fields_json"
    };

    private final List<Object[]> records;
    private final List<Hit> hits;

    EsSearchResult(EsSearchRequest request, EsIndexSearch.Plan plan, List<Hit> hits)
            throws Exception {
        this.hits = Collections.unmodifiableList(new ArrayList<>(hits));
        Map<Long, byte[]> source = request.fetchSource ? plan.fetch(hits) : Collections.emptyMap();
        String schema = request.fetchSource ? JsonSerdeUtil.toJson(plan.sourceType()) : null;
        String queryId = UUID.randomUUID().toString();
        ObjectNode metadata = JsonSerdeUtil.OBJECT_MAPPER_INSTANCE.createObjectNode();
        metadata.put("snapshotId", Long.toString(plan.snapshotId));
        metadata.put("indexType", "es-index");
        metadata.put("coverageComplete", true);
        metadata.put("scoring", "QUERY_THEN_FETCH");
        metadata.put("totalHitsTracked", false);
        metadata.put("hitCount", hits.size());
        records = new ArrayList<>();
        for (int i = 0; i <= hits.size(); i++) {
            boolean end = i == hits.size();
            Object[] row = new Object[COLUMNS.length];
            row[0] = end ? "PAGE_END" : "HIT";
            row[1] = queryId;
            row[2] = 0;
            row[3] = queryId;
            row[4] = request.target;
            row[5] = request.database + "." + request.table;
            row[6] = "main";
            row[7] = plan.snapshotId;
            if (!end) {
                Hit hit = hits.get(i);
                row[8] = hit.rowId;
                row[9] = hit.score;
                row[10] = (long) request.from + i + 1;
                row[12] = schema;
                row[13] = source.get(hit.rowId);
                row[14] = request.fetchSource ? "paimon-internal-row-v1" : null;
            }
            row[15] = metadata.toString();
            records.add(row);
        }
    }

    public List<Hit> hits() {
        return hits;
    }

    /** Ordered columns matching the Spark table-valued function schema. */
    public static String[] columns() {
        return COLUMNS.clone();
    }

    public List<Object[]> records() {
        List<Object[]> copy = new ArrayList<>();
        for (Object[] row : records) {
            Object[] record = row.clone();
            if (record[13] != null) {
                record[13] = ((byte[]) record[13]).clone();
            }
            copy.add(record);
        }
        return copy;
    }

    /** Flink procedure/SDK JSON records; source_binary is Base64, IDs remain JSON integers. */
    public String[] jsonRecords() {
        String[] json = new String[records.size()];
        for (int i = 0; i < json.length; i++) {
            Map<String, Object> row = new LinkedHashMap<>();
            for (int j = 0; j < COLUMNS.length; j++) {
                row.put(COLUMNS[j], records.get(i)[j]);
            }
            json[i] = JsonSerdeUtil.toJson(row);
        }
        return json;
    }
}
