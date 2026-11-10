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

package org.apache.paimon.flink.procedure;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.flink.CatalogITCaseBase;
import org.apache.paimon.fs.Path;
import org.apache.paimon.globalindex.EsIndexSearch;
import org.apache.paimon.globalindex.EsSearchResult;
import org.apache.paimon.utils.JsonSerdeUtil;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.JsonNode;

import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * SQL procedure acceptance against real Java archives, including remote shard work and SDK parity.
 */
public class PaimonEsSearchProcedureITCase extends CatalogITCaseBase {
    private void fixture() {
        setParallelism(1);
        sql(
                "CREATE TABLE T (id INT, vec ARRAY<FLOAT>, title STRING, tag STRING) WITH ("
                        + "'bucket'='-1', 'row-tracking.enabled'='true', 'data-evolution.enabled'='true',"
                        + "'global-index.row-count-per-shard'='2', 'global-index.thread-num'='2',"
                        + "'global-index.es-index.fields.vec.algorithm'='hnsw',"
                        + "'global-index.es-index.fields.vec.dimension'='2', 'global-index.es-index.fields.vec.metric'='l2',"
                        + "'global-index.es-index.fields.tag.type'='keyword')");
        sql(
                "INSERT INTO T VALUES (0, ARRAY[CAST(0 AS FLOAT),CAST(0 AS FLOAT)], 'blue lake','even'),"
                        + "(1, ARRAY[CAST(1 AS FLOAT),CAST(0 AS FLOAT)], 'green lake','odd'),"
                        + "(2, ARRAY[CAST(2 AS FLOAT),CAST(0 AS FLOAT)], 'blue sky','even'),"
                        + "(3, ARRAY[CAST(3 AS FLOAT),CAST(0 AS FLOAT)], 'green sky','odd')");
        setParallelism(2);
        sql("CALL sys.create_global_index('default.T','vec,title,tag,id','es-index')");
        assertThat(sql("SELECT * FROM `T$table_indexes` WHERE index_type='es-index'").size())
                .isGreaterThanOrEqualTo(2);
    }

    private String envelope(String query, int from, int size, boolean fetch) {
        return "{\"version\":1,\"operation\":\"SEARCH\",\"targets\":[{\"name\":\"docs\","
                + "\"database\":\"default\",\"table\":\"T\",\"indexes\":[[1,2,3,0]],"
                + "\"bindings\":{\"v\":1,\"text\":2,\"label\":3,\"number\":0}}],"
                + "\"request\":{\"query\":"
                + query
                + ",\"from\":"
                + from
                + ",\"size\":"
                + size
                + ",\"fetch\":\""
                + (fetch ? "SOURCE" : "NONE")
                + "\"}}";
    }

    private List<Long> search(String json) throws Exception {
        List<JsonNode> records = new ArrayList<>();
        for (Row row : sql("CALL sys.paimon_es_search('" + json + "')")) {
            records.add(JsonSerdeUtil.OBJECT_MAPPER_INSTANCE.readTree(row.getField(0).toString()));
        }
        assertThat(
                        records.stream()
                                .filter(row -> row.get("record_kind").asText().equals("PAGE_END"))
                                .count())
                .isEqualTo(1);
        try (Catalog catalog =
                CatalogFactory.createCatalog(CatalogContext.create(new Path(toWarehouse(path))))) {
            EsSearchResult sdk = EsIndexSearch.search(catalog, json);
            List<JsonNode> expected = new ArrayList<>();
            for (String row : sdk.jsonRecords()) {
                expected.add(JsonSerdeUtil.OBJECT_MAPPER_INSTANCE.readTree(row));
            }
            for (List<JsonNode> values : Arrays.asList(records, expected)) {
                for (JsonNode value : values) {
                    ((org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.node
                                            .ObjectNode)
                                    value)
                            .remove(Arrays.asList("query_id", "execution_query_id"));
                }
            }
            assertThat(records).containsExactlyElementsOf(expected);
        }
        java.util.Map<Long, Long> sourceIds =
                sql("SELECT _ROW_ID,id FROM `T$row_tracking`").stream()
                        .collect(
                                Collectors.toMap(
                                        row -> (Long) row.getField(0),
                                        row -> ((Integer) row.getField(1)).longValue()));
        List<Long> result = new ArrayList<>();
        for (JsonNode record : records) {
            if (!record.get("record_kind").asText().equals("HIT")) {
                continue;
            }
            long rowId = record.get("row_id").longValue();
            assertThat(sourceIds).containsKey(rowId);
            if (!record.get("source_binary").isNull()) {
                org.apache.paimon.types.RowType schema =
                        JsonSerdeUtil.fromJson(
                                record.get("source_schema_json").asText(),
                                org.apache.paimon.types.RowType.class);
                org.apache.paimon.data.InternalRow row =
                        new org.apache.paimon.data.serializer.InternalRowSerializer(schema)
                                .deserialize(
                                        new org.apache.paimon.io.DataInputDeserializer(
                                                record.get("source_binary").binaryValue()));
                assertThat((long) row.getInt(0)).isEqualTo(sourceIds.get(rowId));
                result.add(sourceIds.get(rowId));
            } else {
                result.add(rowId);
            }
        }
        return result;
    }

    @Test
    void textFiltersPaginationAndSource() throws Exception {
        fixture();
        assertThat(search(envelope("{\"match\":{\"text\":\"blue\"}}", 0, 4, true)))
                .containsExactlyInAnyOrder(0L, 2L);
        assertThat(
                        search(
                                envelope(
                                        "{\"bool\":{\"filter\":[{\"term\":{\"label\":\"odd\"}},{\"range\":{\"number\":{\"gt\":1}}}]}}",
                                        0,
                                        4,
                                        true)))
                .containsExactly(3L);
        assertThat(search(envelope("{\"match_all\":{}}", 1, 2, false))).containsExactly(1L, 2L);
        assertThat(search(envelope("{\"match_none\":{}}", 0, 4, true))).isEmpty();
    }

    @Test
    void vectorAndFilterKnn() throws Exception {
        fixture();
        assertThat(
                        search(
                                envelope(
                                        "{\"knn\":{\"field\":\"v\",\"query_vector\":[0,0],\"k\":2,\"num_candidates\":4}}",
                                        0,
                                        2,
                                        true)))
                .containsExactly(0L, 1L);
        assertThat(
                        search(
                                envelope(
                                        "{\"knn\":{\"field\":\"v\",\"query_vector\":[0,0],\"k\":2,\"num_candidates\":4,\"filter\":{\"term\":{\"label\":\"odd\"}}}}",
                                        0,
                                        2,
                                        true)))
                .containsExactly(1L, 3L);
        sql("INSERT INTO T VALUES (4, ARRAY[CAST(4 AS FLOAT),CAST(0 AS FLOAT)],'new lake','even')");
        assertThatThrownBy(() -> search(envelope("{\"match_all\":{}}", 0, 4, false)))
                .hasStackTraceContaining("does not exactly cover");
    }
}
