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

package org.apache.paimon.flink.action.cdc.mongodb.strategy;

import org.apache.paimon.flink.sink.cdc.CdcRecord;
import org.apache.paimon.flink.sink.cdc.RichCdcMultiplexRecord;
import org.apache.paimon.flink.sink.cdc.RichCdcRecord;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.utils.JsonSerdeUtil;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.flink.configuration.Configuration;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.paimon.flink.action.cdc.mongodb.MongoDBActionUtils.FIELD_NAME;
import static org.apache.paimon.flink.action.cdc.mongodb.MongoDBActionUtils.PARSER_PATH;
import static org.apache.paimon.flink.action.cdc.mongodb.MongoDBActionUtils.START_MODE;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link Mongo4VersionStrategy}. */
class Mongo4VersionStrategyTest {

    private static final String ID = "{\"_id\": {\"$oid\": \"507f1f77bcf86cd799439011\"}}";
    private static final String DOCUMENT =
            "{\"_id\": {\"$oid\": \"507f1f77bcf86cd799439011\"}, \"name\": \"scooter\"}";

    @Test
    void testSpecifiedModeDelete() throws Exception {
        List<CdcRecord> records = extract(changeEvent("delete", null, ID));

        assertThat(records).hasSize(1);
        assertThat(records.get(0).kind()).isEqualTo(RowKind.DELETE);
        assertThat(records.get(0).data())
                .containsEntry("_id", "507f1f77bcf86cd799439011")
                .containsEntry("name", null);
    }

    @Test
    void testSpecifiedModeUpdate() throws Exception {
        List<CdcRecord> records = extract(changeEvent("update", DOCUMENT, ID));

        assertThat(records)
                .extracting(CdcRecord::kind)
                .containsExactly(RowKind.DELETE, RowKind.INSERT);
        assertThat(records.get(1).data())
                .containsEntry("_id", "507f1f77bcf86cd799439011")
                .containsEntry("name", "scooter");
    }

    @Test
    void testSpecifiedModeInsertWithoutConfiguredField() throws Exception {
        List<CdcRecord> records = extract(changeEvent("insert", ID, ID));

        assertThat(records).hasSize(1);
        assertThat(records.get(0).data()).containsEntry("name", null);
    }

    private static List<CdcRecord> extract(JsonNode root) throws Exception {
        Configuration config = new Configuration();
        config.set(START_MODE, "specified");
        config.set(FIELD_NAME, "_id,name");
        config.set(PARSER_PATH, "$._id,$.name");
        return new Mongo4VersionStrategy("db", "coll", Collections.emptyList(), config)
                .extractRecords(root).stream()
                        .map(RichCdcMultiplexRecord::toRichCdcRecord)
                        .map(RichCdcRecord::toCdcRecord)
                        .collect(Collectors.toList());
    }

    private static JsonNode changeEvent(String op, String fullDocument, String documentKey) {
        ObjectNode root = JsonSerdeUtil.OBJECT_MAPPER_INSTANCE.createObjectNode();
        root.put("operationType", op);
        if (fullDocument != null) {
            root.put("fullDocument", fullDocument);
        }
        root.put("documentKey", documentKey);
        return root;
    }
}
