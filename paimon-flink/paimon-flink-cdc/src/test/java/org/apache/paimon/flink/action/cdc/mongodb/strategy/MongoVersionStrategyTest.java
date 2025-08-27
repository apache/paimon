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

import org.apache.paimon.flink.sink.cdc.CdcSchema;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link MongoVersionStrategy}. */
class MongoVersionStrategyTest {

    @Test
    void testParseFieldsFromJsonRecordWithDoubleValue() {
        String record =
                "{"
                        + "\n  \"_id\": {\"$oid\": \"507f1f77bcf86cd799439011\"},"
                        + "\n  \"name\": \"John Doe\","
                        + "\n  \"age\": 30,"
                        + "\n  \"price\": 19.99,"
                        + "\n  \"active\": true"
                        + "\n}";

        String fieldPaths = "$.name,$.age,$.price,$.active";
        String fieldNames = "name,age,price,active";

        CdcSchema.Builder schemaBuilder = CdcSchema.newBuilder();

        Map<String, String> result =
                MongoVersionStrategy.parseFieldsFromJsonRecord(
                        record, fieldPaths, fieldNames, Collections.emptyList(), schemaBuilder);

        assertThat(result).containsEntry("name", "John Doe");
        assertThat(result).containsEntry("age", "30");
        assertThat(result).containsEntry("price", "19.99");
        assertThat(result).containsEntry("active", "true");
    }

    @Test
    void testParseFieldsFromJsonRecordWithStringValues() {
        String record =
                "{"
                        + "\n  \"_id\": {\"$oid\": \"507f1f77bcf86cd799439011\"},"
                        + "\n  \"name\": \"John Doe\","
                        + "\n  \"description\": \"A sample record\""
                        + "\n}";

        String fieldPaths = "$.name,$.description";
        String fieldNames = "name,description";

        CdcSchema.Builder schemaBuilder = CdcSchema.newBuilder();

        Map<String, String> result =
                MongoVersionStrategy.parseFieldsFromJsonRecord(
                        record, fieldPaths, fieldNames, Collections.emptyList(), schemaBuilder);

        assertThat(result).containsEntry("name", "John Doe");
        assertThat(result).containsEntry("description", "A sample record");
    }
}
