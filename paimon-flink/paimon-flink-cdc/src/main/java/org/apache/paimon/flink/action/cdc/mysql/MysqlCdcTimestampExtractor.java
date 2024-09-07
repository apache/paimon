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

package org.apache.paimon.flink.action.cdc.mysql;

import org.apache.paimon.flink.action.cdc.CdcSourceRecord;
import org.apache.paimon.flink.action.cdc.watermark.CdcDebeziumTimestampExtractor;
import org.apache.paimon.utils.JsonSerdeUtil;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.JsonNode;

/** Timestamp extractor for MySQL sources in CDC applications. */
public class MysqlCdcTimestampExtractor extends CdcDebeziumTimestampExtractor {

    @Override
    public long extractTimestamp(CdcSourceRecord record) throws JsonProcessingException {
        JsonNode json = JsonSerdeUtil.fromJson((String) record.getValue(), JsonNode.class);

        return JsonSerdeUtil.extractValueOrDefault(
                json, Long.class, Long.MIN_VALUE, "payload", "ts_ms");
    }
}
