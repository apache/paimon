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

package org.apache.paimon.flink.action.cdc.serialization;

import org.apache.paimon.flink.action.cdc.CdcSourceRecord;

import org.apache.flink.api.common.functions.util.ListCollector;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link CdcDebeziumDeserializationSchema}. */
public class CdcDebeziumDeserializationSchemaTest {

    @Test
    public void testDeserializeNonAsciiValues() throws Exception {
        Schema valueSchema =
                SchemaBuilder.struct()
                        .field("id", Schema.INT32_SCHEMA)
                        .field("name", Schema.STRING_SCHEMA)
                        .build();
        Struct value = new Struct(valueSchema).put("id", 1).put("name", "中文 é");
        SourceRecord record =
                new SourceRecord(
                        Collections.emptyMap(),
                        Collections.emptyMap(),
                        "topic",
                        valueSchema,
                        value);

        List<CdcSourceRecord> out = new ArrayList<>();
        new CdcDebeziumDeserializationSchema().deserialize(record, new ListCollector<>(out));

        assertThat(out).hasSize(1);
        assertThat(out.get(0).getTopic()).isEqualTo("topic");
        assertThat(out.get(0).getValue()).isEqualTo("{\"id\":1,\"name\":\"中文 é\"}");
    }
}
