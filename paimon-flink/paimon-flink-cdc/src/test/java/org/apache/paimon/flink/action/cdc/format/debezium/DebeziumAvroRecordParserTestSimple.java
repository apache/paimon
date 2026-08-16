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

package org.apache.paimon.flink.action.cdc.format.debezium;

import org.apache.paimon.flink.action.cdc.CdcSourceRecord;
import org.apache.paimon.flink.action.cdc.TypeMapping;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Simple test for DebeziumAvroRecordParser field documentation preservation. */
public class DebeziumAvroRecordParserTestSimple {

    @Test
    public void testFieldDocumentationPreservation() throws Exception {
        // Create complete Avro schema with field documentation
        String schemaJson =
                "{"
                        + "\"type\":\"record\","
                        + "\"name\":\"Envelope\","
                        + "\"fields\":["
                        + "  {\"name\":\"op\",\"type\":\"string\"},"
                        + "  {\"name\":\"after\",\"type\":[\"null\",{"
                        + "    \"type\":\"record\",\"name\":\"UserRecord\",\"fields\":["
                        + "      {\"name\":\"id\",\"type\":\"int\",\"doc\":\"Primary key identifier\"},"
                        + "      {\"name\":\"name\",\"type\":\"string\",\"doc\":\"Full name of the person\"},"
                        + "      {\"name\":\"email\",\"type\":[\"null\",\"string\"],\"default\":null,\"doc\":\"Email address for contact\"},"
                        + "      {\"name\":\"city\",\"type\":\"string\"}"
                        + "    ]"
                        + "  }],\"default\":null},"
                        + "  {\"name\":\"source\",\"type\":{\"type\":\"record\",\"name\":\"Source\",\"fields\":["
                        + "    {\"name\":\"db\",\"type\":\"string\"},"
                        + "    {\"name\":\"table\",\"type\":\"string\"}"
                        + "  ]}}"
                        + "]}";

        org.apache.avro.Schema envelopeSchema =
                new org.apache.avro.Schema.Parser().parse(schemaJson);
        org.apache.avro.Schema userRecordSchema =
                envelopeSchema.getField("after").schema().getTypes().get(1);

        // Create test record data
        GenericRecord afterRecord = new GenericData.Record(userRecordSchema);
        afterRecord.put("id", 123);
        afterRecord.put("name", "John Doe");
        afterRecord.put("email", "john.doe@example.com");
        afterRecord.put("city", "New York");

        GenericRecord sourceRecord =
                new GenericData.Record(envelopeSchema.getField("source").schema());
        sourceRecord.put("db", "testdb");
        sourceRecord.put("table", "users");

        GenericRecord envelopeRecord = new GenericData.Record(envelopeSchema);
        envelopeRecord.put("op", "c");
        envelopeRecord.put("after", afterRecord);
        envelopeRecord.put("source", sourceRecord);

        // Create CdcSourceRecord and test parser
        CdcSourceRecord cdcRecord = new CdcSourceRecord("test-topic", null, envelopeRecord);

        DebeziumAvroRecordParser parser =
                new DebeziumAvroRecordParser(TypeMapping.defaultMapping(), Collections.emptyList());

        Schema paimonSchema = parser.buildSchema(cdcRecord);

        // Verify schema was built with field documentation
        assertThat(paimonSchema).isNotNull();
        List<DataField> fields = paimonSchema.fields();
        assertThat(fields).hasSize(4);

        // Field with documentation
        DataField idField = findFieldByName(fields, "id");
        assertThat(idField).isNotNull();
        assertThat(idField.description()).isEqualTo("Primary key identifier");
        assertThat(idField.type()).isEqualTo(DataTypes.INT());

        DataField nameField = findFieldByName(fields, "name");
        assertThat(nameField).isNotNull();
        assertThat(nameField.description()).isEqualTo("Full name of the person");
        assertThat(nameField.type()).isEqualTo(DataTypes.STRING());

        DataField emailField = findFieldByName(fields, "email");
        assertThat(emailField).isNotNull();
        assertThat(emailField.description()).isEqualTo("Email address for contact");

        // Field without documentation
        DataField cityField = findFieldByName(fields, "city");
        assertThat(cityField).isNotNull();
        assertThat(cityField.description()).isNull();
        assertThat(cityField.type()).isEqualTo(DataTypes.STRING());
    }

    private static DataField findFieldByName(List<DataField> fields, String name) {
        return fields.stream().filter(field -> field.name().equals(name)).findFirst().orElse(null);
    }
}
