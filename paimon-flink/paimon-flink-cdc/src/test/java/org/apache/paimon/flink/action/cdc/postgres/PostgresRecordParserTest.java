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

package org.apache.paimon.flink.action.cdc.postgres;

import org.apache.paimon.flink.action.cdc.CdcSourceRecord;
import org.apache.paimon.flink.action.cdc.TypeMapping;
import org.apache.paimon.flink.sink.cdc.RichCdcMultiplexRecord;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;

import org.apache.flink.api.common.functions.util.ListCollector;
import org.apache.flink.configuration.Configuration;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for {@link PostgresRecordParser} field type extraction. */
public class PostgresRecordParserTest {

    /**
     * Verifies that {@code io.debezium.time.Timestamp} (int64, millisecond precision) is mapped to
     * {@code TIMESTAMP(3)}, not {@code BIGINT}.
     *
     * <p>PostgreSQL {@code timestamp(n)} columns with {@code n <= 3} are encoded by Debezium using
     * the {@code io.debezium.time.Timestamp} logical type (epoch-millis int64). The JDBC schema
     * path maps the same columns to {@code TIMESTAMP(n)}, so without this fix the two paths
     * disagree and schema evolution crashes with "Cannot convert field from TIMESTAMP(3) to BIGINT".
     */
    @Test
    public void testTimestampMillisFieldMapsToTimestamp3() throws Exception {
        String json = debeziumJson("io.debezium.time.Timestamp");
        List<RichCdcMultiplexRecord> out = parse(json);

        assertThat(out).isNotEmpty();
        DataField field = findField(out.get(0), "ts_col");
        assertThat(field.type())
                .as("io.debezium.time.Timestamp (int64) must map to TIMESTAMP(3), not BIGINT")
                .isEqualTo(DataTypes.TIMESTAMP(3).nullable());
    }

    /** Verifies that {@code io.debezium.time.MicroTimestamp} (int64) still maps to TIMESTAMP(6). */
    @Test
    public void testMicroTimestampFieldMapsToTimestamp6() throws Exception {
        String json = debeziumJson("io.debezium.time.MicroTimestamp");
        List<RichCdcMultiplexRecord> out = parse(json);

        assertThat(out).isNotEmpty();
        DataField field = findField(out.get(0), "ts_col");
        assertThat(field.type())
                .as("io.debezium.time.MicroTimestamp (int64) must map to TIMESTAMP(6)")
                .isEqualTo(DataTypes.TIMESTAMP(6).nullable());
    }

    /**
     * Verifies that a plain int64 field (no logical type name) still maps to BIGINT — i.e. the fix
     * does not break the default fallthrough.
     */
    @Test
    public void testPlainInt64FieldMapsToBigint() throws Exception {
        String json = debeziumJson(null);
        List<RichCdcMultiplexRecord> out = parse(json);

        assertThat(out).isNotEmpty();
        DataField field = findField(out.get(0), "ts_col");
        assertThat(field.type())
                .as("int64 with no logical type name must remain BIGINT")
                .isEqualTo(DataTypes.BIGINT().nullable());
    }

    // -------------------------------------------------------------------------
    // helpers
    // -------------------------------------------------------------------------

    private List<RichCdcMultiplexRecord> parse(String json) throws Exception {
        PostgresRecordParser parser =
                new PostgresRecordParser(
                        new Configuration(),
                        Collections.emptyList(),
                        TypeMapping.defaultMapping(),
                        new org.apache.paimon.flink.action.cdc.CdcMetadataConverter[0]);
        List<RichCdcMultiplexRecord> out = new ArrayList<>();
        parser.flatMap(new CdcSourceRecord(json), new ListCollector<>(out));
        return out;
    }

    private DataField findField(RichCdcMultiplexRecord record, String fieldName) {
        return record.cdcSchema().fields().stream()
                .filter(f -> f.name().equals(fieldName))
                .findFirst()
                .orElseThrow(
                        () ->
                                new AssertionError(
                                        "Field '" + fieldName + "' not found in schema: "
                                                + record.cdcSchema().fields()));
    }

    /**
     * Builds a minimal Debezium PostgreSQL CDC JSON event containing one int64 column {@code
     * ts_col} with the given logical type {@code schemaName} (may be null for a plain int64).
     */
    private static String debeziumJson(String schemaName) {
        String nameField = schemaName == null ? "" : "\"name\":\"" + schemaName + "\",";
        return "{"
                + "\"schema\":{"
                + "  \"type\":\"struct\","
                + "  \"fields\":["
                + "    {"
                + "      \"type\":\"struct\","
                + "      \"optional\":true,"
                + "      \"field\":\"after\","
                + "      \"fields\":["
                + "        {\"type\":\"int32\",\"optional\":false,\"field\":\"id\"},"
                + "        {\"type\":\"int64\",\"optional\":true,"
                + nameField
                + "         \"field\":\"ts_col\"}"
                + "      ]"
                + "    },"
                + "    {"
                + "      \"type\":\"struct\","
                + "      \"optional\":false,"
                + "      \"field\":\"source\","
                + "      \"fields\":[]"
                + "    }"
                + "  ]"
                + "},"
                + "\"payload\":{"
                + "  \"op\":\"r\","
                + "  \"after\":{\"id\":1,\"ts_col\":1700000000000},"
                + "  \"source\":{\"db\":\"testdb\",\"table\":\"test_table\"}"
                + "}"
                + "}";
    }
}
