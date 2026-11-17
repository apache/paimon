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

package org.apache.paimon.flink.sink.cdc;

import org.apache.paimon.data.GenericRow;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowKind;

import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.util.InstantiationUtil;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for binary-aware CDC conversion and compatibility with ordinary string values. */
class CdcRecordUtilsTest {

    private static final byte[] BINARY = {0, (byte) 0xff, (byte) 0x80, 65};

    @Test
    void testBinaryAndTextConversions() {
        String encoded = Base64.getEncoder().encodeToString(BINARY);
        Map<String, String> data = new HashMap<>();
        data.put("binary", encoded);
        data.put("text", encoded);
        data.put("longtext", "中文");
        data.put("bits", "AQI=");
        CdcRecord record =
                CdcRecord.withBase64EncodedFields(
                        RowKind.DELETE, data, Collections.singleton("binary"));
        List<DataField> fields =
                Arrays.asList(
                        new DataField(0, "binary", DataTypes.BYTES()),
                        new DataField(1, "text", DataTypes.BYTES()),
                        new DataField(2, "longtext", DataTypes.BYTES()),
                        new DataField(3, "bits", DataTypes.BINARY(2)));
        GenericRow row = CdcRecordUtils.toGenericRow(record, fields, true).get();
        assertThat(row.getRowKind()).isEqualTo(RowKind.DELETE);
        assertValues(row, encoded);
        GenericRow projected = CdcRecordUtils.projectAsInsert(record, fields);
        assertThat(projected.getRowKind()).isEqualTo(RowKind.INSERT);
        assertValues(projected, encoded);
    }

    private static void assertValues(GenericRow row, String encoded) {
        assertThat(row.getBinary(0)).containsExactly(BINARY);
        // A Base64-looking value from Canal, a custom record, or a text mapping stays text.
        assertThat(row.getBinary(1)).containsExactly(encoded.getBytes(StandardCharsets.UTF_8));
        assertThat(row.getBinary(2)).containsExactly("中文".getBytes(StandardCharsets.UTF_8));
        assertThat(row.getBinary(3)).containsExactly((byte) 1, (byte) 2);
    }

    @Test
    void testBinarySourceIntoExistingTextColumn() {
        CdcRecord record =
                CdcRecord.withBase64EncodedFields(
                        RowKind.INSERT,
                        Collections.singletonMap("payload", "cGxhaW4gdGV4dA=="),
                        Collections.singleton("payload"));
        List<DataField> fields =
                Collections.singletonList(new DataField(0, "payload", DataTypes.STRING()));
        assertThat(CdcRecordUtils.toGenericRow(record, fields, true).get().getString(0).toString())
                .isEqualTo("plain text");
        assertThat(CdcRecordUtils.projectAsInsert(record, fields).getString(0).toString())
                .isEqualTo("plain text");
    }

    @Test
    void testDecodedLengthControlsSchemaEvolution() {
        CdcRecord record =
                CdcRecord.withBase64EncodedFields(
                        RowKind.INSERT,
                        Collections.singletonMap(
                                "payload", Base64.getEncoder().encodeToString(BINARY)),
                        Collections.singleton("payload"));
        List<DataField> fields =
                Collections.singletonList(new DataField(0, "payload", DataTypes.VARBINARY(4)));
        assertThat(CdcRecordUtils.toGenericRow(record, fields, true).get().getBinary(0))
                .containsExactly(BINARY);
        assertThat(CdcRecordUtils.projectAsInsert(record, fields).getBinary(0))
                .containsExactly(BINARY);
        List<DataField> tooShort =
                Collections.singletonList(new DataField(0, "payload", DataTypes.VARBINARY(3)));
        assertThat(CdcRecordUtils.toGenericRow(record, tooShort, false)).isEmpty();
        assertThatThrownBy(() -> CdcRecordUtils.projectAsInsert(record, tooShort))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("length of 4");
        List<DataField> fixed =
                Collections.singletonList(new DataField(0, "payload", DataTypes.BINARY(4)));
        assertThat(CdcRecordUtils.toGenericRow(record, fixed, true).get().getBinary(0))
                .containsExactly(BINARY);
    }

    @Test
    void testBinaryMetadataSurvivesCaseConversionAndSerialization() throws Exception {
        Set<String> base64EncodedFields = new HashSet<>(Collections.singleton("PAYLOAD"));
        CdcRecord record =
                CdcRecord.withBase64EncodedFields(
                        RowKind.INSERT,
                        Collections.singletonMap(
                                "PAYLOAD", Base64.getEncoder().encodeToString(BINARY)),
                        base64EncodedFields);
        base64EncodedFields
                .clear(); // The next parser record must not mutate an emitted record's metadata.
        CdcRecord lower = record.fieldNameLowerCase();
        assertThat(lower.base64EncodedFields()).containsExactly("payload");
        assertThat(record.base64EncodedFields()).containsExactly("PAYLOAD");
        List<DataField> fields =
                Collections.singletonList(new DataField(0, "payload", DataTypes.BYTES()));
        assertThat(CdcRecordUtils.projectAsInsert(lower, fields).getBinary(0))
                .containsExactly(BINARY);
        CdcRecord javaCopy = InstantiationUtil.clone(lower);
        assertThat(javaCopy).isEqualTo(lower);
        assertThat(CdcRecordUtils.toGenericRow(javaCopy, fields, true).get().getBinary(0))
                .containsExactly(BINARY);
        KryoSerializer<CdcRecord> serializer =
                CdcRecordSerializeITCase.createFlinkKryoSerializer(CdcRecord.class);
        DataOutputSerializer output = new DataOutputSerializer(128);
        serializer.serialize(lower, output);
        CdcRecord kryoCopy =
                serializer.deserialize(new DataInputDeserializer(output.getCopyOfBuffer()));
        assertThat(kryoCopy).isEqualTo(lower);
        assertThat(CdcRecordUtils.projectAsInsert(kryoCopy, fields).getBinary(0))
                .containsExactly(BINARY);
    }
}
