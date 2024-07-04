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

import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.VarCharType;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.paimon.testutils.assertj.PaimonAssertions.anyCauseMatches;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link RichCdcMultiplexRecord} be deserialized by flink KryoSerializer. */
public class CdcRecordSerializeITCase {

    @Test
    public void testCdcRecordKryoSerialize() throws IOException {
        KryoSerializer<RichCdcMultiplexRecord> kr =
                createFlinkKryoSerializer(RichCdcMultiplexRecord.class);
        RowType.Builder rowType = RowType.builder();
        rowType.field("id", new BigIntType());
        rowType.field("name", new VarCharType());
        rowType.field("pt", new VarCharType());
        // this is an unmodifiable list.
        List<DataField> fields = rowType.build().getFields();
        List<String> primaryKeys = Collections.singletonList("id");
        Map<String, String> recordData = new HashMap<>();
        recordData.put("id", "1");
        recordData.put("name", "HunterXHunter");
        recordData.put("pt", "2024-06-28");
        CdcRecord cdcRecord = new CdcRecord(RowKind.INSERT, recordData);
        RichCdcMultiplexRecord serializeRecord =
                new RichCdcMultiplexRecord("default", "T", fields, primaryKeys, cdcRecord);

        TestOutputView outputView = new TestOutputView();
        kr.serialize(serializeRecord, outputView);
        RichCdcMultiplexRecord deserializeRecord = kr.deserialize(outputView.getInputView());
        assertThat(deserializeRecord.toRichCdcRecord().toCdcRecord()).isEqualTo(cdcRecord);
        assertThat(deserializeRecord.databaseName()).isEqualTo("default");
        assertThat(deserializeRecord.tableName()).isEqualTo("T");
        assertThat(deserializeRecord.primaryKeys()).isEqualTo(primaryKeys);
        assertThat(deserializeRecord.fields()).isEqualTo(fields);
    }

    @Test
    public void testUnmodifiableListKryoSerialize() throws IOException {
        KryoSerializer<List> kryoSerializer = createFlinkKryoSerializer(List.class);
        RowType.Builder rowType = RowType.builder();
        rowType.field("id", new BigIntType());
        rowType.field("name", new VarCharType());
        rowType.field("pt", new VarCharType());
        // Deserializing an unmodifiable list would be throw
        // java.lang.UnsupportedOperationException.
        List<DataField> fields = rowType.build().getFields();

        TestOutputView outputView = new TestOutputView();
        kryoSerializer.serialize(fields, outputView);
        assertThatThrownBy(() -> kryoSerializer.deserialize(outputView.getInputView()))
                .satisfies(anyCauseMatches(UnsupportedOperationException.class));

        // This `fields` is a modifiable list should be successfully serialized.
        TestOutputView outputView2 = new TestOutputView();
        fields = new ArrayList<>(fields);
        kryoSerializer.serialize(fields, outputView2);
        List<DataField> deserializeRecord = kryoSerializer.deserialize(outputView2.getInputView());
        assertThat(deserializeRecord).isEqualTo(fields);
    }

    public static <T> KryoSerializer<T> createFlinkKryoSerializer(Class<T> type) {
        return new KryoSerializer<>(type, new ExecutionConfig());
    }

    private static final class TestOutputView extends DataOutputStream implements DataOutputView {

        public TestOutputView() {
            super(new ByteArrayOutputStream(4096));
        }

        public TestInputView getInputView() {
            ByteArrayOutputStream baos = (ByteArrayOutputStream) out;
            return new TestInputView(baos.toByteArray());
        }

        @Override
        public void skipBytesToWrite(int numBytes) throws IOException {
            for (int i = 0; i < numBytes; i++) {
                write(0);
            }
        }

        @Override
        public void write(DataInputView source, int numBytes) throws IOException {
            byte[] buffer = new byte[numBytes];
            source.readFully(buffer);
            write(buffer);
        }
    }

    private static final class TestInputView extends DataInputStream implements DataInputView {

        public TestInputView(byte[] data) {
            super(new ByteArrayInputStream(data));
        }

        @Override
        public void skipBytesToRead(int numBytes) throws IOException {
            while (numBytes > 0) {
                int skipped = skipBytes(numBytes);
                numBytes -= skipped;
            }
        }
    }
}
