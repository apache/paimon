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

package org.apache.paimon.utils;

import org.apache.paimon.KeyValue;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.io.DataInputDeserializer;
import org.apache.paimon.io.DataOutputSerializer;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link KeyValueWithLevelNoReusingSerializer} with snapshotId. */
class KeyValueWithLevelNoReusingSerializerSnapshotIdTest {

    private static final RowType KEY_TYPE =
            RowType.of(
                    new org.apache.paimon.types.DataType[] {DataTypes.INT()}, new String[] {"k0"});
    private static final RowType VALUE_TYPE =
            RowType.of(
                    new org.apache.paimon.types.DataType[] {DataTypes.INT()}, new String[] {"v0"});

    @Test
    void testRoundTripWithSnapshotId() throws Exception {
        KeyValueWithLevelNoReusingSerializer serializer =
                new KeyValueWithLevelNoReusingSerializer(KEY_TYPE, VALUE_TYPE, true);

        KeyValue kv =
                new KeyValue()
                        .replace(GenericRow.of(1), 42L, RowKind.INSERT, GenericRow.of(100))
                        .setLevel(3)
                        .setSnapshotId(7L);

        DataOutputSerializer out = new DataOutputSerializer(256);
        serializer.serialize(kv, out);

        DataInputDeserializer in = new DataInputDeserializer(out.getCopyOfBuffer());
        KeyValue deserialized = serializer.deserialize(in);

        assertThat(deserialized.key().getInt(0)).isEqualTo(1);
        assertThat(deserialized.sequenceNumber()).isEqualTo(42L);
        assertThat(deserialized.valueKind()).isEqualTo(RowKind.INSERT);
        assertThat(deserialized.value().getInt(0)).isEqualTo(100);
        assertThat(deserialized.level()).isEqualTo(3);
        assertThat(deserialized.snapshotId()).isEqualTo(7L);
    }

    @Test
    void testRoundTripWithoutSnapshotId() throws Exception {
        KeyValueWithLevelNoReusingSerializer serializer =
                new KeyValueWithLevelNoReusingSerializer(KEY_TYPE, VALUE_TYPE, false);

        KeyValue kv =
                new KeyValue()
                        .replace(GenericRow.of(1), 42L, RowKind.INSERT, GenericRow.of(100))
                        .setLevel(3)
                        .setSnapshotId(7L);

        DataOutputSerializer out = new DataOutputSerializer(256);
        serializer.serialize(kv, out);

        DataInputDeserializer in = new DataInputDeserializer(out.getCopyOfBuffer());
        KeyValue deserialized = serializer.deserialize(in);

        assertThat(deserialized.level()).isEqualTo(3);
        assertThat(deserialized.snapshotId()).isEqualTo(KeyValue.UNKNOWN_SNAPSHOT_ID);
    }
}
