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

package org.apache.paimon.iceberg.manifest;

import org.apache.paimon.iceberg.metadata.IcebergDataField;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.TimeType;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import static org.assertj.core.api.Assertions.assertThat;

class IcebergConversionsTimeTest {

    private static final int MIDDAY_MS = 47_003_524; // 13:03:03.524

    @Test
    void testTimeToByteBuffer() {
        ByteBuffer buffer = IcebergConversions.toByteBuffer(DataTypes.TIME(3), MIDDAY_MS);
        assertThat(buffer.order()).isEqualTo(ByteOrder.LITTLE_ENDIAN);
        assertThat(buffer.getLong(0)).isEqualTo(MIDDAY_MS * 1000L);
    }

    @Test
    void testToPaimonObjectForTime() {
        byte[] bytes = new byte[8];
        ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN).putLong(MIDDAY_MS * 1000L);

        Object actual = IcebergConversions.toPaimonObject(DataTypes.TIME(3), bytes);

        assertThat(actual).isEqualTo(MIDDAY_MS);
    }

    @Test
    void testIcebergDataFieldToTypeObjectForTime() {
        DataField field = new DataField(0, "event_time", DataTypes.TIME(3));
        IcebergDataField icebergField = new IcebergDataField(field);

        assertThat(icebergField.type()).isEqualTo("time");
    }

    @Test
    void testIcebergDataFieldFromTypeForTime() {
        IcebergDataField field = new IcebergDataField(0, "event_time", false, "time", null);

        assertThat(field.dataType()).isInstanceOf(TimeType.class);
    }
}
