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

package org.apache.paimon.hive.objectinspector;

import org.apache.paimon.data.Timestamp;

import org.apache.hadoop.hive.common.type.TimestampTZ;
import org.apache.hadoop.hive.serde2.io.TimestampLocalTZWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.time.ZoneId;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link PaimonTimestampLocalTZObjectInspector}. */
public class PaimonTimestampLocalTZObjectInspectorTest {

    @Test
    public void testCategoryAndClass() {
        PaimonTimestampLocalTZObjectInspector oi = new PaimonTimestampLocalTZObjectInspector();

        assertThat(oi.getCategory()).isEqualTo(ObjectInspector.Category.PRIMITIVE);
        assertThat(oi.getPrimitiveCategory())
                .isEqualTo(PrimitiveObjectInspector.PrimitiveCategory.TIMESTAMPLOCALTZ);

        assertThat(oi.getJavaPrimitiveClass()).isEqualTo(TimestampTZ.class);
        assertThat(oi.getPrimitiveWritableClass()).isEqualTo(TimestampLocalTZWritable.class);
    }

    @Test
    public void testGetPrimitiveJavaObject() {
        PaimonTimestampLocalTZObjectInspector oi = new PaimonTimestampLocalTZObjectInspector();

        LocalDateTime now = LocalDateTime.now().plusNanos(123);
        Timestamp input = Timestamp.fromLocalDateTime(now);
        TimestampTZ expected = new TimestampTZ(now.atZone(ZoneId.systemDefault()));
        assertThat(oi.getPrimitiveJavaObject(input)).isEqualTo(expected);

        assertThat(oi.getPrimitiveJavaObject(null)).isNull();
    }

    @Test
    public void testGetPrimitiveWritableObject() {
        PaimonTimestampLocalTZObjectInspector oi = new PaimonTimestampLocalTZObjectInspector();

        LocalDateTime now = LocalDateTime.now().plusNanos(123);
        Timestamp input = Timestamp.fromLocalDateTime(now);
        TimestampTZ expected = new TimestampTZ(now.atZone(ZoneId.systemDefault()));
        assertThat(oi.getPrimitiveWritableObject(input).getTimestampTZ()).isEqualTo(expected);

        assertThat(oi.getPrimitiveWritableObject(null)).isNull();
    }

    @Test
    public void testCopyObject() {
        PaimonTimestampLocalTZObjectInspector oi = new PaimonTimestampLocalTZObjectInspector();

        // TimestampData is immutable
        Timestamp input1 = Timestamp.fromEpochMillis(10007);
        Object copy1 = oi.copyObject(input1);
        assertThat(copy1).isEqualTo(input1);

        TimestampTZ input2 =
                new TimestampTZ(input1.toLocalDateTime().atZone(ZoneId.systemDefault()));
        Object copy2 = oi.copyObject(input2);
        assertThat(copy2).isEqualTo(input2);
        assertThat(copy2).isNotSameAs(input2);

        assertThat(oi.copyObject(null)).isNull();
    }

    @Test
    public void testConvertObject() {
        PaimonTimestampLocalTZObjectInspector oi = new PaimonTimestampLocalTZObjectInspector();

        LocalDateTime now = LocalDateTime.now().plusNanos(123);
        Timestamp expected = Timestamp.fromLocalDateTime(now);
        TimestampTZ input = new TimestampTZ(now.atZone(ZoneId.systemDefault()));
        assertThat(oi.convert(input)).isEqualTo(expected);

        assertThat(oi.convert(null)).isNull();
    }
}
