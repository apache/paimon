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

import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link PaimonTimestampObjectInspector}. */
public class PaimonTimestampObjectInspectorTest {

    @Test
    public void testCategoryAndClass() {
        PaimonTimestampObjectInspector oi = new PaimonTimestampObjectInspector();

        assertThat(oi.getCategory()).isEqualTo(ObjectInspector.Category.PRIMITIVE);
        assertThat(oi.getPrimitiveCategory())
                .isEqualTo(PrimitiveObjectInspector.PrimitiveCategory.TIMESTAMP);

        assertThat(oi.getJavaPrimitiveClass()).isEqualTo(java.sql.Timestamp.class);
        assertThat(oi.getPrimitiveWritableClass()).isEqualTo(TimestampWritable.class);
    }

    @Test
    public void testGetPrimitiveJavaObject() {
        PaimonTimestampObjectInspector oi = new PaimonTimestampObjectInspector();

        LocalDateTime local = LocalDateTime.of(2022, 4, 27, 15, 0, 0, 100_000_000);
        Timestamp input = Timestamp.fromLocalDateTime(local);
        assertThat(oi.getPrimitiveJavaObject(input).toString()).isEqualTo("2022-04-27 15:00:00.1");
        assertThat(oi.getPrimitiveJavaObject(null)).isNull();
    }

    @Test
    public void testGetPrimitiveWritableObject() {
        PaimonTimestampObjectInspector oi = new PaimonTimestampObjectInspector();

        LocalDateTime local = LocalDateTime.of(2022, 4, 27, 15, 0, 0, 100_000_000);
        Timestamp input = Timestamp.fromLocalDateTime(local);
        assertThat(oi.getPrimitiveWritableObject(input).getTimestamp().toString())
                .isEqualTo("2022-04-27 15:00:00.1");
        assertThat(oi.getPrimitiveWritableObject(null)).isNull();
    }

    @Test
    public void testCopyObject() {
        PaimonTimestampObjectInspector oi = new PaimonTimestampObjectInspector();

        // TimestampData is immutable
        Timestamp input1 = Timestamp.fromEpochMillis(10007);
        Object copy1 = oi.copyObject(input1);
        assertThat(copy1).isEqualTo(input1);

        java.sql.Timestamp input2 = new java.sql.Timestamp(10007);
        Object copy2 = oi.copyObject(input2);
        assertThat(copy2).isEqualTo(input2);
        assertThat(copy2).isNotSameAs(input2);

        assertThat(oi.copyObject(null)).isNull();
    }
}
