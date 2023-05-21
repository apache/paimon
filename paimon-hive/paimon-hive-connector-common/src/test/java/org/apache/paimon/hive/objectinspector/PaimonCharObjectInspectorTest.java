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

import org.apache.paimon.data.BinaryString;

import org.apache.hadoop.hive.common.type.HiveChar;
import org.apache.hadoop.hive.serde2.io.HiveCharWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link PaimonCharObjectInspector}. */
public class PaimonCharObjectInspectorTest {

    @Test
    public void testCategoryAndClass() {
        PaimonCharObjectInspector oi = new PaimonCharObjectInspector(10);

        assertThat(oi.getCategory()).isEqualTo(ObjectInspector.Category.PRIMITIVE);
        assertThat(oi.getPrimitiveCategory())
                .isEqualTo(PrimitiveObjectInspector.PrimitiveCategory.CHAR);

        assertThat(oi.getJavaPrimitiveClass()).isEqualTo(HiveChar.class);
        assertThat(oi.getPrimitiveWritableClass()).isEqualTo(HiveCharWritable.class);
    }

    @Test
    public void testGetPrimitiveJavaObject() {
        PaimonCharObjectInspector oi = new PaimonCharObjectInspector(10);

        BinaryString input1 = BinaryString.fromString("testString");
        HiveChar expected1 = new HiveChar("testString", 10);
        BinaryString input2 = BinaryString.fromString("test");
        HiveChar expected2 = new HiveChar("test", 10);
        assertThat(oi.getPrimitiveJavaObject(input1)).isEqualTo(expected1);
        assertThat(oi.getPrimitiveJavaObject(input2)).isEqualTo(expected2);
        assertThat(oi.getPrimitiveJavaObject(null)).isNull();
    }

    @Test
    public void testGetPrimitiveWritableObject() {
        PaimonCharObjectInspector oi = new PaimonCharObjectInspector(10);

        BinaryString input1 = BinaryString.fromString("testString");
        HiveCharWritable expected1 = new HiveCharWritable(new HiveChar("testString", 10));
        BinaryString input2 = BinaryString.fromString("test");
        HiveCharWritable expected2 = new HiveCharWritable(new HiveChar("test", 10));
        assertThat(oi.getPrimitiveWritableObject(input1)).isEqualTo(expected1);
        assertThat(oi.getPrimitiveWritableObject(input2)).isEqualTo(expected2);
        assertThat(oi.getPrimitiveWritableObject(null)).isNull();
    }

    @Test
    public void testCopyObject() {
        PaimonCharObjectInspector oi = new PaimonCharObjectInspector(10);

        BinaryString input1 = BinaryString.fromString("testString");
        Object copy1 = oi.copyObject(input1);
        assertThat(copy1).isEqualTo(input1);
        assertThat(copy1).isNotSameAs(input1);

        HiveChar input2 = new HiveChar("test", 10);
        Object copy2 = oi.copyObject(input2);
        assertThat(copy2).isEqualTo(input2);
        assertThat(copy2).isNotSameAs(input2);

        assertThat(oi.copyObject(null)).isNull();
    }
}
