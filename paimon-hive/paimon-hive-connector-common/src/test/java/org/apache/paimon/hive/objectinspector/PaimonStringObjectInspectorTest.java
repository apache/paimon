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

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link PaimonStringObjectInspector}. */
public class PaimonStringObjectInspectorTest {

    @Test
    public void testCategoryAndClass() {
        PaimonStringObjectInspector oi = new PaimonStringObjectInspector();

        assertThat(oi.getCategory()).isEqualTo(ObjectInspector.Category.PRIMITIVE);
        assertThat(oi.getPrimitiveCategory())
                .isEqualTo(PrimitiveObjectInspector.PrimitiveCategory.STRING);

        assertThat(oi.getJavaPrimitiveClass()).isEqualTo(String.class);
        assertThat(oi.getPrimitiveWritableClass()).isEqualTo(Text.class);
    }

    @Test
    public void testGetPrimitiveJavaObject() {
        PaimonStringObjectInspector oi = new PaimonStringObjectInspector();

        BinaryString input = BinaryString.fromString("testString");
        String expected = "testString";
        assertThat(oi.getPrimitiveJavaObject(input)).isEqualTo(expected);
        assertThat(oi.getPrimitiveJavaObject(null)).isNull();
    }

    @Test
    public void testGetPrimitiveWritableObject() {
        PaimonStringObjectInspector oi = new PaimonStringObjectInspector();

        BinaryString input = BinaryString.fromString("testString");
        Text expected = new Text("testString");
        assertThat(oi.getPrimitiveWritableObject(input)).isEqualTo(expected);
        assertThat(oi.getPrimitiveWritableObject(null)).isNull();
    }

    @Test
    public void testCopyObject() {
        PaimonStringObjectInspector oi = new PaimonStringObjectInspector();

        BinaryString input = BinaryString.fromString("testString");
        Object copy = oi.copyObject(input);
        assertThat(copy).isEqualTo(input);
        assertThat(copy).isNotSameAs(input);

        assertThat(oi.copyObject("testString")).isEqualTo("testString");
        assertThat(oi.copyObject(null)).isNull();
    }
}
