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

import org.apache.paimon.types.DataTypes;

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaBinaryObjectInspector;
import org.apache.hadoop.io.BytesWritable;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Test create object inspector for blob type. */
public class PaimonBlobObjectInspectorTest {

    @Test
    public void testCategoryAndClass() {
        PrimitiveObjectInspector oi =
                (PrimitiveObjectInspector) PaimonObjectInspectorFactory.create(DataTypes.BLOB());
        assertThat(oi.getCategory()).isEqualTo(ObjectInspector.Category.PRIMITIVE);
        assertThat(oi.getPrimitiveCategory())
                .isEqualTo(PrimitiveObjectInspector.PrimitiveCategory.BINARY);
        assertThat(oi.getJavaPrimitiveClass()).isEqualTo(byte[].class);
        assertThat(oi.getPrimitiveWritableClass()).isEqualTo(BytesWritable.class);
    }

    @Test
    public void testGetPrimitiveJavaObject() {
        PrimitiveObjectInspector oi =
                (PrimitiveObjectInspector) PaimonObjectInspectorFactory.create(DataTypes.BLOB());
        byte[] input = new byte[] {1, 2, 3, 4};
        assertThat((byte[]) oi.getPrimitiveJavaObject(input)).isEqualTo(input);
        assertThat(oi.getPrimitiveJavaObject(null)).isNull();
    }

    @Test
    public void testGetPrimitiveWritableObject() {
        PrimitiveObjectInspector oi =
                (PrimitiveObjectInspector) PaimonObjectInspectorFactory.create(DataTypes.BLOB());
        byte[] input = new byte[] {1, 2, 3, 4};
        BytesWritable expected = new BytesWritable(input);
        assertThat(oi.getPrimitiveWritableObject(input)).isEqualTo(expected);
        assertThat(oi.getPrimitiveWritableObject(null)).isNull();
    }

    @Test
    public void testCopyObject() {
        PrimitiveObjectInspector oi =
                (PrimitiveObjectInspector) PaimonObjectInspectorFactory.create(DataTypes.BLOB());
        byte[] input = new byte[] {1, 2, 3, 4};
        Object copy = oi.copyObject(input);
        assertThat(copy).isEqualTo(input);
        assertThat(oi.copyObject(null)).isNull();
    }

    @Test
    public void testCreateObjectInspector() {
        PrimitiveObjectInspector oi =
                (PrimitiveObjectInspector) PaimonObjectInspectorFactory.create(DataTypes.BLOB());
        assertThat(oi).isInstanceOf(JavaBinaryObjectInspector.class);
    }
}
