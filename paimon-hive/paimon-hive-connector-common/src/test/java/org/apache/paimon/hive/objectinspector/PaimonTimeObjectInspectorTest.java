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

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link PaimonTimeObjectInspector}. */
public class PaimonTimeObjectInspectorTest {

    @Test
    public void testCategoryAndClass() {
        PaimonTimeObjectInspector oi = new PaimonTimeObjectInspector();

        assertThat(oi.getCategory()).isEqualTo(ObjectInspector.Category.PRIMITIVE);
        assertThat(oi.getPrimitiveCategory())
                .isEqualTo(PrimitiveObjectInspector.PrimitiveCategory.STRING);

        assertThat(oi.getJavaPrimitiveClass()).isEqualTo(String.class);
        assertThat(oi.getPrimitiveWritableClass()).isEqualTo(Text.class);
    }

    @Test
    public void testGetPrimitiveJavaObject() {
        PaimonTimeObjectInspector oi = new PaimonTimeObjectInspector();

        int input = 1;
        assertThat(oi.getPrimitiveJavaObject(input)).isEqualTo("00:00:00.001");
        assertThat(oi.getPrimitiveJavaObject(null)).isNull();
    }

    @Test
    public void testGetPrimitiveWritableObject() {
        PaimonTimeObjectInspector oi = new PaimonTimeObjectInspector();

        int input = 86_399_000;
        assertThat(oi.getPrimitiveWritableObject(input).toString()).isEqualTo("23:59:59");
        assertThat(oi.getPrimitiveWritableObject(null)).isNull();
    }
}
