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

import org.apache.paimon.data.Decimal;

import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link PaimonDecimalObjectInspector}. */
public class PaimonDecimalObjectInspectorTest {

    @Test
    public void testCategoryAndClass() {
        PaimonDecimalObjectInspector oi = new PaimonDecimalObjectInspector(5, 3);

        assertThat(oi.getCategory()).isEqualTo(ObjectInspector.Category.PRIMITIVE);
        assertThat(oi.getPrimitiveCategory())
                .isEqualTo(PrimitiveObjectInspector.PrimitiveCategory.DECIMAL);

        assertThat(oi.getJavaPrimitiveClass()).isEqualTo(HiveDecimal.class);
        assertThat(oi.getPrimitiveWritableClass()).isEqualTo(HiveDecimalWritable.class);
    }

    @Test
    public void testGetPrimitiveJavaObject() {
        PaimonDecimalObjectInspector oi = new PaimonDecimalObjectInspector(5, 3);

        Decimal input = Decimal.fromBigDecimal(new BigDecimal("12.345"), 5, 3);
        HiveDecimal expected = HiveDecimal.create("12.345");
        assertThat(oi.getPrimitiveJavaObject(input)).isEqualTo(expected);
        assertThat(oi.getPrimitiveJavaObject(null)).isNull();
    }

    @Test
    public void testGetPrimitiveWritableObject() {
        PaimonDecimalObjectInspector oi = new PaimonDecimalObjectInspector(5, 3);

        Decimal input = Decimal.fromBigDecimal(new BigDecimal("12.345"), 5, 3);
        HiveDecimalWritable expected = new HiveDecimalWritable(HiveDecimal.create("12.345"));
        assertThat(oi.getPrimitiveWritableObject(input)).isEqualTo(expected);
        assertThat(oi.getPrimitiveWritableObject(null)).isNull();
    }

    @Test
    public void testCopyObject() {
        PaimonDecimalObjectInspector oi = new PaimonDecimalObjectInspector(5, 3);

        Decimal input1 = Decimal.fromBigDecimal(new BigDecimal("12.345"), 5, 3);
        Object copy1 = oi.copyObject(input1);
        assertThat(copy1).isEqualTo(input1);
        assertThat(copy1).isNotSameAs(input1);

        HiveDecimal input2 = HiveDecimal.create("12.345");
        Object copy2 = oi.copyObject(input2);
        assertThat(copy2).isEqualTo(input2);
        assertThat(copy2).isNotSameAs(input2);

        assertThat(oi.copyObject(null)).isNull();
    }
}
