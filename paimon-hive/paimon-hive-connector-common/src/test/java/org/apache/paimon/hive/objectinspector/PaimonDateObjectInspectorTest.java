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

import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.junit.jupiter.api.Test;

import java.sql.Date;
import java.time.LocalDate;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link PaimonDateObjectInspector}. */
public class PaimonDateObjectInspectorTest {

    @Test
    public void testCategoryAndClass() {
        PaimonDateObjectInspector oi = new PaimonDateObjectInspector();

        assertThat(oi.getCategory()).isEqualTo(ObjectInspector.Category.PRIMITIVE);
        assertThat(oi.getPrimitiveCategory())
                .isEqualTo(PrimitiveObjectInspector.PrimitiveCategory.DATE);

        assertThat(oi.getJavaPrimitiveClass()).isEqualTo(Date.class);
        assertThat(oi.getPrimitiveWritableClass()).isEqualTo(DateWritable.class);
    }

    @Test
    public void testGetPrimitiveJavaObject() {
        PaimonDateObjectInspector oi = new PaimonDateObjectInspector();

        int input = 375;
        assertThat(oi.getPrimitiveJavaObject(input).toString()).isEqualTo("1971-01-11");
        assertThat(oi.getPrimitiveJavaObject(null)).isNull();
    }

    @Test
    public void testGetPrimitiveWritableObject() {
        PaimonDateObjectInspector oi = new PaimonDateObjectInspector();

        int input = 375;
        assertThat(oi.getPrimitiveWritableObject(input).get().toString()).isEqualTo("1971-01-11");
        assertThat(oi.getPrimitiveWritableObject(null)).isNull();
    }

    @Test
    public void testCopyObject() {
        PaimonDateObjectInspector oi = new PaimonDateObjectInspector();

        Date input = Date.valueOf(LocalDate.ofEpochDay(375));
        Object copy = oi.copyObject(input);
        assertThat(copy).isEqualTo(input);
        assertThat(copy).isNotSameAs(input);

        assertThat(oi.copyObject(375)).isEqualTo(375);
        assertThat(oi.copyObject(null)).isNull();
    }
}
