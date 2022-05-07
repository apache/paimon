/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.store.hive.objectinspector;

import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.junit.jupiter.api.Test;

import java.sql.Date;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link TableStoreDateObjectInspector}. */
public class TableStoreDateObjectInspectorTest {

    @Test
    public void testCategoryAndClass() {
        TableStoreDateObjectInspector oi = new TableStoreDateObjectInspector();

        assertThat(oi.getCategory()).isEqualTo(ObjectInspector.Category.PRIMITIVE);
        assertThat(oi.getPrimitiveCategory())
                .isEqualTo(PrimitiveObjectInspector.PrimitiveCategory.DATE);

        assertThat(oi.getJavaPrimitiveClass()).isEqualTo(Date.class);
        assertThat(oi.getPrimitiveWritableClass()).isEqualTo(DateWritable.class);
    }

    @Test
    public void testGetPrimitiveJavaObject() {
        TableStoreDateObjectInspector oi = new TableStoreDateObjectInspector();

        int input = 10007;
        Date expected = new Date(input);
        assertThat(oi.getPrimitiveJavaObject(input)).isEqualTo(expected);
        assertThat(oi.getPrimitiveJavaObject(null)).isNull();
    }

    @Test
    public void testGetPrimitiveWritableObject() {
        TableStoreDateObjectInspector oi = new TableStoreDateObjectInspector();

        int input = 10007;
        DateWritable expected = new DateWritable(new Date(input));
        assertThat(oi.getPrimitiveWritableObject(input)).isEqualTo(expected);
        assertThat(oi.getPrimitiveWritableObject(null)).isNull();
    }

    @Test
    public void testCopyObject() {
        TableStoreDateObjectInspector oi = new TableStoreDateObjectInspector();

        Date input = new Date(10007);
        Object copy = oi.copyObject(input);
        assertThat(copy).isEqualTo(input);
        assertThat(copy).isNotSameAs(input);

        assertThat(oi.copyObject(10007)).isEqualTo(10007);
        assertThat(oi.copyObject(null)).isNull();
    }
}
