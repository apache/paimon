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
import org.apache.paimon.data.GenericArray;
import org.apache.paimon.types.DataTypes;

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link PaimonListObjectInspector}. */
public class PaimonListObjectInspectorTest {

    @Test
    public void testCategoryAndTypeName() {
        PaimonListObjectInspector oi = new PaimonListObjectInspector(DataTypes.STRING());

        assertThat(oi.getCategory()).isEqualTo(ObjectInspector.Category.LIST);
        assertThat(oi.getTypeName()).isEqualTo("array<string>");
    }

    @Test
    public void testGetListAndElement() {
        PaimonListObjectInspector oi = new PaimonListObjectInspector(DataTypes.STRING());

        BinaryString[] stringDataArray =
                new BinaryString[] {
                    BinaryString.fromString("Hi"),
                    BinaryString.fromString("Hello"),
                    null,
                    BinaryString.fromString("Test")
                };
        GenericArray arrayData = new GenericArray(stringDataArray);
        assertThat(oi.getListLength(arrayData)).isEqualTo(4);
        for (int i = 0; i < 4; i++) {
            assertThat(oi.getListElement(arrayData, i)).isEqualTo(stringDataArray[i]);
        }
        assertThat(oi.getList(arrayData)).isEqualTo(Arrays.asList(stringDataArray));

        assertThat(oi.getListLength(null)).isEqualTo(-1);
        assertThat(oi.getListElement(null, 3)).isNull();
        assertThat(oi.getList(null)).isNull();
    }
}
