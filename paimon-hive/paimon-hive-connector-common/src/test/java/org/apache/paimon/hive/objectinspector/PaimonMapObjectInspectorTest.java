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
import org.apache.paimon.data.GenericMap;
import org.apache.paimon.types.DataTypes;

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link PaimonMapObjectInspector}. */
public class PaimonMapObjectInspectorTest {

    @Test
    public void testCategoryAndTypeName() {
        PaimonMapObjectInspector oi =
                new PaimonMapObjectInspector(DataTypes.STRING(), DataTypes.BIGINT());

        assertThat(oi.getCategory()).isEqualTo(ObjectInspector.Category.MAP);
        assertThat(oi.getTypeName()).isEqualTo("map<string,bigint>");
    }

    @Test
    public void testGetMapAndValue() {
        PaimonMapObjectInspector oi =
                new PaimonMapObjectInspector(DataTypes.STRING(), DataTypes.BIGINT());

        BinaryString[] keyArray =
                new BinaryString[] {
                    BinaryString.fromString("Hi"),
                    BinaryString.fromString("Hello"),
                    BinaryString.fromString("Test")
                };
        Long[] valueArray = new Long[] {1L, null, 2L};
        Map<BinaryString, Long> javaMap = new HashMap<>();
        for (int i = 0; i < keyArray.length; i++) {
            javaMap.put(keyArray[i], valueArray[i]);
        }
        GenericMap mapData = new GenericMap(javaMap);

        assertThat(oi.getMapSize(mapData)).isEqualTo(3);
        for (int i = 0; i < keyArray.length; i++) {
            assertThat(oi.getMapValueElement(mapData, keyArray[i])).isEqualTo(valueArray[i]);
        }
        assertThat(oi.getMapValueElement(mapData, BinaryString.fromString("NotKey"))).isNull();
        assertThat(oi.getMapValueElement(mapData, null)).isNull();
        assertThat(oi.getMap(mapData)).isEqualTo(javaMap);

        assertThat(oi.getMapSize(null)).isEqualTo(-1);
        assertThat(oi.getMapValueElement(null, BinaryString.fromString("Hi"))).isNull();
        assertThat(oi.getMap(null)).isNull();
    }
}
