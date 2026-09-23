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
import org.apache.paimon.types.VarCharType;

import org.apache.hadoop.hive.common.type.HiveChar;
import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link PaimonObjectInspectorFactory}. */
public class PaimonObjectInspectorFactoryTest {

    @Test
    public void testCreateMultisetObjectInspector() {
        ObjectInspector oi =
                PaimonObjectInspectorFactory.create(DataTypes.MULTISET(DataTypes.STRING()));

        assertThat(oi.getCategory()).isEqualTo(ObjectInspector.Category.MAP);
        assertThat(oi.getTypeName()).isEqualTo("map<string,int>");

        MapObjectInspector mapOi = (MapObjectInspector) oi;
        Map<BinaryString, Integer> javaMap = new HashMap<>();
        javaMap.put(BinaryString.fromString("apple"), 2);
        javaMap.put(BinaryString.fromString("banana"), 1);
        GenericMap multisetData = new GenericMap(javaMap);

        assertThat(mapOi.getMapSize(multisetData)).isEqualTo(2);
        assertThat(mapOi.getMapValueElement(multisetData, BinaryString.fromString("apple")))
                .isEqualTo(2);
        assertThat(mapOi.getMapValueElement(multisetData, BinaryString.fromString("banana")))
                .isEqualTo(1);
    }

    @Test
    public void testCreateRowInspectorContainingMultiset() {
        PaimonInternalRowObjectInspector inspector =
                new PaimonInternalRowObjectInspector(
                        Arrays.asList("id", "tags"),
                        Arrays.asList(DataTypes.INT(), DataTypes.MULTISET(DataTypes.STRING())),
                        Arrays.asList(null, null));

        assertThat(inspector.getAllStructFieldRefs()).hasSize(2);
        assertThat(inspector.getStructFieldRef("tags").getFieldObjectInspector().getTypeName())
                .isEqualTo("map<string,int>");
    }

    @Test
    public void testCreateCharVarcharObjectInspectorExceedingHiveLimit() {
        assertThat(
                        PaimonObjectInspectorFactory.create(
                                        DataTypes.CHAR(HiveChar.MAX_CHAR_LENGTH + 1))
                                .getTypeName())
                .isEqualTo("string");
        assertThat(
                        PaimonObjectInspectorFactory.create(
                                        DataTypes.VARCHAR(HiveVarchar.MAX_VARCHAR_LENGTH + 1))
                                .getTypeName())
                .isEqualTo("string");
        // the reproducing case of issue #1565
        assertThat(
                        PaimonObjectInspectorFactory.create(
                                        DataTypes.VARCHAR(VarCharType.MAX_LENGTH - 1))
                                .getTypeName())
                .isEqualTo("string");
    }

    @Test
    public void testCreateCharVarcharObjectInspectorWithinHiveLimit() {
        assertThat(
                        PaimonObjectInspectorFactory.create(
                                        DataTypes.CHAR(HiveChar.MAX_CHAR_LENGTH))
                                .getTypeName())
                .isEqualTo("char(" + HiveChar.MAX_CHAR_LENGTH + ")");
        assertThat(
                        PaimonObjectInspectorFactory.create(
                                        DataTypes.VARCHAR(HiveVarchar.MAX_VARCHAR_LENGTH))
                                .getTypeName())
                .isEqualTo("varchar(" + HiveVarchar.MAX_VARCHAR_LENGTH + ")");
        assertThat(PaimonObjectInspectorFactory.create(DataTypes.STRING()).getTypeName())
                .isEqualTo("string");
    }
}
