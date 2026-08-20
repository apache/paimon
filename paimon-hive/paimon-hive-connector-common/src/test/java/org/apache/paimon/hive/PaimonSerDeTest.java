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

package org.apache.paimon.hive;

import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalMap;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.hive.objectinspector.PaimonInternalRowObjectInspector;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.apache.paimon.hive.RandomGenericRowDataGenerator.FIELD_COMMENTS;
import static org.apache.paimon.hive.RandomGenericRowDataGenerator.FIELD_NAMES;
import static org.apache.paimon.hive.RandomGenericRowDataGenerator.ROW_TYPE;
import static org.apache.paimon.hive.RandomGenericRowDataGenerator.generate;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link PaimonSerDe}. */
public class PaimonSerDeTest {

    @TempDir java.nio.file.Path tempDir;

    @Test
    public void testInitialize() throws Exception {
        PaimonSerDe serDe = createInitializedSerDe();
        ObjectInspector o = serDe.getObjectInspector();
        assertThat(o).isInstanceOf(PaimonInternalRowObjectInspector.class);
        PaimonInternalRowObjectInspector oi = (PaimonInternalRowObjectInspector) o;
        GenericRow rowData = generate();
        List<? extends StructField> structFields = oi.getAllStructFieldRefs();
        for (int i = 0; i < structFields.size(); i++) {
            assertThat(oi.getStructFieldData(rowData, structFields.get(i)))
                    .isEqualTo(rowData.getField(i));
            assertThat(structFields.get(i).getFieldName()).isEqualTo(FIELD_NAMES.get(i));
            assertThat(structFields.get(i).getFieldComment()).isEqualTo(FIELD_COMMENTS.get(i));
        }
    }

    @Test
    public void testDeserialize() throws Exception {
        PaimonSerDe serDe = createInitializedSerDe();
        GenericRow rowData = generate();
        RowDataContainer container = new RowDataContainer();
        container.set(rowData);
        assertThat(serDe.deserialize(container)).isEqualTo(rowData);
    }

    @Test
    public void testSerializeMultiset() throws Exception {
        RowType rowType =
                RowType.of(
                        new DataType[] {DataTypes.INT(), DataTypes.MULTISET(DataTypes.STRING())},
                        new String[] {"id", "tags"});
        PaimonSerDe serDe = createInitializedSerDe(rowType);

        // hive represents a multiset column as map<element, count>
        StructObjectInspector sourceInspector =
                ObjectInspectorFactory.getStandardStructObjectInspector(
                        Arrays.asList("id", "tags"),
                        Arrays.asList(
                                PrimitiveObjectInspectorFactory.javaIntObjectInspector,
                                ObjectInspectorFactory.getStandardMapObjectInspector(
                                        PrimitiveObjectInspectorFactory.javaStringObjectInspector,
                                        PrimitiveObjectInspectorFactory.javaIntObjectInspector)));

        Map<String, Integer> tags = new LinkedHashMap<>();
        tags.put("apple", 2);
        tags.put("banana", 1);

        InternalRow row =
                ((RowDataContainer) serDe.serialize(Arrays.asList(1, tags), sourceInspector)).get();

        assertThat(row.getInt(0)).isEqualTo(1);
        InternalMap actual = row.getMap(1);
        assertThat(actual.size()).isEqualTo(tags.size());
        Map<String, Integer> actualTags = new HashMap<>();
        for (int i = 0; i < actual.size(); i++) {
            actualTags.put(
                    actual.keyArray().getString(i).toString(), actual.valueArray().getInt(i));
        }
        assertThat(actualTags).isEqualTo(tags);
    }

    private PaimonSerDe createInitializedSerDe() throws Exception {
        return createInitializedSerDe(ROW_TYPE);
    }

    private PaimonSerDe createInitializedSerDe(RowType rowType) throws Exception {
        new SchemaManager(LocalFileIO.create(), new Path(tempDir.toString()))
                .createTable(
                        new Schema(
                                rowType.getFields(),
                                Collections.emptyList(),
                                Collections.emptyList(),
                                new HashMap<>(),
                                ""));

        Properties properties = new Properties();
        properties.setProperty("location", tempDir.toString());

        PaimonSerDe serDe = new PaimonSerDe();
        serDe.initialize(null, properties);
        return serDe;
    }
}
