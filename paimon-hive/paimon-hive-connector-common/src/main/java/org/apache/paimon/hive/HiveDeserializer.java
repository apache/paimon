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
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalMap;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.hive.objectinspector.HivePaimonArray;
import org.apache.paimon.hive.objectinspector.WriteableObjectInspector;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Preconditions;

import org.apache.paimon.shade.guava30.com.google.common.collect.Lists;
import org.apache.paimon.shade.guava30.com.google.common.collect.Maps;

import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/** A deserializer to deserialize hive objects to {@link InternalRow}. */
public class HiveDeserializer {
    private final FieldDeserializer fieldDeserializer;

    /**
     * Builder to create a HiveDeserializer instance. Requires a Paimon HiveSchema and the Hive
     * ObjectInspector for converting the data.
     */
    static class Builder {
        private HiveSchema schema;
        private StructObjectInspector writerInspector;
        private StructObjectInspector sourceInspector;

        Builder schema(HiveSchema mainSchema) {
            this.schema = mainSchema;
            return this;
        }

        Builder writerInspector(StructObjectInspector inspector) {
            this.writerInspector = inspector;
            return this;
        }

        Builder sourceInspector(StructObjectInspector inspector) {
            this.sourceInspector = inspector;
            return this;
        }

        HiveDeserializer build() {
            return new HiveDeserializer(
                    schema, new ObjectInspectorPair(writerInspector, sourceInspector));
        }
    }

    /**
     * Deserializes the Hive result object to a Paimon record using the provided ObjectInspectors.
     *
     * @param data The Hive data to deserialize
     * @return The resulting Paimon Record
     */
    InternalRow deserialize(Object data) {
        return (InternalRow) fieldDeserializer.value(data);
    }

    private HiveDeserializer(HiveSchema schema, ObjectInspectorPair pair) {
        this.fieldDeserializer = DeserializerVisitor.visit(schema, pair);
    }

    private static class DeserializerVisitor
            extends SchemaVisitor<ObjectInspectorPair, FieldDeserializer> {

        public static FieldDeserializer visit(HiveSchema schema, ObjectInspectorPair pair) {
            return visit(
                    schema,
                    new SchemaNameMappingObjectInspectorPair(schema, pair),
                    new DeserializerVisitor(),
                    new PartnerObjectInspectorByNameAccessors());
        }

        @Override
        public FieldDeserializer primitive(DataType type, ObjectInspectorPair pair) {
            return o -> {
                if (o == null) {
                    return null;
                }

                ObjectInspector writerFieldInspector = pair.writerInspector();
                ObjectInspector sourceFieldInspector = pair.sourceInspector();

                Object result =
                        ((PrimitiveObjectInspector) sourceFieldInspector).getPrimitiveJavaObject(o);
                if (writerFieldInspector instanceof WriteableObjectInspector) {
                    result = ((WriteableObjectInspector) writerFieldInspector).convert(result);
                }

                return result;
            };
        }

        @Override
        public FieldDeserializer rowType(
                RowType type, ObjectInspectorPair pair, List<FieldDeserializer> deserializers) {
            Preconditions.checkNotNull(type, "Can not create deserializer for null type");

            return o -> {
                if (o == null) {
                    return null;
                }

                List<Object> data =
                        ((StructObjectInspector) pair.sourceInspector())
                                .getStructFieldsDataAsList(o);

                GenericRow row = new GenericRow(data.size());

                for (int i = 0; i < data.size(); i++) {
                    Object fieldValue = data.get(i);
                    if (fieldValue != null) {
                        row.setField(i, deserializers.get(i).value(fieldValue));
                    } else {
                        row.setField(i, null);
                    }
                }

                return row;
            };
        }

        @Override
        public FieldDeserializer list(
                ArrayType type, ObjectInspectorPair pair, FieldDeserializer deserializer) {
            return o -> {
                if (o == null) {
                    return null;
                }

                List<Object> result = Lists.newArrayList();
                ListObjectInspector listInspector = (ListObjectInspector) pair.sourceInspector();
                for (Object val : listInspector.getList(o)) {
                    result.add(deserializer.value(val));
                }
                return new HivePaimonArray(type.getElementType(), result);
            };
        }

        @Override
        public FieldDeserializer map(
                MapType mapType,
                ObjectInspectorPair pair,
                FieldDeserializer keyDeserializer,
                FieldDeserializer valueDeserializer) {
            return o -> {
                if (o == null) {
                    return null;
                }
                List<Object> keys = new ArrayList<>();
                List<Object> values = new ArrayList<>();
                MapObjectInspector mapObjectInspector = (MapObjectInspector) pair.sourceInspector();
                Map<?, ?> map = mapObjectInspector.getMap(o);
                for (Map.Entry<?, ?> entry : map.entrySet()) {
                    keys.add(keyDeserializer.value(entry.getKey()));
                    values.add(valueDeserializer.value(entry.getValue()));
                }
                HivePaimonArray key = new HivePaimonArray(mapType.getKeyType(), keys);
                HivePaimonArray value = new HivePaimonArray(mapType.getValueType(), values);
                return new InternalMap() {
                    @Override
                    public int size() {
                        return map.size();
                    }

                    @Override
                    public InternalArray keyArray() {
                        return key;
                    }

                    @Override
                    public InternalArray valueArray() {
                        return value;
                    }
                };
            };
        }
    }

    private static class PartnerObjectInspectorByNameAccessors
            implements SchemaVisitor.PartnerAccessors<ObjectInspectorPair> {

        @Override
        public ObjectInspectorPair fieldPartner(ObjectInspectorPair pair, String name) {
            String sourceName = pair.sourceName(name);
            return new ObjectInspectorPair(
                    ((StructObjectInspector) pair.writerInspector())
                            .getStructFieldRef(name)
                            .getFieldObjectInspector(),
                    ((StructObjectInspector) pair.sourceInspector())
                            .getStructFieldRef(sourceName)
                            .getFieldObjectInspector());
        }

        @Override
        public ObjectInspectorPair mapKeyPartner(ObjectInspectorPair pair) {
            return new ObjectInspectorPair(
                    ((MapObjectInspector) pair.writerInspector()).getMapKeyObjectInspector(),
                    ((MapObjectInspector) pair.sourceInspector()).getMapKeyObjectInspector());
        }

        @Override
        public ObjectInspectorPair mapValuePartner(ObjectInspectorPair pair) {
            return new ObjectInspectorPair(
                    ((MapObjectInspector) pair.writerInspector()).getMapValueObjectInspector(),
                    ((MapObjectInspector) pair.sourceInspector()).getMapValueObjectInspector());
        }

        @Override
        public ObjectInspectorPair listPartner(ObjectInspectorPair pair) {
            return new ObjectInspectorPair(
                    ((ListObjectInspector) pair.writerInspector()).getListElementObjectInspector(),
                    ((ListObjectInspector) pair.sourceInspector()).getListElementObjectInspector());
        }
    }

    private interface FieldDeserializer {
        Object value(Object object);
    }

    /**
     * The column name in the HiVE query result does not match the actual column name. Therefore, we
     * need to convert the column name
     */
    private static class SchemaNameMappingObjectInspectorPair extends ObjectInspectorPair {
        private final Map<String, String> sourceNameMap;

        SchemaNameMappingObjectInspectorPair(HiveSchema schema, ObjectInspectorPair pair) {
            super(pair.writerInspector(), pair.sourceInspector());

            this.sourceNameMap = Maps.newHashMapWithExpectedSize(schema.fields().size());

            List<? extends StructField> fields =
                    ((StructObjectInspector) sourceInspector()).getAllStructFieldRefs();
            for (int i = 0; i < schema.fields().size(); ++i) {
                sourceNameMap.put(schema.fields().get(i).name(), fields.get(i).getFieldName());
            }
        }

        @Override
        String sourceName(String originalName) {
            return sourceNameMap.get(originalName);
        }
    }

    /**
     * To get the data for Paimon {@link GenericRow}s we have to use both ObjectInspectors.
     *
     * <p>We use the Hive ObjectInspectors (sourceInspector) to get the Hive primitive types.
     *
     * <p>We use the Paimon ObjectInspectors (writerInspector) to generate the correct type for
     * Paimon Records
     */
    private static class ObjectInspectorPair {
        private ObjectInspector writerInspector;
        private ObjectInspector sourceInspector;

        ObjectInspectorPair(ObjectInspector writerInspector, ObjectInspector sourceInspector) {
            this.writerInspector = writerInspector;
            this.sourceInspector = sourceInspector;
        }

        ObjectInspector writerInspector() {
            return writerInspector;
        }

        ObjectInspector sourceInspector() {
            return sourceInspector;
        }

        String sourceName(String originalName) {
            return originalName;
        }
    }
}
