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

import org.apache.paimon.hive.objectinspector.PaimonInternalRowObjectInspector;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.JsonSerdeUtil;

import org.apache.paimon.shade.guava30.com.google.common.collect.Maps;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.core.type.TypeReference;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.Writable;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * {@link AbstractSerDe} for paimon. It transforms map-reduce values to Hive objects.
 *
 * <p>Currently this class only supports deserialization.
 */
public class PaimonSerDe extends AbstractSerDe {

    private PaimonInternalRowObjectInspector inspector;
    private HiveSchema tableSchema;

    private final RowDataContainer rowData = new RowDataContainer();

    private final Map<ObjectInspector, HiveDeserializer> deserializers =
            Maps.newHashMapWithExpectedSize(1);

    @Override
    public void initialize(@Nullable Configuration configuration, Properties properties)
            throws SerDeException {
        String dataFieldStr = properties.getProperty(PaimonStorageHandler.PAIMON_TABLE_FIELDS);
        if (dataFieldStr != null) {
            List<DataField> dataFields =
                    JsonSerdeUtil.fromJson(dataFieldStr, new TypeReference<List<DataField>>() {});
            this.tableSchema = new HiveSchema(new RowType(dataFields));
        } else {
            this.tableSchema = HiveSchema.extract(configuration, properties);
        }

        inspector =
                new PaimonInternalRowObjectInspector(
                        tableSchema.fieldNames(),
                        tableSchema.fieldTypes(),
                        tableSchema.fieldComments());
    }

    @Override
    public Class<? extends Writable> getSerializedClass() {
        return RowDataContainer.class;
    }

    @Override
    public Writable serialize(Object o, ObjectInspector objectInspector) throws SerDeException {
        HiveDeserializer deserializer = deserializers.get(objectInspector);
        if (deserializer == null) {
            deserializer =
                    new HiveDeserializer.Builder()
                            .schema(tableSchema)
                            .sourceInspector((StructObjectInspector) objectInspector)
                            .writerInspector(inspector)
                            .build();
            deserializers.put(objectInspector, deserializer);
        }

        rowData.set(deserializer.deserialize(o));
        return rowData;
    }

    @Override
    public SerDeStats getSerDeStats() {
        return null;
    }

    @Override
    public Object deserialize(Writable writable) throws SerDeException {
        return ((RowDataContainer) writable).get();
    }

    @Override
    public ObjectInspector getObjectInspector() throws SerDeException {
        return inspector;
    }
}
