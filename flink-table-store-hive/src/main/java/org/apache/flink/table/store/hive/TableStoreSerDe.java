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

package org.apache.flink.table.store.hive;

import org.apache.flink.table.store.RowDataContainer;
import org.apache.flink.table.store.hive.objectinspector.TableStoreRowDataObjectInspector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.Writable;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * {@link AbstractSerDe} for table store. It transforms map-reduce values to Hive objects.
 *
 * <p>Currently this class only supports deserialization.
 */
public class TableStoreSerDe extends AbstractSerDe {

    private TableStoreRowDataObjectInspector inspector;

    @Override
    public void initialize(@Nullable Configuration configuration, Properties properties)
            throws SerDeException {
        String columnNames = properties.getProperty(serdeConstants.LIST_COLUMNS);
        String columnNameDelimiter =
                properties.getProperty(
                        serdeConstants.COLUMN_NAME_DELIMITER, String.valueOf(SerDeUtils.COMMA));
        List<String> names = Arrays.asList(columnNames.split(columnNameDelimiter));

        String columnTypes = properties.getProperty(serdeConstants.LIST_COLUMN_TYPES);
        List<TypeInfo> typeInfos = TypeInfoUtils.getTypeInfosFromTypeString(columnTypes);

        // see MetastoreUtils#addCols for the exact property name and separator
        String columnCommentsPropertyName = "columns.comments";
        List<String> comments =
                Arrays.asList(properties.getProperty(columnCommentsPropertyName).split("\0", -1));

        inspector = new TableStoreRowDataObjectInspector(names, typeInfos, comments);
    }

    @Override
    public Class<? extends Writable> getSerializedClass() {
        return RowDataContainer.class;
    }

    @Override
    public Writable serialize(Object o, ObjectInspector objectInspector) throws SerDeException {
        throw new UnsupportedOperationException(
                "TableStoreSerDe currently only supports deserialization.");
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
