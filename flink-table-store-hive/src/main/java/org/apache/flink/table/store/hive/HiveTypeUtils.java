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

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.store.hive.objectinspector.TableStoreCharObjectInspector;
import org.apache.flink.table.store.hive.objectinspector.TableStoreDateObjectInspector;
import org.apache.flink.table.store.hive.objectinspector.TableStoreDecimalObjectInspector;
import org.apache.flink.table.store.hive.objectinspector.TableStoreListObjectInspector;
import org.apache.flink.table.store.hive.objectinspector.TableStoreMapObjectInspector;
import org.apache.flink.table.store.hive.objectinspector.TableStoreStringObjectInspector;
import org.apache.flink.table.store.hive.objectinspector.TableStoreTimestampObjectInspector;
import org.apache.flink.table.store.hive.objectinspector.TableStoreVarcharObjectInspector;
import org.apache.flink.table.types.DataType;

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.CharTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.VarcharTypeInfo;

/** Utils for converting types related classes between Flink and Hive. */
public class HiveTypeUtils {

    public static DataType typeInfoToDataType(TypeInfo typeInfo) {
        ObjectInspector.Category category = typeInfo.getCategory();
        switch (category) {
            case PRIMITIVE:
                PrimitiveObjectInspector.PrimitiveCategory primitiveCategory =
                        ((PrimitiveTypeInfo) typeInfo).getPrimitiveCategory();
                switch (primitiveCategory) {
                    case BOOLEAN:
                        return DataTypes.BOOLEAN();
                    case BYTE:
                        return DataTypes.TINYINT();
                    case SHORT:
                        return DataTypes.SMALLINT();
                    case INT:
                        return DataTypes.INT();
                    case LONG:
                        return DataTypes.BIGINT();
                    case FLOAT:
                        return DataTypes.FLOAT();
                    case DOUBLE:
                        return DataTypes.DOUBLE();
                    case DECIMAL:
                        DecimalTypeInfo decimalTypeInfo = (DecimalTypeInfo) typeInfo;
                        return DataTypes.DECIMAL(
                                decimalTypeInfo.getPrecision(), decimalTypeInfo.getScale());
                    case CHAR:
                        return DataTypes.CHAR(((CharTypeInfo) typeInfo).getLength());
                    case VARCHAR:
                        return DataTypes.VARCHAR(((VarcharTypeInfo) typeInfo).getLength());
                    case STRING:
                        return DataTypes.STRING();
                    case BINARY:
                        return DataTypes.BYTES();
                    case DATE:
                        return DataTypes.DATE();
                    case TIMESTAMP:
                        // TODO verify precision
                        return DataTypes.TIMESTAMP(3);
                    default:
                        throw new UnsupportedOperationException(
                                "Unsupported primitive type info category "
                                        + primitiveCategory.name());
                }
            case LIST:
                ListTypeInfo listTypeInfo = (ListTypeInfo) typeInfo;
                return DataTypes.ARRAY(typeInfoToDataType(listTypeInfo.getListElementTypeInfo()));
            case MAP:
                MapTypeInfo mapTypeInfo = (MapTypeInfo) typeInfo;
                return DataTypes.MAP(
                        typeInfoToDataType(mapTypeInfo.getMapKeyTypeInfo()),
                        typeInfoToDataType(mapTypeInfo.getMapValueTypeInfo()));
            default:
                throw new UnsupportedOperationException(
                        "Unsupported type info category " + category.name());
        }
    }

    public static ObjectInspector getObjectInspector(TypeInfo typeInfo) {
        ObjectInspector.Category category = typeInfo.getCategory();
        switch (category) {
            case PRIMITIVE:
                PrimitiveTypeInfo primitiveTypeInfo = (PrimitiveTypeInfo) typeInfo;
                switch (primitiveTypeInfo.getPrimitiveCategory()) {
                    case DECIMAL:
                        return new TableStoreDecimalObjectInspector((DecimalTypeInfo) typeInfo);
                    case CHAR:
                        return new TableStoreCharObjectInspector((CharTypeInfo) typeInfo);
                    case VARCHAR:
                        return new TableStoreVarcharObjectInspector((VarcharTypeInfo) typeInfo);
                    case STRING:
                        return new TableStoreStringObjectInspector();
                    case DATE:
                        return new TableStoreDateObjectInspector();
                    case TIMESTAMP:
                        return new TableStoreTimestampObjectInspector();
                    default:
                        return PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(
                                primitiveTypeInfo);
                }
            case LIST:
                ListTypeInfo listTypeInfo = (ListTypeInfo) typeInfo;
                return new TableStoreListObjectInspector(listTypeInfo.getListElementTypeInfo());
            case MAP:
                MapTypeInfo mapTypeInfo = (MapTypeInfo) typeInfo;
                return new TableStoreMapObjectInspector(
                        mapTypeInfo.getMapKeyTypeInfo(), mapTypeInfo.getMapValueTypeInfo());
            default:
                throw new UnsupportedOperationException(
                        "Unsupported type info category " + category.name());
        }
    }
}
