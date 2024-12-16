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

import org.apache.paimon.types.LocalZonedTimestampType;

import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;

import java.lang.reflect.Field;

/**
 * Utils to convert between Hive TimestampLocalTZTypeInfo and Paimon {@link
 * LocalZonedTimestampType}, using reflection to solve compatibility between Hive 2 and Hive 3.
 */
public class LocalZonedTimestampTypeUtils {

    public static boolean isHiveLocalZonedTimestampType(TypeInfo hiveTypeInfo) {
        return "org.apache.hadoop.hive.serde2.typeinfo.TimestampLocalTZTypeInfo"
                .equals(hiveTypeInfo.getClass().getName());
    }

    public static TypeInfo hiveLocalZonedTimestampType() {
        try {
            Class<?> typeInfoFactoryClass =
                    Class.forName("org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory");
            Field field = typeInfoFactoryClass.getField("timestampLocalTZTypeInfo");
            return (TypeInfo) field.get(null);
        } catch (Exception e) {
            return TypeInfoFactory.timestampTypeInfo;
        }
    }
}
