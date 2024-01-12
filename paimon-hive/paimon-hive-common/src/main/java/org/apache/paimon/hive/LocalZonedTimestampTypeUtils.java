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

import org.apache.paimon.types.DataType;
import org.apache.paimon.types.LocalZonedTimestampType;

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;

import java.util.Optional;

/** To maintain compatibility with Hive 3. */
public class LocalZonedTimestampTypeUtils {

    public static Optional<DataType> toPaimonType(TypeInfo hiveTypeInfo) {
        return Optional.empty();
    }

    public static TypeInfo toHiveType(LocalZonedTimestampType paimonType) {
        return TypeInfoFactory.timestampTypeInfo;
    }

    public static ObjectInspector createObjectInspector(DataType logicalType) {
        throw new UnsupportedOperationException(
                "Unsupported logical type " + logicalType.asSQLString());
    }
}
