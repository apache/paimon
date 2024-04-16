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

package org.apache.paimon.fileindex;

import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.MapType;

import java.util.Map;

/** Common function of file index put here. */
public class FileIndexCommon {

    private static final String JUNCTION_SYMBOL = "##";

    public static String toMapKey(String mapColumnName, String keyName) {
        return mapColumnName + JUNCTION_SYMBOL + keyName;
    }

    public static DataType getFieldType(Map<String, DataField> fields, String columnsName) {
        int index = columnsName.indexOf(JUNCTION_SYMBOL);
        if (index == -1) {
            return fields.get(columnsName).type();
        } else {
            return ((MapType) fields.get(columnsName.substring(0, index)).type()).getValueType();
        }
    }
}
