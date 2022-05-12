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

import org.apache.flink.util.Preconditions;

import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/** Column names, types and comments of a Hive table. */
public class HiveSchema {

    private final List<String> fieldNames;
    private final List<TypeInfo> fieldTypeInfos;
    private final List<String> fieldComments;

    private HiveSchema(
            List<String> fieldNames, List<TypeInfo> fieldTypeInfos, List<String> fieldComments) {
        Preconditions.checkArgument(
                fieldNames.size() == fieldTypeInfos.size()
                        && fieldNames.size() == fieldComments.size(),
                "Length of field names (%s), type infos (%s) and comments (%s) are different.",
                fieldNames.size(),
                fieldTypeInfos.size(),
                fieldComments.size());
        this.fieldNames = fieldNames;
        this.fieldTypeInfos = fieldTypeInfos;
        this.fieldComments = fieldComments;
    }

    public List<String> fieldNames() {
        return fieldNames;
    }

    public List<TypeInfo> fieldTypeInfos() {
        return fieldTypeInfos;
    }

    public List<String> fieldComments() {
        return fieldComments;
    }

    /** Extract {@link HiveSchema} from Hive serde properties. */
    public static HiveSchema extract(Properties properties) {
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

        return new HiveSchema(names, typeInfos, comments);
    }
}
