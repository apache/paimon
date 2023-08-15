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

package org.apache.paimon.utils;

import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;

import java.util.List;
import java.util.stream.Collectors;

import static org.apache.paimon.schema.SystemColumns.KEY_FIELD_PREFIX;

/** Utils to convert table schema RowType to real storage RowType. */
public class RowTypeUtils {
    public static RowType toStorageRowType(RowType keyWithValue) {
        List<String> fields = keyWithValue.getFieldNames();

        List<DataField> dataFields = keyWithValue.getFields();

        return new RowType(
                dataFields.stream()
                        .filter(a -> !fields.contains(KEY_FIELD_PREFIX + a.name()))
                        .collect(Collectors.toList()));
    }

    public static int[] projectionRowTypeWithKeyPrefix(RowType source, RowType expected) {
        int[] projection = new int[expected.getFieldCount()];

        List<String> expectedFields = expected.getFieldNames();
        List<String> sourceFields = source.getFieldNames();

        for (int i = 0; i < projection.length; i++) {
            String exField = expectedFields.get(i);
            int index = sourceFields.indexOf(exField);
            if (index == -1) {
                index = sourceFields.indexOf(KEY_FIELD_PREFIX + exField);
            }

            projection[i] = index;
        }
        return projection;
    }
}
