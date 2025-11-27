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

package org.apache.paimon.data.variant;

import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Utils for variant access. */
public class VariantAccessInfoUtils {

    /**
     * Extracted the fields from the variant fields, and build a new rowType to represent the actual
     * readType of the variant.
     */
    public static RowType actualReadType(List<VariantAccessInfo.VariantField> variantFields) {
        List<DataField> fields = new ArrayList<>();
        for (VariantAccessInfo.VariantField variantField : variantFields) {
            fields.add(variantField.dataField());
        }
        return new RowType(fields);
    }

    /** Replace the variant with the actual readType. */
    public static RowType buildReadRowType(
            RowType readType, VariantAccessInfo[] variantAccessInfo) {
        Map<String, List<VariantAccessInfo.VariantField>> variantFieldsMap = new HashMap<>();
        for (VariantAccessInfo info : variantAccessInfo) {
            variantFieldsMap.put(info.columnName(), info.variantFields());
        }
        List<DataField> fields = new ArrayList<>();
        for (DataField field : readType.getFields()) {
            if (variantFieldsMap.containsKey(field.name())) {
                fields.add(
                        field.newType(
                                VariantAccessInfoUtils.actualReadType(
                                        variantFieldsMap.get(field.name()))));
            } else {
                fields.add(field);
            }
        }
        return new RowType(fields);
    }
}
