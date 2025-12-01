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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.paimon.data.variant.PaimonShreddingUtils.METADATA_FIELD_NAME;
import static org.apache.paimon.data.variant.PaimonShreddingUtils.TYPED_VALUE_FIELD_NAME;
import static org.apache.paimon.data.variant.PaimonShreddingUtils.VARIANT_VALUE_FIELD_NAME;

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

    /** Clip the variant schema to read with variant access fields. */
    public static RowType clipVariantSchema(
            RowType shreddingSchema, List<VariantAccessInfo.VariantField> variantFields) {
        boolean canClip = true;
        Set<String> fieldsToRead = new HashSet<>();
        for (VariantAccessInfo.VariantField variantField : variantFields) {
            VariantPathSegment[] pathSegments = VariantPathSegment.parse(variantField.path());
            if (pathSegments.length < 1) {
                canClip = false;
                break;
            }

            // todo: support nested column pruning
            VariantPathSegment pathSegment = pathSegments[0];
            if (pathSegment instanceof VariantPathSegment.ObjectExtraction) {
                fieldsToRead.add(((VariantPathSegment.ObjectExtraction) pathSegment).getKey());
            } else {
                canClip = false;
                break;
            }
        }

        if (!canClip) {
            return shreddingSchema;
        }

        List<DataField> typedFieldsToRead = new ArrayList<>();
        DataField typedValue = shreddingSchema.getField(TYPED_VALUE_FIELD_NAME);
        for (DataField field : ((RowType) typedValue.type()).getFields()) {
            if (fieldsToRead.contains(field.name())) {
                typedFieldsToRead.add(field);
                fieldsToRead.remove(field.name());
            }
        }

        List<DataField> shreddingSchemaFields = new ArrayList<>();
        shreddingSchemaFields.add(shreddingSchema.getField(METADATA_FIELD_NAME));
        // If there are fields to read not in the `typed_value`, add the `value` field.
        if (!fieldsToRead.isEmpty()) {
            shreddingSchemaFields.add(shreddingSchema.getField(VARIANT_VALUE_FIELD_NAME));
        }
        shreddingSchemaFields.add(typedValue.newType(new RowType(typedFieldsToRead)));
        return new RowType(shreddingSchemaFields);
    }
}
