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

package org.apache.paimon.format.parquet;

import org.apache.paimon.data.variant.PaimonShreddingUtils;
import org.apache.paimon.data.variant.VariantAccessInfo;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.VariantType;
import org.apache.paimon.utils.JsonSerdeUtil;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.schema.MessageType;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/** Utils for variant. */
public class VariantUtils {

    /** For reader, extract shredding schemas from each parquet file's schema. */
    public static RowType[] extractShreddingSchemasFromParquetSchema(
            DataField[] readFields, MessageType fileSchema) {
        RowType[] shreddingSchemas = new RowType[readFields.length];
        for (int i = 0; i < readFields.length; i++) {
            DataField field = readFields[i];
            if (field.type() instanceof VariantType
                    && fileSchema
                            .getType(field.name())
                            .asGroupType()
                            .containsField(PaimonShreddingUtils.TYPED_VALUE_FIELD_NAME)) {
                RowType shreddingSchema =
                        (RowType)
                                ParquetSchemaConverter.convertToPaimonField(
                                                fileSchema.getType(field.name()))
                                        .type();
                shreddingSchemas[i] = shreddingSchema;
            }
        }
        return shreddingSchemas;
    }

    @Nullable
    public static RowType shreddingFields(Configuration conf) {
        String shreddingSchema =
                conf.get(ParquetOptions.PARQUET_VARIANT_SHREDDING_SCHEMA.key(), "");
        if (shreddingSchema.isEmpty()) {
            return null;
        } else {
            return (RowType) JsonSerdeUtil.fromJson(shreddingSchema, DataType.class);
        }
    }

    /** For writer, extract shredding schemas from conf. */
    @Nullable
    public static RowType extractShreddingSchemaFromConf(Configuration conf, String fieldName) {
        RowType shreddingFields = shreddingFields(conf);
        if (shreddingFields != null && shreddingFields.containsField(fieldName)) {
            return PaimonShreddingUtils.variantShreddingSchema(
                    (RowType) shreddingFields.getField(fieldName).type());
        } else {
            return null;
        }
    }

    public static RowType replaceWithShreddingType(Configuration conf, RowType rowType) {
        RowType shreddingFields = shreddingFields(conf);
        if (shreddingFields == null) {
            return rowType;
        }

        List<DataField> newFields = new ArrayList<>();
        for (DataField field : rowType.getFields()) {
            if (field.type() instanceof VariantType
                    && shreddingFields.containsField(field.name())) {
                RowType shreddingSchema =
                        PaimonShreddingUtils.variantShreddingSchema(
                                (RowType) shreddingFields.getField(field.name()).type());
                newFields.add(field.newType(shreddingSchema));
            } else {
                newFields.add(field);
            }
        }
        return new RowType(rowType.isNullable(), newFields);
    }

    public static List<List<VariantAccessInfo.VariantField>> buildVariantFields(
            DataField[] readFields, @Nullable VariantAccessInfo[] variantAccess) {
        HashMap<String, List<VariantAccessInfo.VariantField>> map = new HashMap<>();
        if (variantAccess != null) {
            for (VariantAccessInfo accessInfo : variantAccess) {
                map.put(accessInfo.columnName(), accessInfo.variantFields());
            }
        }
        List<List<VariantAccessInfo.VariantField>> variantFields = new ArrayList<>();
        for (DataField readField : readFields) {
            variantFields.add(map.getOrDefault(readField.name(), null));
        }
        return variantFields;
    }
}
