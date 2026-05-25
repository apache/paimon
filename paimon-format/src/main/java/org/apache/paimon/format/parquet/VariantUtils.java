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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.variant.PaimonShreddingUtils;
import org.apache.paimon.options.Options;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.VariantType;
import org.apache.paimon.utils.JsonSerdeUtil;

import org.apache.parquet.schema.Type;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;

import static org.apache.paimon.data.variant.Variant.METADATA;
import static org.apache.paimon.data.variant.Variant.VALUE;

/** Utils for variant. */
public class VariantUtils {

    /** For reader, extract variant file schema from each parquet file. */
    public static RowType variantFileType(Type fileType) {
        boolean isShredded =
                fileType.asGroupType().containsField(PaimonShreddingUtils.TYPED_VALUE_FIELD_NAME);
        if (isShredded) {
            return (RowType) ParquetSchemaConverter.convertToPaimonField(fileType).type();
        } else {
            List<DataField> dataFields = new ArrayList<>();
            dataFields.add(new DataField(0, VALUE, DataTypes.BYTES().notNull()));
            dataFields.add(new DataField(1, METADATA, DataTypes.BYTES().notNull()));
            return new RowType(dataFields);
        }
    }

    /** For writer, extract shredding schemas from conf. */
    @Nullable
    public static RowType shreddingSchemasFromOptions(Options options) {
        if (!options.contains(CoreOptions.VARIANT_SHREDDING_SCHEMA)) {
            return null;
        }

        String shreddingSchema = options.get(CoreOptions.VARIANT_SHREDDING_SCHEMA);
        RowType rowType = (RowType) JsonSerdeUtil.fromJson(shreddingSchema, DataType.class);
        ArrayList<DataField> fields = new ArrayList<>();
        for (DataField field : rowType.getFields()) {
            fields.add(field.newType(PaimonShreddingUtils.variantShreddingSchema(field.type())));
        }
        return new RowType(fields);
    }

    public static RowType replaceWithShreddingType(
            RowType rowType, @Nullable RowType shreddingSchemas) {
        if (shreddingSchemas == null) {
            return rowType;
        }

        List<DataField> newFields = new ArrayList<>();
        for (DataField field : rowType.getFields()) {
            // todo: support nested variant.
            if (field.type() instanceof VariantType
                    && shreddingSchemas.containsField(field.name())) {
                RowType shreddingSchema = (RowType) shreddingSchemas.getField(field.name()).type();
                newFields.add(field.newType(shreddingSchema));
            } else {
                newFields.add(field);
            }
        }
        return new RowType(rowType.isNullable(), newFields);
    }
}
