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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.variant.PaimonShreddingUtils;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.VariantType;

import java.util.ArrayList;
import java.util.List;

/** Utils for variant. */
public class VariantUtils {

    public static RowType setShreddingSchema(RowType rowType, CoreOptions options) {
        List<DataField> fields = rowType.getFields();
        List<DataField> newFields = new ArrayList<>(fields.size());
        for (DataField field : fields) {
            String shreddingSchema = options.shreddingSchema(field.name());
            if (field.type() instanceof VariantType && shreddingSchema != null) {
                VariantType variantType = (VariantType) field.type().copy();
                variantType.setShreddingSchema(
                        PaimonShreddingUtils.variantShreddingSchema(
                                (RowType) JsonSerdeUtil.fromJson(shreddingSchema, DataType.class)));
                newFields.add(field.newType(variantType));
            } else {
                newFields.add(field);
            }
        }
        return rowType.copy(newFields);
    }
}
