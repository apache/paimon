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

package org.apache.paimon.format.variant;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.variant.InferVariantShreddingSchema;
import org.apache.paimon.options.Options;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.VariantType;

/** Variant schema inference configuration. */
public class VariantInferenceConfig {

    private final RowType rowType;
    private final Options options;

    public VariantInferenceConfig(RowType rowType, Options options) {
        this.rowType = rowType;
        this.options = options;
    }

    /** Determines whether variant schema inference should be enabled. */
    public boolean shouldEnableInference() {
        if (options.contains(CoreOptions.VARIANT_SHREDDING_SCHEMA)) {
            return false;
        }

        if (!options.get(CoreOptions.VARIANT_INFER_SHREDDING_SCHEMA)) {
            return false;
        }

        return containsVariantFields(rowType);
    }

    private boolean containsVariantFields(RowType rowType) {
        for (DataField field : rowType.getFields()) {
            if (field.type() instanceof VariantType) {
                return true;
            }
        }
        return false;
    }

    /** Create a schema inferrer. */
    public InferVariantShreddingSchema createInferrer() {
        return new InferVariantShreddingSchema(
                rowType,
                options.get(CoreOptions.VARIANT_SHREDDING_MAX_SCHEMA_WIDTH),
                options.get(CoreOptions.VARIANT_SHREDDING_MAX_SCHEMA_DEPTH),
                options.get(CoreOptions.VARIANT_SHREDDING_MIN_FIELD_CARDINALITY_RATIO));
    }

    /** Get the maximum number of rows to buffer for inference. */
    public int getMaxBufferRow() {
        return options.get(CoreOptions.VARIANT_SHREDDING_MAX_INFER_BUFFER_ROW);
    }

    public RowType rowType() {
        return rowType;
    }
}
