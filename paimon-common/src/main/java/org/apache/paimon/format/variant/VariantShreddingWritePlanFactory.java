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
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.shredding.ShreddingWritePlan;
import org.apache.paimon.data.variant.InferVariantShreddingSchema;
import org.apache.paimon.data.variant.VariantShreddingWritePlan;
import org.apache.paimon.format.shredding.ShreddingWritePlanFactory;
import org.apache.paimon.options.Options;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.VariantType;
import org.apache.paimon.utils.JsonSerdeUtil;

import java.util.List;

/** Creates Variant shredding write plans from configured or inferred schemas. */
public class VariantShreddingWritePlanFactory implements ShreddingWritePlanFactory {

    private final RowType rowType;
    private final Options options;

    public VariantShreddingWritePlanFactory(RowType rowType, Options options) {
        this.rowType = rowType;
        this.options = options;
    }

    @Override
    public RowType logicalRowType() {
        return rowType;
    }

    @Override
    public boolean shouldCreateWritePlan() {
        return hasConfiguredShreddingSchema() || shouldInferWritePlan();
    }

    @Override
    public boolean shouldInferWritePlan() {
        if (hasConfiguredShreddingSchema()) {
            return false;
        }

        if (!options.get(CoreOptions.VARIANT_INFER_SHREDDING_SCHEMA)) {
            return false;
        }

        return containsVariantFields(rowType);
    }

    @Override
    public int inferBufferRowCount() {
        return options.get(CoreOptions.VARIANT_SHREDDING_MAX_INFER_BUFFER_ROW);
    }

    @Override
    public ShreddingWritePlan createWritePlan(List<InternalRow> sampleRows) {
        if (hasConfiguredShreddingSchema()) {
            return VariantShreddingWritePlan.fromConfiguredSchema(
                    rowType, configuredShreddingSchema());
        }

        RowType physicalRowType = createInferrer().inferSchema(sampleRows);
        return new VariantShreddingWritePlan(rowType, physicalRowType);
    }

    private boolean hasConfiguredShreddingSchema() {
        return options.contains(CoreOptions.VARIANT_SHREDDING_SCHEMA);
    }

    private RowType configuredShreddingSchema() {
        String shreddingSchema = options.get(CoreOptions.VARIANT_SHREDDING_SCHEMA);
        return (RowType) JsonSerdeUtil.fromJson(shreddingSchema, DataType.class);
    }

    private InferVariantShreddingSchema createInferrer() {
        return new InferVariantShreddingSchema(
                rowType,
                options.get(CoreOptions.VARIANT_SHREDDING_MAX_SCHEMA_WIDTH),
                options.get(CoreOptions.VARIANT_SHREDDING_MAX_SCHEMA_DEPTH),
                options.get(CoreOptions.VARIANT_SHREDDING_MIN_FIELD_CARDINALITY_RATIO));
    }

    private boolean containsVariantFields(RowType rowType) {
        for (DataField field : rowType.getFields()) {
            if (field.type() instanceof VariantType) {
                return true;
            }
            if (field.type() instanceof RowType && containsVariantFields((RowType) field.type())) {
                return true;
            }
        }
        return false;
    }
}
