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

package org.apache.paimon.format.shredding;

import org.apache.paimon.data.columnar.VectorizedColumnBatch;
import org.apache.paimon.data.shredding.ShreddingBatchAssembler;
import org.apache.paimon.data.shredding.ShreddingReadPlan;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Preconditions;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/** Helpers for composing per-file shredding read plans. */
public class ShreddingReadPlanFactories {

    private ShreddingReadPlanFactories() {}

    public static ShreddingReadPlan createReadPlan(
            RowType logicalRowType,
            Map<String, Map<String, String>> fieldMetadata,
            @Nullable Object fileSchema,
            List<ShreddingReadPlanFactory> factories) {
        List<ShreddingReadPlan> activePlans = new ArrayList<>();
        for (ShreddingReadPlanFactory factory : factories) {
            Preconditions.checkArgument(
                    logicalRowType.equals(factory.logicalRowType()),
                    "Shredding read plan factory logical row type does not match.");
            if (!factory.shouldCreateReadPlan(fieldMetadata, fileSchema)) {
                continue;
            }
            ShreddingReadPlan readPlan = factory.createReadPlan(fieldMetadata, fileSchema);
            if (readPlan.isIdentity()) {
                continue;
            }
            activePlans.add(readPlan);
        }
        if (activePlans.isEmpty()) {
            return identity(logicalRowType);
        }
        if (activePlans.size() == 1) {
            return activePlans.get(0);
        }
        throw new UnsupportedOperationException(
                "Composing multiple active shredding read plans is not supported.");
    }

    public static ShreddingReadPlan identity(RowType rowType) {
        return new IdentityShreddingReadPlan(rowType);
    }

    private static class IdentityShreddingReadPlan implements ShreddingReadPlan {

        private final RowType rowType;

        private IdentityShreddingReadPlan(RowType rowType) {
            this.rowType = rowType;
        }

        @Override
        public RowType logicalRowType() {
            return rowType;
        }

        @Override
        public RowType physicalRowType() {
            return rowType;
        }

        @Override
        public ShreddingBatchAssembler batchAssembler() {
            return new ShreddingBatchAssembler() {
                @Override
                public VectorizedColumnBatch assemble(VectorizedColumnBatch physicalBatch) {
                    return physicalBatch;
                }
            };
        }
    }
}
