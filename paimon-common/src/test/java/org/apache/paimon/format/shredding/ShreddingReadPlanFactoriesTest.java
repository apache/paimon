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
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link ShreddingReadPlanFactories}. */
class ShreddingReadPlanFactoriesTest {

    @Test
    void testRejectMultipleActiveReadPlans() {
        RowType logicalType = DataTypes.ROW(DataTypes.FIELD(0, "id", DataTypes.INT()));

        assertThatThrownBy(
                        () ->
                                ShreddingReadPlanFactories.createReadPlan(
                                        logicalType,
                                        Collections.emptyMap(),
                                        null,
                                        Arrays.asList(
                                                new ActiveReadPlanFactory(logicalType),
                                                new ActiveReadPlanFactory(logicalType))))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining(
                        "Composing multiple active shredding read plans is not supported.");
    }

    private static class ActiveReadPlanFactory implements ShreddingReadPlanFactory {

        private final RowType logicalType;

        private ActiveReadPlanFactory(RowType logicalType) {
            this.logicalType = logicalType;
        }

        @Override
        public RowType logicalRowType() {
            return logicalType;
        }

        @Override
        public boolean shouldCreateReadPlan(
                Map<String, Map<String, String>> fieldMetadata, Object fileSchema) {
            return true;
        }

        @Override
        public ShreddingReadPlan createReadPlan(
                Map<String, Map<String, String>> fieldMetadata, Object fileSchema) {
            return new ActiveReadPlan(logicalType);
        }
    }

    private static class ActiveReadPlan implements ShreddingReadPlan {

        private final RowType logicalType;
        private final RowType physicalType;

        private ActiveReadPlan(RowType logicalType) {
            this.logicalType = logicalType;
            this.physicalType = DataTypes.ROW(DataTypes.FIELD(0, "id", DataTypes.BIGINT()));
        }

        @Override
        public RowType logicalRowType() {
            return logicalType;
        }

        @Override
        public RowType physicalRowType() {
            return physicalType;
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
