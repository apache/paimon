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

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.shredding.ShreddingWritePlan;
import org.apache.paimon.format.FormatWriter;
import org.apache.paimon.format.FormatWriterFactory;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link ShreddingWritePlanWriterFactory}. */
class ShreddingWritePlanWriterFactoryTest {

    @Test
    void testRejectUnsupportedDelegateForActiveWritePlan() {
        RowType logicalType = DataTypes.ROW(DataTypes.FIELD(0, "id", DataTypes.INT()));
        ShreddingWritePlanWriterFactory factory =
                new ShreddingWritePlanWriterFactory(
                        new UnsupportedDelegateWriterFactory(),
                        new ActiveWritePlanFactory(logicalType));

        assertThatThrownBy(() -> factory.create(null, "zstd"))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining(
                        "Delegate writer factory does not support shredding write plans");
    }

    private static class UnsupportedDelegateWriterFactory implements FormatWriterFactory {

        @Override
        public FormatWriter create(PositionOutputStream out, String compression) {
            throw new AssertionError("Unsupported delegate should fail before create is called.");
        }
    }

    private static class ActiveWritePlanFactory implements ShreddingWritePlanFactory {

        private final RowType logicalType;

        private ActiveWritePlanFactory(RowType logicalType) {
            this.logicalType = logicalType;
        }

        @Override
        public RowType logicalRowType() {
            return logicalType;
        }

        @Override
        public boolean shouldCreateWritePlan() {
            return true;
        }

        @Override
        public boolean shouldInferWritePlan() {
            return false;
        }

        @Override
        public int inferBufferRowCount() {
            return 0;
        }

        @Override
        public ShreddingWritePlan createWritePlan(List<InternalRow> sampleRows) {
            throw new AssertionError("Write plan should not be created for unsupported delegate.");
        }
    }
}
