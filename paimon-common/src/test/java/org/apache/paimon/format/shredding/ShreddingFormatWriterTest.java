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
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link ShreddingFormatWriter}. */
class ShreddingFormatWriterTest {

    @Test
    void testCloseDelegateWhenCommitMetadataFails() {
        IOException failure = new IOException("metadata failed");
        TestingFormatWriter delegate = new TestingFormatWriter();
        ShreddingFormatWriter writer =
                new ShreddingFormatWriter(
                        delegate,
                        new ThrowingMetadataFactory(failure),
                        NoOpWritePlan.INSTANCE,
                        "none");

        assertThatThrownBy(writer::close).isSameAs(failure);
        assertThat(delegate.closed).isTrue();
    }

    private static class TestingFormatWriter implements FormatWriter {

        private boolean closed;

        @Override
        public void addElement(InternalRow element) {}

        @Override
        public boolean reachTargetSize(boolean suggestedCheck, long targetSize) {
            return false;
        }

        @Override
        public void close() {
            closed = true;
        }
    }

    private static class ThrowingMetadataFactory implements SupportsShreddingWritePlan {

        private final IOException failure;

        private ThrowingMetadataFactory(IOException failure) {
            this.failure = failure;
        }

        @Override
        public FormatWriter createWithShreddingWritePlan(
                PositionOutputStream out, String compression, ShreddingWritePlan writePlan) {
            return new TestingFormatWriter();
        }

        @Override
        public void commitShreddingMetadata(
                FormatWriter writer, ShreddingWritePlan writePlan, String compression)
                throws IOException {
            throw failure;
        }
    }

    private enum NoOpWritePlan implements ShreddingWritePlan {
        INSTANCE;

        private final RowType rowType = new RowType(Collections.emptyList());

        @Override
        public RowType logicalRowType() {
            return rowType;
        }

        @Override
        public RowType physicalRowType() {
            return rowType;
        }

        @Override
        public InternalRow toPhysicalRow(InternalRow row) {
            return row;
        }
    }
}
