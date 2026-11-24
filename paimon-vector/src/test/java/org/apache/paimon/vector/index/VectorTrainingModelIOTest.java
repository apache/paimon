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

package org.apache.paimon.vector.index;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Length validation for native router file reads. */
class VectorTrainingModelIOTest {
    @Test
    void rejectsPayloadOverReaderLimitBeforeAllocation() {
        assertThatThrownBy(
                        () ->
                                VectorTrainingModelIO.read(
                                        new ByteArrayInputStream(new byte[0]), 2, 1))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("payload length");
    }

    @Test
    void readsRawPayload() throws IOException {
        byte[] payload = {1, 2, 3};
        assertThat(
                        VectorTrainingModelIO.read(
                                new ByteArrayInputStream(payload), payload.length, payload.length))
                .containsExactly(payload);
    }

    @Test
    void rejectsTruncatedPayload() {
        assertThatThrownBy(
                        () ->
                                VectorTrainingModelIO.read(
                                        new ByteArrayInputStream(new byte[] {1}), 2, 2))
                .isInstanceOf(EOFException.class);
    }

    @Test
    void rejectsInvalidFileSizes() {
        for (long fileSize : new long[] {-1, 0, (long) Integer.MAX_VALUE + 1}) {
            assertThatThrownBy(
                            () ->
                                    VectorTrainingModelIO.read(
                                            new ByteArrayInputStream(new byte[0]),
                                            fileSize,
                                            Integer.MAX_VALUE))
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("payload length");
        }
    }
}
