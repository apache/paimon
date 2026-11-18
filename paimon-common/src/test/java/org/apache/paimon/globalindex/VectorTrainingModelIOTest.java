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

package org.apache.paimon.globalindex;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Persisted envelope version, model identity and length validation. */
class VectorTrainingModelIOTest {
    @Test
    void rejectsPayloadOverReaderLimitBeforeAllocation() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        VectorTrainingModelIO.write(out, new byte[] {1, 2}, "test");
        byte[] bytes = out.toByteArray();
        assertThatThrownBy(
                        () ->
                                VectorTrainingModelIO.read(
                                        new ByteArrayInputStream(bytes), bytes.length, "test", 1))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("payload length");
    }

    @Test
    void testPayloadRoundTripAndModelIdentity() throws Exception {
        byte[] payload = {1, 2, 3};
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        VectorTrainingModelIO.write(out, payload, "model-a");
        byte[] bytes = out.toByteArray();
        assertThat(
                        VectorTrainingModelIO.read(
                                new ByteArrayInputStream(bytes), bytes.length, "model-a"))
                .containsExactly(payload);
        assertThatThrownBy(
                        () ->
                                VectorTrainingModelIO.read(
                                        new ByteArrayInputStream(bytes), bytes.length, "model-b"))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("mismatched");
        String encoded = new String(bytes, StandardCharsets.ISO_8859_1);
        byte[] unsupported =
                encoded.replace("\"version\":1", "\"version\":2")
                        .getBytes(StandardCharsets.ISO_8859_1);
        assertThatThrownBy(
                        () ->
                                VectorTrainingModelIO.read(
                                        new ByteArrayInputStream(unsupported),
                                        unsupported.length,
                                        "model-a"))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Unsupported");
    }

    @Test
    void testInvalidLengthsFailBeforePayloadAllocation() throws Exception {
        byte[] invalid = {-1, -1, -1, -1};
        assertThatThrownBy(
                        () ->
                                VectorTrainingModelIO.read(
                                        new ByteArrayInputStream(invalid), 4, "model"))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("length");
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        VectorTrainingModelIO.write(out, new byte[] {1}, "model");
        byte[] bytes = out.toByteArray();
        assertThatThrownBy(
                        () ->
                                VectorTrainingModelIO.read(
                                        new ByteArrayInputStream(bytes),
                                        (long) Integer.MAX_VALUE + 65536,
                                        "model"))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("payload length");
        assertThatThrownBy(
                        () ->
                                VectorTrainingModelIO.read(
                                        new ByteArrayInputStream(bytes), bytes.length - 1, "model"))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("payload length");
    }
}
