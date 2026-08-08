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

package org.apache.paimon.service.messages;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.service.exceptions.GlobalIndexQueryException;

import org.apache.paimon.shade.netty4.io.netty.buffer.Unpooled;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import static org.apache.paimon.service.exceptions.GlobalIndexQueryErrorCode.INVALID_REQUEST;
import static org.apache.paimon.service.exceptions.GlobalIndexQueryErrorCode.OVERLOADED;
import static org.apache.paimon.service.exceptions.GlobalIndexQueryErrorCode.UNSUPPORTED_PROTOCOL;
import static org.apache.paimon.service.messages.KvRequestTest.row;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for the snapshot-fenced {@link GlobalIndexResponse} wire payload. */
class GlobalIndexResponseTest {

    @Test
    void testSerializationWithMiss() {
        GlobalIndexResponse response =
                new GlobalIndexResponse(
                        "server-epoch", 9L, 101L, new BinaryRow[] {row(1), null, row(3)});

        GlobalIndexResponse deserialized =
                new GlobalIndexResponse.Deserializer()
                        .deserializeMessage(Unpooled.wrappedBuffer(response.serialize()));

        assertThat(deserialized).isEqualTo(response);
        assertThat(deserialized.protocolVersion()).isEqualTo(GlobalIndexRequest.PROTOCOL_VERSION);
        assertThat(deserialized.isSuccess()).isTrue();
        assertThat(deserialized.serverEpoch()).isEqualTo("server-epoch");
        assertThat(deserialized.servedGeneration()).isEqualTo(9L);
        assertThat(deserialized.servedSnapshotId()).isEqualTo(101L);
        assertThat(deserialized.values()[0].getOffset()).isZero();
        assertThat(deserialized.values()[0].anyNull()).isFalse();
        assertThat(deserialized.values()[2].getOffset()).isZero();
        assertThat(deserialized.values()[2].anyNull()).isFalse();
    }

    @Test
    void testStructuredFailureRoundTripAndBoundedMessage() {
        char[] messageChars = new char[GlobalIndexResponse.MAX_ERROR_MESSAGE_BYTES * 2];
        Arrays.fill(messageChars, 'x');
        GlobalIndexResponse response =
                GlobalIndexResponse.failure(
                        "server-epoch", 9L, 101L, OVERLOADED, true, new String(messageChars));

        GlobalIndexResponse deserialized =
                new GlobalIndexResponse.Deserializer()
                        .deserializeMessage(Unpooled.wrappedBuffer(response.serialize()));

        assertThat(deserialized.isSuccess()).isFalse();
        assertThat(deserialized.errorCode()).isEqualTo(OVERLOADED);
        assertThat(deserialized.retryable()).isTrue();
        assertThat(deserialized.errorMessage().getBytes(StandardCharsets.UTF_8))
                .hasSize(GlobalIndexResponse.MAX_ERROR_MESSAGE_BYTES);
        assertThat(deserialized.values()).isEmpty();
        assertThat(deserialized.toException())
                .isInstanceOfSatisfying(
                        GlobalIndexQueryException.class,
                        failure -> {
                            assertThat(failure.errorCode()).isEqualTo(OVERLOADED);
                            assertThat(failure.retryable()).isTrue();
                        });
    }

    @Test
    void testRejectUnsupportedProtocolAndInvalidNullMarker() {
        byte[] unsupported =
                new GlobalIndexResponse("epoch", 1L, 2L, new BinaryRow[] {null}).serialize();
        ByteBuffer.wrap(unsupported).putInt(GlobalIndexRequest.PROTOCOL_VERSION + 1);
        assertStructuredFailure(unsupported, UNSUPPORTED_PROTOCOL);

        byte[] invalidMarker =
                new GlobalIndexResponse("epoch", 1L, 2L, new BinaryRow[] {null}).serialize();
        invalidMarker[invalidMarker.length - 1] = 2;
        assertStructuredFailure(invalidMarker, INVALID_REQUEST);
    }

    @Test
    void testRejectMalformedAndTrailingPayload() {
        assertStructuredFailure(new byte[] {0}, INVALID_REQUEST);

        byte[] response =
                new GlobalIndexResponse("epoch", 1L, 2L, new BinaryRow[] {row(1)}).serialize();
        assertStructuredFailure(Arrays.copyOf(response, response.length + 1), INVALID_REQUEST);
    }

    @Test
    void testRejectUnknownErrorCodeAndInvalidRetryableMarker() {
        byte[] response =
                GlobalIndexResponse.failure("epoch", 1L, 2L, OVERLOADED, "busy").serialize();
        int statusOffset =
                Integer.BYTES
                        + Integer.BYTES
                        + "epoch".getBytes(StandardCharsets.UTF_8).length
                        + Long.BYTES
                        + Long.BYTES;

        byte[] unknownCode = Arrays.copyOf(response, response.length);
        ByteBuffer.wrap(unknownCode).putInt(statusOffset, Integer.MAX_VALUE);
        assertStructuredFailure(unknownCode, INVALID_REQUEST);

        byte[] invalidRetryable = Arrays.copyOf(response, response.length);
        invalidRetryable[statusOffset + Integer.BYTES] = 2;
        assertStructuredFailure(invalidRetryable, INVALID_REQUEST);
    }

    private static void assertStructuredFailure(
            byte[] payload,
            org.apache.paimon.service.exceptions.GlobalIndexQueryErrorCode expectedCode) {
        assertThatThrownBy(
                        () ->
                                new GlobalIndexResponse.Deserializer()
                                        .deserializeMessage(Unpooled.wrappedBuffer(payload)))
                .isInstanceOfSatisfying(
                        GlobalIndexQueryException.class,
                        e -> assertThat(e.errorCode()).isEqualTo(expectedCode));
    }
}
