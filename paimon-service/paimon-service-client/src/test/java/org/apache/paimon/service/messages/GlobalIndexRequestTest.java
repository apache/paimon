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
import org.apache.paimon.data.BinaryRowWriter;
import org.apache.paimon.service.exceptions.GlobalIndexQueryException;
import org.apache.paimon.types.RowKind;

import org.apache.paimon.shade.netty4.io.netty.buffer.Unpooled;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import static org.apache.paimon.service.exceptions.GlobalIndexQueryErrorCode.INVALID_REQUEST;
import static org.apache.paimon.service.exceptions.GlobalIndexQueryErrorCode.REQUEST_TOO_LARGE;
import static org.apache.paimon.service.exceptions.GlobalIndexQueryErrorCode.UNSUPPORTED_PROTOCOL;
import static org.apache.paimon.service.messages.KvRequestTest.row;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for the versioned and bounded {@link GlobalIndexRequest} wire payload. */
class GlobalIndexRequestTest {

    @Test
    void testSerialization() {
        GlobalIndexRequest request =
                new GlobalIndexRequest("server-epoch", 17L, new BinaryRow[] {row(1), row(2)});

        GlobalIndexRequest deserialized =
                new GlobalIndexRequest.Deserializer()
                        .deserializeMessage(Unpooled.wrappedBuffer(request.serialize()));

        assertThat(deserialized).isEqualTo(request);
        assertThat(deserialized.protocolVersion()).isEqualTo(GlobalIndexRequest.PROTOCOL_VERSION);
        assertThat(deserialized.serverEpoch()).isEqualTo("server-epoch");
        assertThat(deserialized.servedGeneration()).isEqualTo(17L);
        assertThat(deserialized.keys())
                .allSatisfy(
                        key -> {
                            assertThat(key.getOffset()).isZero();
                            assertThat(key.anyNull()).isFalse();
                        });
    }

    @Test
    void testCanonicalizesRowKindAndCopiesCallerKeys() {
        BinaryRow insert = row(7);
        BinaryRow delete = row(7);
        delete.setRowKind(RowKind.DELETE);
        BinaryRow update = row(7);
        update.setRowKind(RowKind.UPDATE_AFTER);

        GlobalIndexRequest insertRequest =
                new GlobalIndexRequest("epoch", 1L, new BinaryRow[] {insert});
        GlobalIndexRequest deleteRequest =
                new GlobalIndexRequest("epoch", 1L, new BinaryRow[] {delete});
        GlobalIndexRequest updateRequest =
                new GlobalIndexRequest("epoch", 1L, new BinaryRow[] {update});

        assertThat(deleteRequest).isEqualTo(insertRequest).isEqualTo(updateRequest);
        assertThat(deleteRequest.serialize())
                .containsExactly(insertRequest.serialize())
                .containsExactly(updateRequest.serialize());
        assertThat(deleteRequest.keys()[0]).isNotSameAs(delete);
        assertThat(deleteRequest.keys()[0].getRowKind()).isEqualTo(RowKind.INSERT);
        assertThat(delete.getRowKind()).isEqualTo(RowKind.DELETE);
    }

    @Test
    void testRejectUnsupportedProtocol() {
        byte[] bytes = new GlobalIndexRequest("epoch", 1L, new BinaryRow[] {row(1)}).serialize();
        ByteBuffer.wrap(bytes).putInt(GlobalIndexRequest.PROTOCOL_VERSION + 1);

        assertStructuredFailure(bytes, UNSUPPORTED_PROTOCOL);
    }

    @Test
    void testRejectTrailingBytes() {
        byte[] bytes = new GlobalIndexRequest("epoch", 1L, new BinaryRow[] {row(1)}).serialize();
        assertStructuredFailure(Arrays.copyOf(bytes, bytes.length + 1), INVALID_REQUEST);
    }

    @Test
    void testRejectMalformedAndNullKeyRows() {
        assertStructuredFailure(new byte[] {0}, INVALID_REQUEST);

        byte[] bytes = new GlobalIndexRequest("epoch", 1L, new BinaryRow[] {row(1)}).serialize();
        int keyArityOffset =
                Integer.BYTES
                        + Integer.BYTES
                        + "epoch".getBytes(StandardCharsets.UTF_8).length
                        + Long.BYTES
                        + Integer.BYTES
                        + Integer.BYTES;
        ByteBuffer.wrap(bytes).putInt(keyArityOffset, 2);
        assertStructuredFailure(bytes, INVALID_REQUEST);

        BinaryRow nullKey = new BinaryRow(1);
        BinaryRowWriter writer = new BinaryRowWriter(nullKey);
        writer.setNullAt(0);
        writer.complete();
        assertThatThrownBy(() -> new GlobalIndexRequest("epoch", 1L, new BinaryRow[] {nullKey}))
                .isInstanceOfSatisfying(
                        GlobalIndexQueryException.class,
                        e -> assertThat(e.errorCode()).isEqualTo(INVALID_REQUEST));
    }

    @Test
    void testRejectEmptyAndTooManyKeys() {
        assertThatThrownBy(() -> new GlobalIndexRequest("epoch", 1L, new BinaryRow[0]))
                .isInstanceOfSatisfying(
                        GlobalIndexQueryException.class,
                        e -> assertThat(e.errorCode()).isEqualTo(INVALID_REQUEST));
        assertThatThrownBy(() -> new GlobalIndexRequest("epoch", 1L, new BinaryRow[] {null}))
                .isInstanceOfSatisfying(
                        GlobalIndexQueryException.class,
                        e -> assertThat(e.errorCode()).isEqualTo(INVALID_REQUEST));

        BinaryRow[] keys = new BinaryRow[GlobalIndexRequest.MAX_KEYS + 1];
        Arrays.fill(keys, row(1));
        assertThatThrownBy(() -> new GlobalIndexRequest("epoch", 1L, keys))
                .isInstanceOfSatisfying(
                        GlobalIndexQueryException.class,
                        e -> assertThat(e.errorCode()).isEqualTo(REQUEST_TOO_LARGE));
    }

    @Test
    void testRejectOversizedEpochAndKeyBeforeAllocation() {
        char[] epochChars = new char[GlobalIndexRequest.MAX_SERVER_EPOCH_BYTES + 1];
        Arrays.fill(epochChars, 'e');
        assertThatThrownBy(
                        () ->
                                new GlobalIndexRequest(
                                        new String(epochChars), 1L, new BinaryRow[] {row(1)}))
                .isInstanceOfSatisfying(
                        GlobalIndexQueryException.class,
                        e -> assertThat(e.errorCode()).isEqualTo(REQUEST_TOO_LARGE));
        assertStructuredFailure(
                ByteBuffer.allocate(2 * Integer.BYTES)
                        .putInt(GlobalIndexRequest.PROTOCOL_VERSION)
                        .putInt(GlobalIndexRequest.MAX_SERVER_EPOCH_BYTES + 1)
                        .array(),
                REQUEST_TOO_LARGE);

        byte[] epoch = "epoch".getBytes(StandardCharsets.UTF_8);
        ByteBuffer payload =
                ByteBuffer.allocate(
                                Integer.BYTES
                                        + Integer.BYTES
                                        + epoch.length
                                        + Long.BYTES
                                        + Integer.BYTES
                                        + Integer.BYTES)
                        .putInt(GlobalIndexRequest.PROTOCOL_VERSION)
                        .putInt(epoch.length)
                        .put(epoch)
                        .putLong(1L)
                        .putInt(1)
                        .putInt(GlobalIndexRequest.MAX_TOTAL_KEY_BYTES + 1);
        assertStructuredFailure(payload.array(), REQUEST_TOO_LARGE);
    }

    private static void assertStructuredFailure(
            byte[] payload,
            org.apache.paimon.service.exceptions.GlobalIndexQueryErrorCode expectedCode) {
        assertThatThrownBy(
                        () ->
                                new GlobalIndexRequest.Deserializer()
                                        .deserializeMessage(Unpooled.wrappedBuffer(payload)))
                .isInstanceOfSatisfying(
                        GlobalIndexQueryException.class,
                        e -> assertThat(e.errorCode()).isEqualTo(expectedCode));
    }
}
