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

package org.apache.paimon.service.network;

import org.apache.paimon.service.messages.KvRequest;
import org.apache.paimon.service.messages.KvResponse;
import org.apache.paimon.service.messages.KvResponseTest;
import org.apache.paimon.service.network.messages.MessageBody;
import org.apache.paimon.service.network.messages.MessageSerializer;

import org.apache.paimon.shade.netty4.io.netty.buffer.ByteBuf;
import org.apache.paimon.shade.netty4.io.netty.channel.embedded.EmbeddedChannel;

import org.junit.jupiter.api.Test;

import java.nio.channels.ClosedChannelException;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link ClientHandler}. */
class ClientHandlerTest {

    /**
     * Tests that on reads the expected callback methods are called and read buffers are recycled.
     */
    @Test
    void testReadCallbacksAndBufferRecycling() throws Exception {
        final TestingClientHandlerCallback callback = new TestingClientHandlerCallback();

        final MessageSerializer<KvRequest, KvResponse> serializer =
                new MessageSerializer<>(
                        new KvRequest.KvRequestDeserializer(),
                        new KvResponse.KvResponseDeserializer());
        @SuppressWarnings({"unchecked", "rawtypes"})
        final EmbeddedChannel channel =
                new EmbeddedChannel(new ClientHandler(serializer, callback));

        final KvResponse response = KvResponseTest.random();

        //
        // Request success
        //
        ByteBuf buf = MessageSerializer.serializeResponse(channel.alloc(), 1222112277L, response);
        buf.skipBytes(4); // skip frame length

        // Verify callback
        callback.reset();
        channel.writeInbound(buf);
        assertThat(callback.onRequestCnt).isEqualTo(1);
        assertThat(callback.onRequestId).isEqualTo(1222112277L);
        assertThat(callback.onRequestBody).isInstanceOf(KvResponse.class);
        assertThat(buf.refCnt()).isEqualTo(0);
        //
        // Request failure
        //
        buf =
                MessageSerializer.serializeRequestFailure(
                        channel.alloc(),
                        1222112278,
                        new RuntimeException("Expected test Exception"));
        buf.skipBytes(4); // skip frame length

        // Verify callback
        callback.reset();
        channel.writeInbound(buf);
        assertThat(callback.onRequestFailureCnt).isEqualTo(1);
        assertThat(callback.onRequestFailureId).isEqualTo(1222112278L);
        assertThat(callback.onRequestFailureBody).isInstanceOf(RuntimeException.class);
        assertThat(buf.refCnt()).isEqualTo(0);

        //
        // Server failure
        //
        buf =
                MessageSerializer.serializeServerFailure(
                        channel.alloc(), new RuntimeException("Expected test Exception"));
        buf.skipBytes(4); // skip frame length

        // Verify callback
        callback.reset();
        channel.writeInbound(buf);
        assertThat(callback.onFailureCnt).isEqualTo(1);
        assertThat(callback.onFailureBody).isInstanceOf(RuntimeException.class);

        //
        // Unexpected messages
        //
        buf = channel.alloc().buffer(4).writeInt(1223823);

        // Verify callback
        callback.reset();
        channel.writeInbound(buf);
        assertThat(callback.onFailureCnt).isEqualTo(1);
        assertThat(callback.onFailureBody).isInstanceOf(RuntimeException.class);
        assertThat(buf.refCnt()).isEqualTo(0);

        //
        // Exception caught
        //
        callback.reset();
        channel.pipeline().fireExceptionCaught(new RuntimeException("Expected test Exception"));
        assertThat(callback.onFailureCnt).isEqualTo(1);
        assertThat(callback.onFailureBody).isInstanceOf(RuntimeException.class);

        //
        // Channel inactive
        //
        callback.reset();
        channel.pipeline().fireChannelInactive();
        assertThat(callback.onFailureCnt).isEqualTo(1);
        assertThat(callback.onFailureBody).isInstanceOf(ClosedChannelException.class);
    }

    @SuppressWarnings("rawtypes")
    private static class TestingClientHandlerCallback implements ClientHandlerCallback {

        private int onRequestCnt;
        private long onRequestId;
        private MessageBody onRequestBody;
        private int onRequestFailureCnt;
        private long onRequestFailureId;
        private Throwable onRequestFailureBody;
        private int onFailureCnt;
        private Throwable onFailureBody;

        @Override
        public void onRequestResult(long requestId, MessageBody response) {
            onRequestCnt++;
            onRequestId = requestId;
            onRequestBody = response;
        }

        @Override
        public void onRequestFailure(long requestId, Throwable cause) {
            onRequestFailureCnt++;
            onRequestFailureId = requestId;
            onRequestFailureBody = cause;
        }

        @Override
        public void onFailure(Throwable cause) {
            onFailureCnt++;
            onFailureBody = cause;
        }

        public void reset() {
            onRequestCnt = 0;
            onRequestId = -1;
            onRequestBody = null;
            onRequestFailureCnt = 0;
            onRequestFailureId = -1;
            onRequestFailureBody = null;
            onFailureCnt = 0;
            onFailureBody = null;
        }
    }
}
