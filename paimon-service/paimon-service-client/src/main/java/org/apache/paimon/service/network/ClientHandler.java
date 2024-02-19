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

import org.apache.paimon.service.network.messages.MessageBody;
import org.apache.paimon.service.network.messages.MessageSerializer;
import org.apache.paimon.service.network.messages.MessageType;
import org.apache.paimon.service.network.messages.RequestFailure;
import org.apache.paimon.utils.Preconditions;

import org.apache.paimon.shade.netty4.io.netty.buffer.ByteBuf;
import org.apache.paimon.shade.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.paimon.shade.netty4.io.netty.channel.ChannelInboundHandlerAdapter;
import org.apache.paimon.shade.netty4.io.netty.util.ReferenceCountUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.channels.ClosedChannelException;

/**
 * The handler used by a {@link NetworkClient} to handling incoming messages.
 *
 * @param <REQ> the type of request the client will send.
 * @param <RESP> the type of response the client expects to receive.
 */
public class ClientHandler<REQ extends MessageBody, RESP extends MessageBody>
        extends ChannelInboundHandlerAdapter {

    private static final Logger LOG = LoggerFactory.getLogger(ClientHandler.class);

    private final MessageSerializer<REQ, RESP> serializer;

    private final ClientHandlerCallback<RESP> callback;

    /**
     * Creates a handler with the callback.
     *
     * @param serializer the serializer used to (de-)serialize messages.
     * @param callback Callback for responses.
     */
    public ClientHandler(
            final MessageSerializer<REQ, RESP> serializer,
            final ClientHandlerCallback<RESP> callback) {
        this.serializer = Preconditions.checkNotNull(serializer);
        this.callback = Preconditions.checkNotNull(callback);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        try {
            ByteBuf buf = (ByteBuf) msg;
            MessageType msgType = MessageSerializer.deserializeHeader(buf);

            if (msgType == MessageType.REQUEST_RESULT) {
                long requestId = MessageSerializer.getRequestId(buf);
                RESP result = serializer.deserializeResponse(buf);
                callback.onRequestResult(requestId, result);
            } else if (msgType == MessageType.REQUEST_FAILURE) {
                RequestFailure failure = MessageSerializer.deserializeRequestFailure(buf);
                callback.onRequestFailure(failure.getRequestId(), failure.getCause());
            } else if (msgType == MessageType.SERVER_FAILURE) {
                throw MessageSerializer.deserializeServerFailure(buf);
            } else {
                throw new IllegalStateException("Unexpected response type '" + msgType + "'");
            }
        } catch (Throwable t1) {
            try {
                callback.onFailure(t1);
            } catch (Throwable t2) {
                LOG.error("Failed to notify callback about failure", t2);
            }
        } finally {
            ReferenceCountUtil.release(msg);
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        try {
            callback.onFailure(cause);
        } catch (Throwable t) {
            LOG.error("Failed to notify callback about failure", t);
        }
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        // Only the client is expected to close the channel. Otherwise it
        // indicates a failure. Note that this will be invoked in both cases
        // though. If the callback closed the channel, the callback must be
        // ignored.
        try {
            callback.onFailure(new ClosedChannelException());
        } catch (Throwable t) {
            LOG.error("Failed to notify callback about failure", t);
        }
    }
}
