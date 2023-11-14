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

package org.apache.paimon.flink.action.cdc.rabbitmq;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConsumerCancelledException;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Delivery;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.ShutdownSignalException;
import com.rabbitmq.utility.Utility;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class QueueingConsumer extends DefaultConsumer {

    private final Connection connection;
    private final Channel channel;
    private final BlockingQueue<Delivery> queue;
    private volatile ShutdownSignalException shutdown;
    private volatile ConsumerCancelledException cancelled;
    private static final Delivery POISON = new Delivery(null, null, null);

    public QueueingConsumer(Connection connection, Channel channel) {
        this(connection, channel, Integer.MAX_VALUE);
    }

    public QueueingConsumer(Connection connection, Channel channel, int capacity) {
        super(channel);
        this.connection = connection;
        this.channel = channel;
        this.queue = new LinkedBlockingQueue(capacity);
    }

    private void checkShutdown() {
        if (this.shutdown != null) {
            throw Utility.fixStackTrace(this.shutdown);
        }
    }

    private Delivery handle(Delivery delivery) {
        if (delivery == POISON
                || delivery == null && (this.shutdown != null || this.cancelled != null)) {
            if (delivery == POISON) {
                this.queue.add(POISON);
                if (this.shutdown == null && this.cancelled == null) {
                    throw new IllegalStateException(
                            "POISON in queue, but null shutdown and null cancelled. This should never happen, please report as a BUG");
                }
            }

            if (null != this.shutdown) {
                throw Utility.fixStackTrace(this.shutdown);
            }

            if (null != this.cancelled) {
                throw Utility.fixStackTrace(this.cancelled);
            }
        }

        return delivery;
    }

    public Delivery nextDelivery(long timeout, TimeUnit unit)
            throws InterruptedException, ShutdownSignalException, ConsumerCancelledException {
        return this.handle(this.queue.poll(timeout, unit));
    }

    public void handleShutdownSignal(String consumerTag, ShutdownSignalException sig) {
        this.shutdown = sig;
        this.queue.add(POISON);
    }

    public void handleCancel(String consumerTag) {
        this.cancelled = new ConsumerCancelledException();
        this.queue.add(POISON);
    }

    public void handleDelivery(
            String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) {
        this.checkShutdown();
        this.queue.add(new Delivery(envelope, properties, body));
    }

    public void close() throws IOException, TimeoutException {
        if (channel != null) {
            channel.close();
        }
        if (connection != null) {
            connection.close();
        }
    }
}
