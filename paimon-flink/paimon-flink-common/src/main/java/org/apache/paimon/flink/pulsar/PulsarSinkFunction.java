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

package org.apache.paimon.flink.pulsar;

import org.apache.paimon.flink.pulsar.sink.FlinkPulsarSink;
import org.apache.paimon.flink.pulsar.sink.PulsarSinkSemantic;
import org.apache.paimon.flink.pulsar.sink.PulsarSinkSerializationSchema;
import org.apache.paimon.flink.sink.LogSinkFunction;
import org.apache.paimon.table.sink.SinkRecord;

import org.apache.flink.configuration.Configuration;
import org.apache.pulsar.client.api.MessageRouter;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.client.util.MessageIdUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.Properties;

/**
 * A {@link FlinkPulsarSink} which implements {@link LogSinkFunction} to register {@link
 * WriteCallback}.
 */
public class PulsarSinkFunction extends FlinkPulsarSink<SinkRecord> implements LogSinkFunction {

    private static final Logger LOG = LoggerFactory.getLogger(PulsarSinkFunction.class);

    private WriteCallback writeCallback;

    /**
     * Creates a {@link PulsarSinkFunction} for a given topic. The sink produces its input to the
     * topic. It accepts a {@link PulsarSinkSerializationSchema} for serializing records to , including
     * partitioning information.
     */
    public PulsarSinkFunction(
            String adminUrl,
            Optional<String> defaultTopicName,
            ClientConfigurationData clientConf,
            Properties properties,
            PulsarSinkSerializationSchema serializationSchema,
            MessageRouter messageRouter,
            PulsarSinkSemantic semantic) {
        super(
                adminUrl,
                defaultTopicName,
                clientConf,
                properties,
                serializationSchema,
                messageRouter,
                semantic);
    }

    public void setWriteCallback(WriteCallback writeCallback) {
        this.writeCallback = writeCallback;
    }

    @Override
    public void flush() throws Exception {
        super.preCommit(super.currentTransaction());
    }

    @Override
    public void open(Configuration configuration) throws Exception {
        super.open(configuration);
    }

    private void acknowledgeMessageInternal() {
        synchronized (pendingRecordsLock) {
            pendingRecords--;
            if (pendingRecords == 0) {
                pendingRecordsLock.notifyAll();
            }
        }
    }

    protected void initializeSendCallback() {
        if (sendCallback != null) {
            return;
        }

        this.sendCallback =
                (t, u) -> {
                    if (failedWrite == null && u == null) {
                        acknowledgeMessageInternal();
                        MessageIdImpl messageId = (MessageIdImpl) t;
                        writeCallback.onCompletion(
                                messageId.getPartitionIndex(),
                                MessageIdUtils.getOffset(messageId));
                    } else if (failedWrite == null && u != null) {
                        failedWrite = u;
                    } else { // failedWrite != null
                        LOG.warn("callback error {}", u);
                        // do nothing and wait next checkForError to throw exception
                    }
                };
    }
}
