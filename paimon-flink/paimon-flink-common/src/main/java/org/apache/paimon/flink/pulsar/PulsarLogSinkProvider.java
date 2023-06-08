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

import org.apache.paimon.CoreOptions.LogChangelogMode;
import org.apache.paimon.CoreOptions.LogConsistency;
import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.flink.log.LogSinkProvider;
import org.apache.paimon.flink.pulsar.sink.PulsarClientUtils;
import org.apache.paimon.flink.pulsar.sink.PulsarSinkSemantic;
import org.apache.paimon.flink.sink.LogSinkFunction;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.table.data.RowData;
import org.apache.pulsar.client.api.MessageRouter;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;

import javax.annotation.Nullable;

import java.util.Optional;
import java.util.Properties;

/**
 * A Pulsar {@link LogSinkProvider}.
 */
public class PulsarLogSinkProvider implements LogSinkProvider {

    private static final long serialVersionUID = 1L;

    private final String adminUrl;
    private final String serviceUrl;
    private final String topicName;
    private final Properties properties;

    private final MessageRouter messageRouter;

    @Nullable
    private final SerializationSchema<RowData> primaryKeySerializer;

    private final LogConsistency consistency;
    private final LogChangelogMode changelogMode;
    private final SerializationSchema<RowData> valueSerializer;

    public PulsarLogSinkProvider(
            String serviceUrl,
            String adminUrl,
            String topicName,
            Properties properties,
            @Nullable SerializationSchema<RowData> primaryKeySerializer,
            SerializationSchema<RowData> valueSerializer,
            LogConsistency logConsistency,
            LogChangelogMode changelogMode) {
        this.serviceUrl = serviceUrl;
        this.adminUrl = adminUrl;
        this.topicName = topicName;
        this.properties = properties;
        this.messageRouter = new PartitionMessageRouter();
        this.valueSerializer = valueSerializer;
        this.primaryKeySerializer = primaryKeySerializer;
        this.consistency = logConsistency;
        this.changelogMode = changelogMode;
    }

    @Override
    public LogSinkFunction createSink() {
        PulsarSinkSemantic semantic;
        switch (consistency) {
            case TRANSACTIONAL:
                semantic = PulsarSinkSemantic.EXACTLY_ONCE;
                break;
            case EVENTUAL:
                semantic = PulsarSinkSemantic.AT_LEAST_ONCE;
                break;
            default:
                throw new IllegalArgumentException("Unsupported: " + consistency);
        }
        ClientConfigurationData clientConf =
                PulsarClientUtils.newClientConf(serviceUrl, properties);
        return new PulsarSinkFunction(
                adminUrl,
                Optional.ofNullable(topicName),
                clientConf,
                properties,
                createSerializationSchema(),
                messageRouter,
                semantic);
    }

    @VisibleForTesting
    PulsarLogSerializationSchema createSerializationSchema() {
        return new PulsarLogSerializationSchema(
                topicName, primaryKeySerializer, valueSerializer, changelogMode);
    }
}
