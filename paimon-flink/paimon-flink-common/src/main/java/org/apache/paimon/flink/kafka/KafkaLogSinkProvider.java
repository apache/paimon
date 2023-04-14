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

package org.apache.paimon.flink.kafka;

import org.apache.paimon.CoreOptions.LogChangelogMode;
import org.apache.paimon.CoreOptions.LogConsistency;
import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.flink.log.LogSinkProvider;
import org.apache.paimon.flink.sink.LogSinkFunction;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer.Semantic;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.data.RowData;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.errors.TopicExistsException;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.Properties;

/** A Kafka {@link LogSinkProvider}. */
public class KafkaLogSinkProvider implements LogSinkProvider {

    private static final long serialVersionUID = 1L;

    private final String topic;

    private final Properties properties;

    @Nullable private final SerializationSchema<RowData> primaryKeySerializer;

    private final SerializationSchema<RowData> valueSerializer;

    private final LogConsistency consistency;

    private final LogChangelogMode changelogMode;

    private final Integer numBuckets;

    public static final int DEFAULT_REPLICATION_FACTOR = 2;

    public KafkaLogSinkProvider(
            String topic,
            Properties properties,
            @Nullable SerializationSchema<RowData> primaryKeySerializer,
            SerializationSchema<RowData> valueSerializer,
            LogConsistency consistency,
            LogChangelogMode changelogMode,
            Integer numBuckets) {
        this.topic = topic;
        this.properties = properties;
        this.primaryKeySerializer = primaryKeySerializer;
        this.valueSerializer = valueSerializer;
        this.consistency = consistency;
        this.changelogMode = changelogMode;
        this.numBuckets = numBuckets;
    }

    @Override
    public LogSinkFunction createSink() {
        Semantic semantic;
        switch (consistency) {
            case TRANSACTIONAL:
                semantic = Semantic.EXACTLY_ONCE;
                break;
            case EVENTUAL:
                semantic = Semantic.AT_LEAST_ONCE;
                break;
            default:
                throw new IllegalArgumentException("Unsupported: " + consistency);
        }
        createTopicIfNotExists();
        return new KafkaSinkFunction(topic, createSerializationSchema(), properties, semantic);
    }

    @VisibleForTesting
    KafkaLogSerializationSchema createSerializationSchema() {
        return new KafkaLogSerializationSchema(
                topic, primaryKeySerializer, valueSerializer, changelogMode);
    }

    private void createTopicIfNotExists() {
        try (final AdminClient adminClient = AdminClient.create(properties)) {
            if (!adminClient.listTopics().names().get().contains(topic)) {
                int numBrokers = adminClient.describeCluster().nodes().get().size();
                int replicationFactor =
                        DEFAULT_REPLICATION_FACTOR > numBrokers
                                ? numBrokers
                                : DEFAULT_REPLICATION_FACTOR;

                NewTopic newTopic = new NewTopic(topic, numBuckets, (short) replicationFactor);

                adminClient.createTopics(Collections.singleton(newTopic)).all().get();
            }
        } catch (Exception e) {
            if (e.getCause() instanceof TopicExistsException) {
                throw new TableException(
                        String.format(
                                "Failed to create kafka topic. " + "Reason: topic %s exists. ",
                                topic));
            }
            throw new TableException("Error in createTopicIfNotExists", e);
        }
    }
}
