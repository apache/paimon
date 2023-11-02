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

import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.flink.log.LogStoreRegister;
import org.apache.paimon.flink.log.LogStoreTableFactory;

import org.apache.paimon.shade.guava30.com.google.common.collect.ImmutableMap;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.apache.paimon.CoreOptions.BUCKET;
import static org.apache.paimon.flink.FlinkCatalogOptions.REGISTER_TIMEOUT;
import static org.apache.paimon.flink.FlinkConnectorOptions.LOG_SYSTEM_PARTITIONS;
import static org.apache.paimon.flink.FlinkConnectorOptions.LOG_SYSTEM_REPLICATION;
import static org.apache.paimon.flink.kafka.KafkaLogOptions.BOOTSTRAP_SERVERS;
import static org.apache.paimon.flink.kafka.KafkaLogOptions.TOPIC;
import static org.apache.paimon.flink.kafka.KafkaLogStoreFactory.toKafkaProperties;
import static org.apache.paimon.utils.Preconditions.checkNotNull;

/** KafkaLogStoreRegister is used to register/unregister topics in Kafka for paimon table. */
public class KafkaLogStoreRegister implements LogStoreRegister {

    private final String bootstrapServers;

    private final String topic;

    private final int partition;

    private final int replicationFactor;

    private final Duration timeout;

    private final Properties properties;

    private final Identifier identifier;

    public KafkaLogStoreRegister(LogStoreTableFactory.RegisterContext context) {
        this.bootstrapServers = context.getOptions().get(BOOTSTRAP_SERVERS);
        this.identifier = context.getIdentifier();
        this.topic =
                context.getOptions().getOptional(TOPIC).isPresent()
                        ? context.getOptions().get(TOPIC)
                        : String.format(
                                "%s_%s_%s",
                                this.identifier.getDatabaseName(),
                                this.identifier.getObjectName(),
                                UUID.randomUUID().toString().replace("-", ""));

        checkNotNull(context.getOptions().get(BOOTSTRAP_SERVERS));
        checkNotNull(this.topic);
        checkNotNull(this.identifier);

        // handle the type information missing when Map is converted to Options
        if (context.getOptions().get(REGISTER_TIMEOUT.key()) == null) {
            this.timeout = REGISTER_TIMEOUT.defaultValue();
        } else {
            this.timeout = Duration.parse(context.getOptions().get(REGISTER_TIMEOUT.key()));
        }

        // handle bucket=-1
        int bucketNum =
                context.getOptions().get(BUCKET) == -1 ? 1 : context.getOptions().get(BUCKET);
        this.partition =
                context.getOptions().getOptional(LOG_SYSTEM_PARTITIONS).isPresent()
                        ? context.getOptions().get(LOG_SYSTEM_PARTITIONS)
                        : bucketNum;

        this.replicationFactor = context.getOptions().get(LOG_SYSTEM_REPLICATION);

        this.properties = toKafkaProperties(context.getOptions());
    }

    @Override
    public Map<String, String> registerTopic() {
        try (AdminClient admin = AdminClient.create(properties)) {
            NewTopic newTopic =
                    new NewTopic(this.topic, this.partition, (short) this.replicationFactor);

            // Since the call is Async, let's wait for it to complete.
            admin.createTopics(Collections.singleton(newTopic))
                    .all()
                    .get(this.timeout.getSeconds(), TimeUnit.SECONDS);
        } catch (TimeoutException e) {
            throw new IllegalStateException(
                    String.format(
                            "Register topic for table %s timeout with properties %s",
                            this.identifier.getFullName(), properties),
                    e);
        } catch (Exception e) {
            throw new IllegalStateException(
                    String.format(
                            "Register topic for table %s failed with properties %s",
                            this.identifier.getFullName(), properties),
                    e);
        }

        return ImmutableMap.of(
                TOPIC.key(),
                this.topic,
                LOG_SYSTEM_PARTITIONS.key(),
                String.valueOf(this.partition),
                LOG_SYSTEM_REPLICATION.key(),
                String.valueOf(this.replicationFactor));
    }

    @Override
    public void unRegisterTopic() {
        try (AdminClient admin = AdminClient.create(properties)) {
            admin.deleteTopics(Collections.singleton(this.topic))
                    .all()
                    .get(this.timeout.getSeconds(), TimeUnit.SECONDS);
        } catch (ExecutionException e) {
            if (e.getCause()
                    instanceof org.apache.kafka.common.errors.UnknownTopicOrPartitionException) {
                // ignore
                return;
            } else {
                throw new IllegalStateException(
                        String.format(
                                "Unregister topic for table %s failed with properties %s",
                                this.identifier.getFullName(), properties),
                        e);
            }
        } catch (TimeoutException e) {
            throw new RuntimeException(
                    String.format(
                            "Unregister topic for table %s timeout with properties %s",
                            this.identifier.getFullName(), properties),
                    e);
        } catch (Exception e) {
            throw new RuntimeException(
                    String.format(
                            "Unregister topic for table %s failed with properties %s",
                            this.identifier.getFullName(), properties),
                    e);
        }
    }
}
