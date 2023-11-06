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
import org.apache.paimon.flink.log.LogStoreTableFactory;
import org.apache.paimon.options.Options;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.errors.TopicExistsException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.apache.paimon.flink.FlinkCatalogOptions.REGISTER_TIMEOUT;
import static org.apache.paimon.flink.FlinkConnectorOptions.LOG_SYSTEM_PARTITIONS;
import static org.apache.paimon.flink.kafka.KafkaLogOptions.BOOTSTRAP_SERVERS;
import static org.apache.paimon.flink.kafka.KafkaLogOptions.TOPIC;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Fail.fail;

/** Tests for {@link KafkaLogStoreRegister}. */
public class KafkaLogStoreRegisterITCase extends KafkaTableTestBase {
    private static final String DATABASE = "mock_db";

    private static final String TABLE = "mock_table";

    @AfterEach
    public void tearDown() {
        // clean up all the topics
        try (AdminClient admin = createAdminClient()) {
            Set<String> topics = admin.listTopics().names().get();
            admin.deleteTopics(topics).all().get();
        } catch (Exception ignored) {
            // ignored
        }
    }

    @Test
    public void testRegisterTopic() {
        String topic = "register-topic";

        Map<String, String> result =
                createKafkaLogStoreRegister(getBootstrapServers(), topic, 2).registerTopic();
        checkTopicExists(topic, 2, 1);
        assertThat(result.get(TOPIC.key())).isEqualTo(topic);
    }

    @Test
    public void testRegisterTopicAuto() {
        Map<String, String> result =
                createKafkaLogStoreRegister(getBootstrapServers()).registerTopic();

        try (AdminClient admin = createAdminClient()) {
            Set<String> topics = admin.listTopics().names().get(5, TimeUnit.SECONDS);
            assertThat(topics.size()).isEqualTo(1);

            String topicName = topics.stream().findFirst().get();
            assertThat(result.get(TOPIC.key())).isEqualTo(topicName);

            String preFix = String.format("%s_%s_", DATABASE, TABLE);
            assertThat(topicName).startsWith(preFix);

            String uuid = topicName.substring(preFix.length());
            assertThat(uuid).matches("[0-9a-fA-F]{32}");

            // assert use bucket count when log.system.partitions is missed.
            assertThat(result.get(LOG_SYSTEM_PARTITIONS.key())).isEqualTo("1");
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    @Test
    public void testRegisterTopicException() {
        String topic = "register-topic";
        String invalidBootstrapServers = "invalid-bootstrap-servers:9092";

        KafkaLogStoreRegister kafkaLogStoreRegister =
                createKafkaLogStoreRegister(invalidBootstrapServers, topic);
        assertThatThrownBy(kafkaLogStoreRegister::registerTopic)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Register topic for table mock_db.mock_table failed")
                .hasRootCauseInstanceOf(ConfigException.class);
    }

    @Test
    public void testRegisterTopicExist() {
        String topic = "topic-exist";
        createTopic(topic, 1, 1);

        assertThatThrownBy(
                        () ->
                                createKafkaLogStoreRegister(getBootstrapServers(), topic)
                                        .registerTopic())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Register topic for table mock_db.mock_table failed")
                .hasRootCauseInstanceOf(TopicExistsException.class);
    }

    @Test
    public void testUnregisterTopic() {
        String topic = "unregister-topic";
        createTopic(topic, 2, 1);

        createKafkaLogStoreRegister(getBootstrapServers(), topic, 2).unRegisterTopic();
        checkTopicNotExist(topic);
    }

    @Test
    public void testUnregisterTopicException() {
        String topic = "not_exist_topic";

        assertThatCode(
                        () ->
                                createKafkaLogStoreRegister(getBootstrapServers(), topic)
                                        .unRegisterTopic())
                .doesNotThrowAnyException();
    }

    private KafkaLogStoreRegister createKafkaLogStoreRegister(String bootstrapServers) {
        return createKafkaLogStoreRegister(bootstrapServers, null, null);
    }

    private KafkaLogStoreRegister createKafkaLogStoreRegister(
            String bootstrapServers, String topic) {
        return createKafkaLogStoreRegister(bootstrapServers, topic, null);
    }

    private KafkaLogStoreRegister createKafkaLogStoreRegister(
            String bootstrapServers, String topic, Integer partition) {
        Options tableOptions = new Options();
        tableOptions.set(BOOTSTRAP_SERVERS, bootstrapServers);

        if (topic != null) {
            tableOptions.set(TOPIC, topic);
        }

        if (partition != null) {
            tableOptions.set(LOG_SYSTEM_PARTITIONS, partition);
        }
        tableOptions.set(REGISTER_TIMEOUT.key(), Duration.ofSeconds(20).toString());

        return new KafkaLogStoreRegister(
                new LogStoreTableFactory.RegisterContext() {
                    @Override
                    public Options getOptions() {
                        return tableOptions;
                    }

                    @Override
                    public Identifier getIdentifier() {
                        return Identifier.create(DATABASE, TABLE);
                    }
                });
    }

    private void createTopic(String topic, int partition, int replicationFactor) {
        try (AdminClient admin = createAdminClient()) {
            admin.createTopics(
                    Collections.singletonList(
                            new NewTopic(topic, partition, (short) replicationFactor)));
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }
}
