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

package org.apache.paimon.flink.action.cdc;

import org.apache.paimon.flink.action.cdc.format.DataFormat;
import org.apache.paimon.flink.action.cdc.format.RecordParser;
import org.apache.paimon.schema.Schema;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/** Utility class to build schema by trying to read and parse records from message queue. */
public class MessageQueueSchemaUtils {

    private static final int MAX_RETRY = 5;
    private static final int POLL_TIMEOUT_MILLIS = 1000;

    /**
     * Retrieves the Kafka schema for a given topic.
     *
     * @param consumer The wrapper of message queue consumer to fetch messages.
     * @param topic The topic to retrieve the schema for.
     * @param dataFormat The data format for the messages in the message queue.
     * @param typeMapping Data type mapping options.
     * @return The schema for the topic.
     * @throws SchemaRetrievalException If unable to retrieve the schema after max retries.
     */
    public static Schema getSchema(
            ConsumerWrapper consumer, String topic, DataFormat dataFormat, TypeMapping typeMapping)
            throws SchemaRetrievalException {
        int retry = 0;
        int retryInterval = 1000;

        RecordParser recordParser =
                dataFormat.createParser(true, typeMapping, Collections.emptyList());

        while (true) {
            Optional<Schema> schema =
                    consumer.getRecords(topic, POLL_TIMEOUT_MILLIS).stream()
                            .map(recordParser::buildSchema)
                            .filter(Objects::nonNull)
                            .findFirst();

            if (schema.isPresent()) {
                return schema.get();
            }

            if (retry >= MAX_RETRY) {
                throw new SchemaRetrievalException(
                        String.format("Could not get metadata from server, topic: %s", topic));
            }

            sleepSafely(retryInterval);
            retryInterval *= 2;
            retry++;
        }
    }

    private static void sleepSafely(int duration) {
        try {
            Thread.sleep(duration);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /** Custom exception to indicate issues with schema retrieval. */
    public static class SchemaRetrievalException extends Exception {
        public SchemaRetrievalException(String message) {
            super(message);
        }
    }

    /** Wrap the consumer for different message queues. */
    public interface ConsumerWrapper extends AutoCloseable {

        List<String> getRecords(String topic, int pollTimeOutMills);
    }
}
