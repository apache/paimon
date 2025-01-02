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

package org.apache.paimon.flink.action.cdc.kafka;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** IT cases for {@link KafkaSyncDatabaseAction}. */
public class KafkaDebeziumSyncDatabaseActionITCase extends KafkaSyncDatabaseActionITCase {
    private static final String DEBEZIUM = "debezium";

    @Disabled // TODO How to append table in combined mode?
    @Test
    @Timeout(90)
    public void testSchemaEvolutionMultiTopic() throws Exception {
        testSchemaEvolutionMultiTopic(DEBEZIUM);
    }

    @Disabled // TODO How to append table in combined mode?
    @Test
    @Timeout(90)
    public void testSchemaEvolutionOneTopic() throws Exception {
        testSchemaEvolutionOneTopic(DEBEZIUM);
    }

    @Test
    public void testTopicIsEmpty() {
        testTopicIsEmpty(DEBEZIUM);
    }

    @Disabled // TODO How to append table in combined mode?
    @Test
    @Timeout(90)
    public void testTableAffixMultiTopic() throws Exception {
        testTableAffixMultiTopic(DEBEZIUM);
    }

    @Disabled // TODO How to append table in combined mode?
    @Test
    @Timeout(90)
    public void testTableAffixOneTopic() throws Exception {
        testTableAffixOneTopic(DEBEZIUM);
    }

    @Disabled // TODO How to append table in combined mode?
    @Test
    @Timeout(90)
    public void testIncludingTables() throws Exception {
        testIncludingTables(DEBEZIUM);
    }

    @Disabled // TODO How to append table in combined mode?
    @Test
    @Timeout(90)
    public void testExcludingTables() throws Exception {
        testExcludingTables(DEBEZIUM);
    }

    @Disabled // TODO How to append table in combined mode?
    @Test
    @Timeout(90)
    public void testIncludingAndExcludingTables() throws Exception {
        testIncludingAndExcludingTables(DEBEZIUM);
    }
}
