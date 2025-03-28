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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** IT cases for {@link KafkaSyncDatabaseAction}. */
class KafkaMaxwellSyncDatabaseActionITCase extends KafkaSyncDatabaseActionITCase {

    private static final String MAXWELL = "maxwell";

    @Test
    @Timeout(60)
    void testSchemaEvolutionMultiTopic() throws Exception {
        testSchemaEvolutionMultiTopic(MAXWELL);
    }

    @Test
    @Timeout(60)
    void testSchemaEvolutionOneTopic() throws Exception {
        testSchemaEvolutionOneTopic(MAXWELL);
    }

    @Test
    void testTopicIsEmpty() {
        testTopicIsEmpty(MAXWELL);
    }

    @Test
    @Timeout(60)
    void testTableAffixMultiTopic() throws Exception {
        testTableAffixMultiTopic(MAXWELL);
    }

    @Test
    @Timeout(60)
    void testTableAffixOneTopic() throws Exception {
        testTableAffixOneTopic(MAXWELL);
    }

    @Test
    @Timeout(60)
    void testIncludingTables() throws Exception {
        testIncludingTables(MAXWELL);
    }

    @Test
    @Timeout(60)
    void testExcludingTables() throws Exception {
        testExcludingTables(MAXWELL);
    }

    @Test
    @Timeout(60)
    void testIncludingAndExcludingTables() throws Exception {
        testIncludingAndExcludingTables(MAXWELL);
    }
}
