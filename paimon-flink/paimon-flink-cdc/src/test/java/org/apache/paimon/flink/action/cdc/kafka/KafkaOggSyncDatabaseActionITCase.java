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
class KafkaOggSyncDatabaseActionITCase extends KafkaSyncDatabaseActionITCase {
    private static final String OGG = "ogg";

    @Test
    @Timeout(60)
    void testSchemaEvolutionMultiTopic() throws Exception {
        testSchemaEvolutionMultiTopic(OGG);
    }

    @Test
    @Timeout(60)
    void testSchemaEvolutionOneTopic() throws Exception {
        testSchemaEvolutionOneTopic(OGG);
    }

    @Test
    void testTopicIsEmpty() {
        testTopicIsEmpty(OGG);
    }

    @Test
    @Timeout(60)
    void testTableAffixMultiTopic() throws Exception {
        testTableAffixMultiTopic(OGG);
    }

    @Test
    @Timeout(60)
    void testTableAffixOneTopic() throws Exception {
        testTableAffixOneTopic(OGG);
    }

    @Test
    @Timeout(60)
    void testIncludingTables() throws Exception {
        testIncludingTables(OGG);
    }

    @Test
    @Timeout(60)
    void testExcludingTables() throws Exception {
        testExcludingTables(OGG);
    }

    @Test
    @Timeout(60)
    void testIncludingAndExcludingTables() throws Exception {
        testIncludingAndExcludingTables(OGG);
    }

    @Test
    @Timeout(60)
    void testCaseInsensitive() throws Exception {
        testCaseInsensitive(OGG);
    }
}
