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
public class KafkaMaxwellSyncDatabaseActionITCase extends KafkaSyncDatabaseActionITCase {

    private static final String MAXWELL = "maxwell";

    @Test
    @Timeout(60)
    public void testSchemaEvolutionMultiTopic() throws Exception {
        testSchemaEvolutionMultiTopic(MAXWELL);
    }

    @Test
    @Timeout(60)
    public void testSchemaEvolutionOneTopic() throws Exception {
        testSchemaEvolutionOneTopic(MAXWELL);
    }

    @Test
    public void testTopicIsEmpty() {
        testTopicIsEmpty(MAXWELL);
    }

    @Test
    @Timeout(60)
    public void testTableAffixMultiTopic() throws Exception {
        testTableAffixMultiTopic(MAXWELL);
    }

    @Test
    @Timeout(60)
    public void testTableAffixOneTopic() throws Exception {
        testTableAffixOneTopic(MAXWELL);
    }

    @Test
    @Timeout(60)
    public void testIncludingTables() throws Exception {
        testIncludingTables(MAXWELL);
    }

    @Test
    @Timeout(60)
    public void testExcludingTables() throws Exception {
        testExcludingTables(MAXWELL);
    }

    @Test
    @Timeout(60)
    public void testIncludingAndExcludingTables() throws Exception {
        testIncludingAndExcludingTables(MAXWELL);
    }

    @Test
    @Timeout(60)
    public void testMergeShards() throws Exception {
        testMergeShards(MAXWELL);
    }
}
