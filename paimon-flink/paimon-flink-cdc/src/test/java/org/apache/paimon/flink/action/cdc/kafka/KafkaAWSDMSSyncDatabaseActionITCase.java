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

import java.util.Map;

/** IT cases for {@link KafkaSyncDatabaseAction}. */
public class KafkaAWSDMSSyncDatabaseActionITCase extends KafkaSyncDatabaseActionITCase {

    private static final String AWSDMS = "aws-dms";

    @Override
    protected KafkaSyncDatabaseActionBuilder syncDatabaseActionBuilder(
            Map<String, String> kafkaConfig) {
        KafkaSyncDatabaseActionBuilder builder = new KafkaSyncDatabaseActionBuilder(kafkaConfig);
        builder.withPrimaryKeys("id");
        return builder;
    }

    @Test
    @Timeout(90)
    public void testSchemaEvolutionMultiTopic() throws Exception {
        testSchemaEvolutionMultiTopic(AWSDMS);
    }

    @Test
    @Timeout(90)
    public void testSchemaEvolutionOneTopic() throws Exception {
        testSchemaEvolutionOneTopic(AWSDMS);
    }

    @Test
    public void testTopicIsEmpty() {
        testTopicIsEmpty(AWSDMS);
    }

    @Test
    @Timeout(90)
    public void testTableAffixMultiTopic() throws Exception {
        testTableAffixMultiTopic(AWSDMS);
    }

    @Test
    @Timeout(90)
    public void testTableAffixOneTopic() throws Exception {
        testTableAffixOneTopic(AWSDMS);
    }

    @Test
    @Timeout(90)
    public void testIncludingTables() throws Exception {
        testIncludingTables(AWSDMS);
    }

    @Test
    @Timeout(90)
    public void testExcludingTables() throws Exception {
        testExcludingTables(AWSDMS);
    }

    @Test
    @Timeout(90)
    public void testIncludingAndExcludingTables() throws Exception {
        testIncludingAndExcludingTables(AWSDMS);
    }
}
