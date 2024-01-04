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

import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.table.AbstractFileStoreTable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.Map;

import static org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptions.TOPIC;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptions.VALUE_FORMAT;

/** IT cases for {@link KafkaSyncTableAction}. */
public class KafkaOggSyncTableActionITCase extends KafkaSyncTableActionITCase {

    private static final String OGG = "ogg";

    @Test
    @Timeout(60)
    public void testSchemaEvolution() throws Exception {
        runSingleTableSchemaEvolution("schemaevolution", OGG);
    }

    @Test
    @Timeout(60)
    public void testNotSupportFormat() throws Exception {
        testNotSupportFormat(OGG);
    }

    @Test
    @Timeout(60)
    public void testAssertSchemaCompatible() throws Exception {
        testAssertSchemaCompatible(OGG);
    }

    @Test
    @Timeout(60)
    public void testStarUpOptionSpecific() throws Exception {
        testStarUpOptionSpecific(OGG);
    }

    @Test
    @Timeout(60)
    public void testStarUpOptionLatest() throws Exception {
        testStarUpOptionLatest(OGG);
    }

    @Test
    @Timeout(60)
    public void testStarUpOptionTimestamp() throws Exception {
        testStarUpOptionTimestamp(OGG);
    }

    @Test
    @Timeout(60)
    public void testStarUpOptionEarliest() throws Exception {
        testStarUpOptionEarliest(OGG);
    }

    @Test
    @Timeout(60)
    public void testStarUpOptionGroup() throws Exception {
        testStarUpOptionGroup(OGG);
    }

    @Test
    @Timeout(60)
    public void testComputedColumn() throws Exception {
        testComputedColumn(OGG);
    }

    @Test
    @Timeout(60)
    public void testCDCOperations() throws Exception {
        testCDCOperations(OGG);
    }


    @Test
    @Timeout(60)
    public void testWaterMarkSyncTable() throws Exception {
        String topic = "watermark";
        createTestTopic(topic, 1, 1);
        writeRecordsToKafka(topic, readLines("kafka/ogg/table/watermark/ogg-data-1.txt"));

        Map<String, String> kafkaConfig = getBasicKafkaConfig();
        kafkaConfig.put(VALUE_FORMAT.key(), "ogg-json");
        kafkaConfig.put(TOPIC.key(), topic);

        Map<String, String> config = getBasicTableConfig();
        config.put("tag.automatic-creation", "watermark");
        config.put("tag.creation-period", "hourly");
        config.put("scan.watermark.alignment.group", "alignment-group-1");
        config.put("scan.watermark.alignment.max-drift", "20 s");
        config.put("scan.watermark.alignment.update-interval", "1 s");

        KafkaSyncTableAction action =
            syncTableActionBuilder(kafkaConfig).withTableConfig(config).build();
        runActionWithDefaultEnv(action);

        AbstractFileStoreTable table =
            (AbstractFileStoreTable) catalog.getTable(new Identifier(database, tableName));
        while (true) {
            if (table.snapshotManager().snapshotCount() > 0
                && table.snapshotManager().latestSnapshot().watermark()
                != -9223372036854775808L) {
                return;
            }
            Thread.sleep(1000);
        }
    }
}
