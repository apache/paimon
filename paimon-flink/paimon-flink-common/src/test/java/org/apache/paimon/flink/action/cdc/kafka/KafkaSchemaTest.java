package org.apache.paimon.flink.action.cdc.kafka;

import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;

import org.apache.flink.configuration.Configuration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class KafkaSchemaTest extends KafkaCanalActionITCaseBase {
    @Test
    @Timeout(60)
    public void testKafkaSchema() throws Exception {
        final String topic = "test_kafka_schema";
        createTestTopic(topic, 1, 1);
        // ---------- Write the Canal json into Kafka -------------------
        List<String> lines = readLines("kafka.canal/table/schemaevolution/canal-data-1.txt");
        try {
            writeRecordsToKafka(topic, lines);
        } catch (Exception e) {
            throw new Exception("Failed to write canal data to Kafka.", e);
        }
        Map<String, String> kafkaConfig = getBasicKafkaConfig();
        kafkaConfig.put("value.format", "canal-json");
        kafkaConfig.put("topic", topic);

        KafkaSchema kafkaSchema = new KafkaSchema(Configuration.fromMap(kafkaConfig), topic);
        Map<String, DataType> fields = new LinkedHashMap<>();
        fields.put("pt", DataTypes.INT());
        fields.put("_id", DataTypes.INT());
        fields.put("v1", DataTypes.VARCHAR(10));
        String tableName = "schema_evolution_1";
        String databasesName = "paimon_sync_table";
        assertThat(kafkaSchema.fields()).isEqualTo(fields);
        assertThat(kafkaSchema.tableName()).isEqualTo(tableName);
        assertThat(kafkaSchema.databaseName()).isEqualTo(databasesName);
    }
}
