package org.apache.paimon.flink.action.cdc.postgresql;

import com.ververica.cdc.connectors.postgres.PostgreSQLSource;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

public class PostgreSqlCDCTEst {

    public static void main(String[] args) throws Exception {
        SourceFunction<String> sourceFunction = PostgreSQLSource.<String>builder()
                .hostname("hadoop201")
                .port(5432)
                .database("test") // monitor postgres database
                .schemaList("public")  // monitor inventory schema
                .tableList("public.company") // monitor products table
                .username("postgres")
                .password("123456")
                .decodingPluginName("pgoutput")
                .deserializer(new JsonDebeziumDeserializationSchema(true)) // converts SourceRecord to JSON String
                .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env
                .addSource(sourceFunction)
                .print().setParallelism(1); // use parallelism 1 for sink to keep message ordering

        env.execute();
    }
}
