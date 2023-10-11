package org.apache.paimon.flink.action.cdc.kafka;

import org.apache.paimon.flink.action.cdc.TypeMapping;
import org.apache.paimon.flink.action.cdc.format.canal.CanalRecordParser;
import org.apache.paimon.schema.Schema;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;

public class CanalRecordParserTest {

    @Test
    public void test1() {
        String json =
                "{\n"
                        + "  \"data\": [\n"
                        + "    {\n"
                        + "      \"VERSION\": \"6.3.1\",\n"
                        + "      \"GUID\": \"3895b336-f7d4-4b00-b110-c82fb6c3ad88\",\n"
                        + "      \"LAST_UPDATE_INSTANT\": \"1676564925610\",\n"
                        + "      \"TS\": \"0\",\n"
                        + "      \"HOSTNAME\": \"cdh1/172.18.5.44\",\n"
                        + "      \"LAST_ACTIVE_TIMESTAMP\": \"1696939709931\"\n"
                        + "    }\n"
                        + "  ],\n"
                        + "  \"database\": \"scm\",\n"
                        + "  \"es\": 1696939709000,\n"
                        + "  \"gtid\": \"\",\n"
                        + "  \"id\": 641,\n"
                        + "  \"isDdl\": false,\n"
                        + "  \"mysqlType\": {\n"
                        + "    \"VERSION\": \"varchar(256)\",\n"
                        + "    \"GUID\": \"varchar(36)\",\n"
                        + "    \"LAST_UPDATE_INSTANT\": \"bigint\",\n"
                        + "    \"TS\": \"bigint\",\n"
                        + "    \"HOSTNAME\": \"varchar(255)\",\n"
                        + "    \"LAST_ACTIVE_TIMESTAMP\": \"bigint\"\n"
                        + "  },\n"
                        + "  \"old\": [\n"
                        + "    {\n"
                        + "      \"LAST_ACTIVE_TIMESTAMP\": \"1696939694927\"\n"
                        + "    }\n"
                        + "  ],\n"
                        + "  \"pkNames\": null,\n"
                        + "  \"sql\": \"\",\n"
                        + "  \"sqlType\": {\n"
                        + "    \"VERSION\": 12,\n"
                        + "    \"GUID\": 12,\n"
                        + "    \"LAST_UPDATE_INSTANT\": -5,\n"
                        + "    \"TS\": -5,\n"
                        + "    \"HOSTNAME\": 12,\n"
                        + "    \"LAST_ACTIVE_TIMESTAMP\": -5\n"
                        + "  },\n"
                        + "  \"table\": \"cm_version\",\n"
                        + "  \"ts\": 1696939709937,\n"
                        + "  \"type\": \"UPDATE\"\n"
                        + "}";

        CanalRecordParser canalRecordParser =
                new CanalRecordParser(false, TypeMapping.defaultMapping(), new ArrayList<>());
        Schema schema = canalRecordParser.buildSchema(json);
    }
}
