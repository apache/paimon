package org.apache.paimon.flink.widetable.utils;

import org.apache.paimon.flink.widetable.map.BuildMessageFlatMapFunction;
import org.apache.paimon.flink.widetable.msg.Message;
import org.apache.paimon.flink.widetable.msg.cdc.mysql.MysqlCDCCustomerDeserialization;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.utils.MultipleParameterTool;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Preconditions;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class ParamUitl {

    public static Map<String, String> optionalConfigMap(MultipleParameterTool params, String key) {
        if (!params.has(key)) {
            return Collections.emptyMap();
        }

        Map<String, String> config = new HashMap<>();
        for (String kvString : params.getMultiParameter(key)) {
            parseKeyValueString(config, kvString);
        }
        return config;
    }

    public static void parseKeyValueString(Map<String, String> map, String kvString) {
        String[] kv = kvString.split("=", 2);
        if (kv.length != 2) {
            throw new IllegalArgumentException(
                    String.format(
                            "Invalid key-value string '%s'. Please use format 'key=value'",
                            kvString));
        }
        map.put(kv[0].trim(), kv[1].trim());
    }

    public static void checkRequiredArgument(MultipleParameterTool params, String key) {
        Preconditions.checkArgument(
                params.has(key), "Argument '%s' is required. Run '<action> --help' for help.", key);
    }

    public static String praseTables(String dbTables) {
        String[] spTables = dbTables.split(",");
        StringBuffer dbTableBf = new StringBuffer();
        int i = 0;
        for (String dbTable : spTables) {
            String[] tabAndKP = dbTable.split(Constant.AIT);
            if (tabAndKP.length == 2) {
                dbTable = tabAndKP[0];
            }
            if (i == 0) {
                dbTableBf.append(dbTable);
            } else {
                dbTableBf.append(",").append(dbTable);
            }
            i++;
        }
        dbTables = dbTableBf.toString();
        return dbTables;
    }

    public static Map<String, String> praseKpMap(String dbTables) {
        Map<String, String> kpMap = new HashMap<String, String>();
        String[] spTables = dbTables.split(Constant.DOU_HAO);

        for (String dbTable : spTables) {
            String[] tabAndKP = dbTable.split(Constant.AIT);
            if (tabAndKP.length == 2) {
                dbTable = tabAndKP[0];
                kpMap.put(dbTable, tabAndKP[1]);
            }
        }
        return kpMap;
    }

    public static void praseSql(Configuration sqlConf) throws JSQLParserException {
        String business_sql = sqlConf.getString(SqlInfoOptions.BUSINESS_SQL);
        System.out.println(business_sql);
        String selectFields = SQLParser.parseAndJoinSelectFields(business_sql);
        String onConditions = SQLParser.parseAndJoinOnConditions(business_sql);
        selectFields = selectFields.replace(Constant.BLANK_SPACE, Constant.EMPTY_STR);
        onConditions = onConditions.replace(Constant.BLANK_SPACE, Constant.EMPTY_STR);

        if (sqlConf.getString(SqlInfoOptions.JOIN_FIELDS) == null) {
            sqlConf.set(SqlInfoOptions.JOIN_FIELDS, onConditions);
        }

        if (sqlConf.getString(SqlInfoOptions.MAIN_TABLE_KEY) == null) {
            sqlConf.set(SqlInfoOptions.MAIN_TABLE_KEY, onConditions.split(Constant.DENG_HAO)[0]);
        }

        if (sqlConf.getString(SqlInfoOptions.SELECT_FIELDS) == null) {
            sqlConf.set(SqlInfoOptions.SELECT_FIELDS, selectFields);
        }
    }

    public static Map<String, Configuration> buildSourceConfigMap(Configuration sourceConfig) {
        StringBuffer dbTables = new StringBuffer();
        Map<String, Configuration> sourceConfigMap = new HashMap<>();
        for (String sk : sourceConfig.keySet()) {
            String[] skArr = sk.split(Constant.HENGLINE);
            if (skArr.length != 2) {
                continue;
            }
            String s = skArr[0];
            String v = skArr[1];
            if (!sourceConfigMap.containsKey(s)) {
                sourceConfigMap.put(s, new Configuration());
            }
            if (v.equals(SourceOptions.USERNAME.key())) {
                ConfigOption<String> key = ConfigOptions.key(sk).stringType().noDefaultValue();
                sourceConfigMap.get(s).set(SourceOptions.USERNAME, sourceConfig.getString(key));
            }
            if (v.equals(SourceOptions.PASSWORD.key())) {
                ConfigOption<String> key = ConfigOptions.key(sk).stringType().noDefaultValue();
                sourceConfigMap.get(s).set(SourceOptions.PASSWORD, sourceConfig.getString(key));
            }
            if (v.equals(SourceOptions.HOSTNAME.key())) {
                ConfigOption<String> key = ConfigOptions.key(sk).stringType().noDefaultValue();
                sourceConfigMap.get(s).set(SourceOptions.HOSTNAME, sourceConfig.getString(key));
            }
            if (v.equals(SourceOptions.DATABASE.key())) {
                ConfigOption<String> key = ConfigOptions.key(sk).stringType().noDefaultValue();
                sourceConfigMap.get(s).set(SourceOptions.DATABASE, sourceConfig.getString(key));
            }
            if (v.equals(SourceOptions.TABLES.key())) {
                ConfigOption<String> key = ConfigOptions.key(sk).stringType().noDefaultValue();
                String tables = sourceConfig.getString(key);
                if (dbTables.toString().equals(Constant.EMPTY_STR)) {
                    dbTables.append(tables);
                } else {
                    dbTables.append(Constant.DOU_HAO).append(tables);
                }
                sourceConfigMap.get(s).set(SourceOptions.TABLES, ParamUitl.praseTables(tables));
            }
            if (v.equals(SourceOptions.PORT.key())) {
                ConfigOption<Integer> key = ConfigOptions.key(sk).intType().noDefaultValue();
                sourceConfigMap.get(s).set(SourceOptions.PORT, sourceConfig.getInteger(key));
            }
            if (v.equals(SourceOptions.PORT.key())) {
                ConfigOption<Integer> key = ConfigOptions.key(sk).intType().noDefaultValue();
                sourceConfigMap.get(s).set(SourceOptions.PORT, sourceConfig.getInteger(key));
            }
        }

        sourceConfig.set(SourceOptions.INCLUDING_TABLES, dbTables.toString());
        return sourceConfigMap;
    }

    public static DataStream<Message> buildSourceDataStream(
            int parallel,
            Map<String, Configuration> sourceConfigMap,
            int fetchSize,
            StartupOptions startupOptions,
            StreamExecutionEnvironment env) {
        // 生成DataStream
        DataStream<Message> messageDataStream = null;
        for (Map.Entry<String, Configuration> entry : sourceConfigMap.entrySet()) {
            String key = entry.getKey();
            Configuration value = entry.getValue();

            String username = value.getString(SourceOptions.USERNAME);
            String password = value.getString(SourceOptions.PASSWORD);
            String dbHostname = value.getString(SourceOptions.HOSTNAME);
            int dbPort = value.getInteger(SourceOptions.PORT);
            String dbname = value.getString(SourceOptions.DATABASE);
            String tables = value.getString(SourceOptions.TABLES);

            MySqlSource<String> mySqlSource =
                    MySqlSource.<String>builder()
                            .hostname(dbHostname)
                            .port(dbPort)
                            .databaseList(dbname) // 设置捕获的数据库， 如果需要同步整个数据库，请将 tableList 设置为 ".*".
                            .tableList(tables) // 设置捕获的表
                            .username(username)
                            .password(password)
                            .deserializer(
                                    new MysqlCDCCustomerDeserialization()) // 将 SourceRecord 转换为
                            // JSON 字符串
                            .startupOptions(startupOptions)
                            .connectionPoolSize(parallel)
                            .fetchSize(fetchSize)
                            .build();

            DataStream<Message> sourceRecoder =
                    env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), key)
                            .setParallelism(parallel)
                            .flatMap(new BuildMessageFlatMapFunction());

            if (messageDataStream == null) {
                messageDataStream = sourceRecoder;
            } else {
                messageDataStream = messageDataStream.union(sourceRecoder);
            }
        }
        return messageDataStream;
    }
}
