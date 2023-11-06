package org.apache.paimon.flink.widetable.msg.cdc.mysql;

import java.util.LinkedHashMap;

public class MysqlCDCMessage {
    // 数据库名
    private String database;

    // 操作类型
    private String type;

    // 操作类型
    private String mode;

    private String pk;

    private long ts;
    // 数据产生时间
    private long es;
    // 数据表名
    private String table;

    private LinkedHashMap<String, String> before;

    private LinkedHashMap<String, String> after;
}
