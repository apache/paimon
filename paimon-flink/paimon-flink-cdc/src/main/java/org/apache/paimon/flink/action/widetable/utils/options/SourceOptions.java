package org.apache.paimon.flink.action.widetable.utils.options;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

public class SourceOptions {

    public static String PRE_NAME = "source-conf";
    public static final ConfigOption<String> USERNAME =
            ConfigOptions.key("username")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("username of the Mysql server.");
    public static final ConfigOption<String> PASSWORD =
            ConfigOptions.key("password")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("password of the Mysql server.");
    public static final ConfigOption<String> HOSTNAME =
            ConfigOptions.key("hostname")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("hostname of the Mysql server.");
    public static final ConfigOption<Integer> PORT =
            ConfigOptions.key("port")
                    .intType()
                    .defaultValue(3306)
                    .withDescription("port of the Mysql server.");
    public static final ConfigOption<String> DATABASE =
            ConfigOptions.key("database")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("database of the Mysql server.");
    public static final ConfigOption<String> INCLUDING_TABLES =
            ConfigOptions.key("including-tables")
                    .stringType()
                    .defaultValue("db.table")
                    .withDescription("including tables  of the Mysql server.");
    public static final ConfigOption<String> TABLES =
            ConfigOptions.key("tables")
                    .stringType()
                    .defaultValue("db.table")
                    .withDescription("including tables  of the Mysql server.");
}
