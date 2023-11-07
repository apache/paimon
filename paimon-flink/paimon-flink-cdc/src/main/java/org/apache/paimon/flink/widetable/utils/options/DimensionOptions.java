package org.apache.paimon.flink.widetable.utils.options;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

public class DimensionOptions {

    public static String PRE_NAME = "dim-conf";
    public static final ConfigOption<String> USERNAME =
            ConfigOptions.key("username")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("username of the indexdb server.");
    public static final ConfigOption<String> PASSWORD =
            ConfigOptions.key("password")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("password of the indexdb server.");
    public static final ConfigOption<String> HOSTNAME =
            ConfigOptions.key("hostname")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("hostname of the indexdb server.");
    public static final ConfigOption<Integer> PORT =
            ConfigOptions.key("port")
                    .intType()
                    .defaultValue(3306)
                    .withDescription("port of the indexdb server.");
    public static final ConfigOption<String> DATABASE =
            ConfigOptions.key("database")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("database of the indexdb server.");
    public static final ConfigOption<String> URL_PARAM =
            ConfigOptions.key("url-param")
                    .stringType()
                    .defaultValue("useSSL=false&useUnicode=true&characterEncoding=UTF-8")
                    .withDescription("db-type of the indexdb server.");
}
