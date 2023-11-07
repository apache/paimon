package org.apache.paimon.flink.action.widetable.utils.options;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

public class SqlInfoOptions {

    public static String PRE_NAME = "sql-conf";
    public static final ConfigOption<String> BUSINESS_SQL =
            ConfigOptions.key("business-sql")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("business sql");
    public static final ConfigOption<String> MAIN_TABLE_KEY =
            ConfigOptions.key("main-table-key")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("main table key field of the sql");
    public static final ConfigOption<String> JOIN_FIELDS =
            ConfigOptions.key("join-fields")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("join on fields of the sql");
    public static final ConfigOption<String> SELECT_FIELDS =
            ConfigOptions.key("select-fields")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("select fields of the sql");
    public static final ConfigOption<String> INSERT_FLINK_SQL =
            ConfigOptions.key("insert-flinksql")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("insert flink sql");
}
