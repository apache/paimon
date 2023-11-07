package org.apache.paimon.flink.widetable.utils.options;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

public class JobOptions {

    public static String PRE_NAME = "job-conf";
    public static final ConfigOption<Integer> PARALLEL =
            ConfigOptions.key("parallel")
                    .intType()
                    .defaultValue(6)
                    .withDescription("parallel of the Applicaton Job.");
    public static final ConfigOption<Integer> WINDOW_SIZE =
            ConfigOptions.key("window-size")
                    .intType()
                    .defaultValue(70)
                    .withDescription("window-size of the Applicaton Job.");
    public static final ConfigOption<Integer> CHECKPOINT_INTERVAL =
            ConfigOptions.key("checkpoint-interval")
                    .intType()
                    .defaultValue(30000)
                    .withDescription("checkpoint interval of the Applicaton Job.");
}
