package org.apache.paimon.spark;

public class SparkSQLProperties {
    private SparkSQLProperties(){}

    public static final String USE_V2_WRITE = "spark.sql.paimon.use-v2-write";
    public static final String USE_V2_WRITE_DEFAULT = "false";
}
