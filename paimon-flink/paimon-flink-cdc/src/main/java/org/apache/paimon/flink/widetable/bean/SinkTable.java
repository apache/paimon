package org.apache.paimon.flink.widetable.bean;

public class SinkTable {

    public static String SINK_TABLENAME = "big_table";

    public static String buildSinkCreateTableFlinkSql(
            String selectFields, Configuration sinkConfig) {

        String connector = sinkConfig.getString(SinkOptions.CONNECTOR);
        String tablename = sinkConfig.getString(SinkOptions.TABLE_NAME);
        String username = sinkConfig.getString(SinkOptions.USERNAME);
        String password = sinkConfig.getString(SinkOptions.PASSWORD);
        String url = sinkConfig.getString(SinkOptions.URL);

        String[] dbTableFields = selectFields.split(Constant.DOU_HAO);

        int i = 0;
        String primary_key = "";
        StringBuffer colsBuf = new StringBuffer();
        String end = " , \n";
        for (String dtf : dbTableFields) {
            String[] dtfArr = dtf.split(Constant.REG_POINT);
            String tableName = dtfArr[0] + Constant.XIALINE + dtfArr[1];
            String field = dtfArr[2];

            if (i == 0) {
                primary_key = field;
            }
            colsBuf.append(" `").append(field).append("` ").append("STRING").append(end);
            i++;
        }

        colsBuf.append(" `").append(Constant.IS_SYS_DEL).append("` ").append("INT").append(end);

        String create_table_flink_sql =
                "CREATE  TABLE "
                        + SINK_TABLENAME
                        + " \n"
                        + "("
                        + colsBuf.toString()
                        + "PRIMARY KEY (`"
                        + primary_key
                        + "`) NOT ENFORCED "
                        + ") WITH (\n"
                        + "      'connector' = '"
                        + connector
                        + "',\n"
                        + "      'url' = '"
                        + url
                        + "',\n"
                        + "      'table-name' = '"
                        + tablename
                        + "',\n"
                        + "      'username' = '"
                        + username
                        + "',\n"
                        + "      'password' = '"
                        + password
                        + "'\n"
                        + ")";
        System.out.println(create_table_flink_sql);
        return create_table_flink_sql;
    }
}
