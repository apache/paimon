package org.apache.paimon.flink.action.widetable.bean;

import java.io.Serializable;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class DimensionTable implements Serializable {

    public static final String POINT = ".";

    private String table_name;
    private List<Column> join_fields;
    private List<Column> select_fields;
    private Column main_key;
    private String main_key_sql;
    private String create_table_sql;
    private Column primary_key;
    private Column del;
    private String create_table_flink_sql;

    {
        Column d = new Column();
        d.setCol_name(Constant.IS_SYS_DEL);
        d.setCol_type(ColType.INT.getValue());
        del = d;
    }

    public List<Column> buildColumns() {
        List<Column> columns = new ArrayList();
        columns.add(primary_key);
        columns.add(del);

        // 需要测试包含逻辑去重是否有用
        for (Column jk : join_fields) {
            if (columns.contains(jk)) {
                continue;
            }
            columns.add(jk);
        }

        for (Column sk : select_fields) {
            if (columns.contains(sk)) {
                continue;
            }
            columns.add(sk);
        }
        return columns;
    }

    public void buildJoinFields(Map<String, Set<String>> tablesJoinFieldMap) {
        Set<String> jcolSet = tablesJoinFieldMap.get(this.table_name);
        List<Column> joinFields = new ArrayList<>();
        for (String colName : jcolSet) {
            Column col = new Column();
            col.setCol_name(colName);
            col.setCol_type(ColType.STRING.getValue());
            joinFields.add(col);
        }
        this.setJoin_fields(joinFields);
    }

    public void buildSelectFileds(Map<String, Set<String>> tablesJoinFieldMap) {
        Set<String> scolSet = tablesJoinFieldMap.get(this.table_name);
        List<Column> selectFields = new ArrayList<>();
        for (String colName : scolSet) {
            Column col = new Column();
            col.setCol_name(colName);
            col.setCol_type(ColType.STRING.getValue());
            selectFields.add(col);
        }
        this.setSelect_fields(selectFields);
    }

    public void buildCreateTableSql() {
        List<Column> jcolList = this.getJoin_fields();
        StringBuffer colsBuf = new StringBuffer();
        StringBuffer keyindexBuf = new StringBuffer();

        String end = " , \n";

        colsBuf.append(" `")
                .append(primary_key.getCol_name())
                .append("` ")
                .append(primary_key.getCol_type())
                .append(end);

        String delColName = del.getCol_name();
        colsBuf.append(" `").append(delColName).append("` ").append(del.getCol_type()).append(end);
        keyindexBuf
                .append(", \n KEY `")
                .append(delColName)
                .append("` (`")
                .append(delColName)
                .append("`)");

        for (Column col : jcolList) {
            if (col.equals(primary_key)) {
                continue;
            }
            String colName = col.getCol_name();
            colsBuf.append(" `").append(colName).append("` ").append(col.getCol_type()).append(end);

            keyindexBuf
                    .append(", \n KEY `")
                    .append(colName)
                    .append("` (`")
                    .append(colName)
                    .append("`)");
        }

        List<Column> scolList = this.getSelect_fields();
        for (Column col : scolList) {
            if (col.equals(primary_key)) {
                continue;
            }
            String colName = col.getCol_name();
            colsBuf.append(" `").append(colName).append("` ").append(col.getCol_type()).append(end);
        }

        String create_table_sql =
                "CREATE TABLE IF NOT EXISTS  "
                        + table_name
                        + " \n"
                        + "("
                        + colsBuf.toString()
                        + "PRIMARY KEY (`"
                        + primary_key.getCol_name()
                        + "`)"
                        + keyindexBuf.toString()
                        + ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb3";
        System.out.println(create_table_sql);
        this.setCreate_table_sql(create_table_sql);
    }

    private String getFlinkType(Column col) {
        return ColType.STRING.getValue().equals(col.getCol_type()) ? "STRING" : "INT";
    }

    public void buildCreateTableFlinkSql(String url, String username, String password) {

        List<Column> jcolList = this.getJoin_fields();
        StringBuffer colsBuf = new StringBuffer();

        String end = " , \n";

        colsBuf.append(" `")
                .append(primary_key.getCol_name())
                .append("` ")
                .append(getFlinkType(primary_key))
                .append(end);

        String delColName = del.getCol_name();
        colsBuf.append(" `").append(delColName).append("` ").append(getFlinkType(del)).append(end);

        for (Column col : jcolList) {
            if (col.equals(primary_key)) {
                continue;
            }
            String colName = col.getCol_name();
            colsBuf.append(" `").append(colName).append("` ").append(getFlinkType(col)).append(end);
        }

        List<Column> scolList = this.getSelect_fields();
        for (Column col : scolList) {
            if (col.equals(primary_key)) {
                continue;
            }
            String colName = col.getCol_name();
            colsBuf.append(" `").append(colName).append("` ").append(getFlinkType(col)).append(end);
        }

        String create_table_flink_sql =
                "CREATE TEMPORARY  TABLE "
                        + table_name
                        + " \n"
                        + "("
                        + colsBuf.toString()
                        + "PRIMARY KEY (`"
                        + primary_key.getCol_name()
                        + "`) NOT ENFORCED "
                        + ") WITH (\n"
                        + "      'connector' = 'jdbc',\n"
                        + "      'url' = '"
                        + url
                        + "',\n"
                        + "      'table-name' = '"
                        + this.table_name
                        + "',\n"
                        + "      'username' = '"
                        + username
                        + "',\n"
                        + "      'password' = '"
                        + password
                        + "'\n"
                        + ")";
        System.out.println(create_table_flink_sql);
        this.setCreate_table_flink_sql(create_table_flink_sql);
    }

    public void buildPrimaryName(Map<String, String> kpMap) {
        String pkName = Constant.PK;
        if (kpMap.containsKey(table_name)) {
            pkName = kpMap.get(table_name);
        }
        Column primary_key = new Column();
        primary_key.setCol_type(ColType.STRING.getValue());
        primary_key.setCol_name(pkName);
        this.setPrimary_key(primary_key);
    }

    public void buildMainKey(String mainKey) {
        Column col = new Column();
        col.setCol_name(mainKey);
        col.setCol_type(ColType.STRING.getValue());
        this.setMain_key(col);
    }

    public void buildMainKeySql(
            Map<String, Set<String>> tablesJoinFieldMap,
            Map<String, List<String>> joinTableFieldMap,
            String mainTableKey) {
        // cdc_biz_keeper_grade.bus_opp_num=cdc_biz_bus_opp.bus_opp_num,cdc_biz_bus_opp.house_id=cdc_biz_house.id
        List<String> myTableFieldList = geTableFieldList(tablesJoinFieldMap, this.table_name);
        List<String> joinPath = new ArrayList<>();
        List<String> mainKeyIndexFields =
                DimensionTableHelper.buildMainKeyIndexFields(mainTableKey, joinTableFieldMap);

        buildJoinPath(
                myTableFieldList,
                joinPath,
                mainKeyIndexFields,
                joinTableFieldMap,
                tablesJoinFieldMap);
        // System.out.println(joinPath);
        // [cdc_biz_house.id, cdc_biz_bus_opp.house_id, cdc_biz_bus_opp.bus_opp_num]

        Set<String> tableSet = new HashSet<>();
        StringBuffer joinBuf = new StringBuffer();

        String select = "";

        for (int i = 0; i < joinPath.size(); i++) {
            String tableField = joinPath.get(i);
            String[] tableInfo = tableField.split(Constant.REG_POINT);
            String tabName = tableInfo[0];
            String field = tableInfo[1];
            int num = tableSet.size();

            tableSet.add(tabName);

            int newNum = tableSet.size();
            if (newNum == 1) {
                String sql =
                        "select "
                                + field
                                + " from "
                                + tabName
                                + " where "
                                + tabName
                                + POINT
                                + this.getPrimary_key().getCol_name()
                                + " in (?)";
                joinBuf.append("(").append(sql).append(") ").append(tabName);
            } else if (num != newNum) {
                String beforeTableField = joinPath.get(i - 1);

                String sql = " join " + tabName + " on " + beforeTableField + " = " + tableField;
                joinBuf.append(sql);
            } else {
                select = "select " + tableField + " ";
            }
        }

        String sql = select + " from " + joinBuf.toString();
        // System.out.println(sql);
        this.setMain_key_sql(sql);
    }

    private List<String> geTableFieldList(
            Map<String, Set<String>> tablesFieldMap, String tableName) {
        Set<String> joinFields = tablesFieldMap.get(tableName);
        List<String> myTableFieldList = new ArrayList<>();
        for (String field : joinFields) {
            String tableField = tableName + POINT + field;
            myTableFieldList.add(tableField);
        }
        return myTableFieldList;
    }

    private void buildJoinPath(
            List<String> tableFieldList,
            List<String> joinPath,
            List<String> mainKeyIndexFields,
            Map<String, List<String>> joinTableFieldMap,
            Map<String, Set<String>> tablesFieldMap) {
        for (String tableField : tableFieldList) {
            if (joinPath.contains(tableField)) {
                continue;
            } else {
                joinPath.add(tableField);
            }
            if (mainKeyIndexFields.contains(tableField)) {
                break;
            }
            List<String> joinTableFieldList = joinTableFieldMap.get(tableField);
            if (joinTableFieldList == null || joinTableFieldList.size() == 0) {
                continue;
            }

            for (String jointableField : joinTableFieldList) {
                joinPath.add(jointableField);
                String tableName = jointableField.split(Constant.REG_POINT)[0];
                List<String> myTableFieldList = geTableFieldList(tablesFieldMap, tableName);
                buildJoinPath(
                        myTableFieldList,
                        joinPath,
                        mainKeyIndexFields,
                        joinTableFieldMap,
                        tablesFieldMap);
            }
        }
    }

    public List<String> findMainKeyBySql(Connection sql_connection, List<Map<String, String>> datas)
            throws Exception {

        return DimensionTableHelper.findMainKeyBySql(
                sql_connection, datas, main_key_sql, primary_key);
    }
}
