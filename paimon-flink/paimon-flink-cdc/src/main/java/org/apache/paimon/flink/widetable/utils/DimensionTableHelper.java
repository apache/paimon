package org.apache.paimon.flink.widetable.utils;

import org.apache.paimon.flink.widetable.bean.DimensionTable;
import org.apache.paimon.flink.widetable.utils.options.DimensionOptions;
import org.apache.paimon.flink.widetable.utils.options.SourceOptions;
import org.apache.paimon.flink.widetable.utils.options.SqlInfoOptions;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

public class DimensionTableHelper {

    public static String url;
    public static String username;
    public static String password;

    public static Connection getConnection() throws Exception {

        Connection sql_conn = null;
        // 定义数据库连接对象
        Properties info = new Properties(); // 定义Properties对象
        info.setProperty("user", username); // 设置Properties对象属
        info.setProperty("password", password);
        try {
            Class.forName("com.mysql.jdbc.Driver");
            sql_conn = DriverManager.getConnection(url, info);

            log.info("数据库连接成功！");
        } catch (Exception e) {
            log.error("数据库连接失败！", e);
            e.printStackTrace();
            throw new RuntimeException("Cannot connection to Mysql.", e);
        }
        return sql_conn; // 返回一个连接
    }

    public static void praseDimensionDbConnection(Configuration globConf) {
        String database = globConf.getString(DimensionOptions.DATABASE);
        Integer port = globConf.getInteger(DimensionOptions.PORT);
        String hostname = globConf.getString(DimensionOptions.HOSTNAME);
        username = globConf.getString(DimensionOptions.USERNAME);
        password = globConf.getString(DimensionOptions.PASSWORD);
        String urlParam = globConf.getString(DimensionOptions.URL_PARAM);
        String jdbcUrl = "jdbc:mysql://" + hostname + ":" + port;
        url = jdbcUrl + "/" + database + Constant.WEN_HAO + urlParam;
    }

    public static void buildData(
            DimensionTable ztable, PreparedStatement p, Map<String, String> data, boolean isdel)
            throws SQLException {
        List<Column> columns = ztable.buildColumns();
        Column del = ztable.getDel();
        for (int i = 1; i <= columns.size(); i++) {
            Column col = columns.get(i - 1);
            String colName = col.getCol_name();
            if (del.equals(col)) {
                int delVal = isdel ? DelType.DEL.getValue() : DelType.NOTDEL.getValue();
                setVal(p, i, col, delVal + "");
                continue;
            }
            String colVal = data.get(colName);
            setVal(p, i, col, colVal);
        }
    }

    public static StringBuffer buildReplaceSql(String tableName, List<Column> columns) {
        StringBuffer colsBuf = new StringBuffer();
        StringBuffer valBuf = new StringBuffer();
        for (int i = 0; i < columns.size(); i++) {
            Column col = columns.get(i);
            if (i == 0) {
                colsBuf.append(col.getCol_name());
                valBuf.append(Constant.WEN_HAO);
            } else {
                colsBuf.append(Constant.DOU_HAO).append(col.getCol_name());
                valBuf.append(Constant.DOU_HAO).append(Constant.WEN_HAO);
            }
        }
        StringBuffer replace = new StringBuffer();
        replace.append("replace into ")
                .append(tableName)
                .append("(")
                .append(colsBuf.toString())
                .append(") values(")
                .append(valBuf.toString())
                .append(")");
        return replace;
    }

    public static List<String> findMainKeyBySql(
            Connection sql_connection,
            List<Map<String, String>> datas,
            String main_key_sql,
            Column primaryKey)
            throws SQLException {
        List<String> mainKeys = new ArrayList<>();
        if (StringUtils.isEmpty(main_key_sql)) {
            System.out.println("findMainKeyBySql >> main_key_sql is null");
        }

        main_key_sql = joinMainKeySql(datas, main_key_sql);
        // System.out.println(main_key_sql);
        PreparedStatement p = sql_connection.prepareStatement(main_key_sql);
        appendPrimaryKeyData(datas, primaryKey, p);

        ResultSet rs = p.executeQuery();

        while (rs.next()) {
            String mainKey = rs.getString(1);
            mainKeys.add(mainKey);
        }

        p.close();
        rs.close();
        return mainKeys;
    }

    private static void appendPrimaryKeyData(
            List<Map<String, String>> datas, Column primaryKey, PreparedStatement p)
            throws SQLException {
        for (int i = 1; i <= datas.size(); i++) {
            Map<String, String> data = datas.get(i - 1);
            String colVal = data.get(primaryKey.getCol_name());
            setVal(p, i, primaryKey, colVal);
        }
    }

    private static String joinMainKeySql(List<Map<String, String>> datas, String main_key_sql) {
        StringBuffer valBuf = new StringBuffer();
        for (int i = 0; i < datas.size(); i++) {
            if (i == 0) {
                valBuf.append(Constant.WEN_HAO);
            } else {
                valBuf.append(Constant.DOU_HAO).append(Constant.WEN_HAO);
            }
        }
        return main_key_sql.replace(Constant.WEN_HAO, valBuf.toString());
    }

    public static void setVal(PreparedStatement p, int index, Column col, String colVal)
            throws SQLException {
        if (ColType.STRING.getValue().equals(col.getCol_type().toLowerCase())) {
            p.setString(index, colVal);
        } else if (ColType.INT.getValue().equals(col.getCol_type().toLowerCase())) {
            Integer intVal = 0;
            if (intVal != null) {
                intVal = Integer.parseInt(colVal);
            }
            p.setInt(index, intVal);
        } else {
            log.error("setVal >>  数据类型不识别：" + col.getCol_type());
        }
    }

    public static Map<String, DimensionTable> buildTableMap(Configuration globConf) {

        DimensionTableHelper.praseDimensionDbConnection(globConf);

        String dbtables = globConf.getString(SourceOptions.INCLUDING_TABLES);
        String mainTableKey = globConf.getString(SqlInfoOptions.MAIN_TABLE_KEY);
        String selects = globConf.getString(SqlInfoOptions.SELECT_FIELDS);
        String joinFields = globConf.getString(SqlInfoOptions.JOIN_FIELDS);

        Map<String, String> kpMap = ParamUitl.praseKpMap(dbtables);
        String tables = ParamUitl.praseTables(dbtables);

        String[] tableArr = tables.split(Constant.DOU_HAO);
        List<String> tableList = new ArrayList<>();
        // 改写输入 将db.table --> db_table
        for (String table : tableArr) {
            String dbtable = table.replace(Constant.POINT, Constant.XIALINE);

            String before = table + Constant.POINT;
            String after = dbtable + Constant.POINT;
            if (mainTableKey.contains(before)) {
                mainTableKey = mainTableKey.replace(before, after);
            }
            if (selects.contains(before)) {
                selects = selects.replace(before, after);
            }
            if (joinFields.contains(before)) {
                joinFields = joinFields.replace(before, after);
            }
            tableList.add(dbtable);
        }

        String[] selectArr = selects.split(Constant.DOU_HAO);
        String[] joins = joinFields.split(Constant.DOU_HAO);

        Map<String, Set<String>> tablesJoinFieldMap = new HashMap<>();
        Map<String, List<String>> joinTableFieldMap = new HashMap<>();
        List<String> joinTableSortList = new ArrayList<>();
        for (String join : joins) {
            String[] fields = join.split(Constant.DENG_HAO);
            buildJoinTableSortList(joinTableSortList, fields);

            buildLeftAndRightJoinTableFieldMap(joinTableFieldMap, fields);
            // join表和字段映射构建
            buildTableJoinFieldMap(tablesJoinFieldMap, fields);
        }
        // select 表和字段映射构建
        Map<String, Set<String>> tablesSelectFieldMap = new HashMap<>();
        buildTableSelectFieldMap(tablesJoinFieldMap, tablesSelectFieldMap, selectArr);

        Map<String, String> tableMainKeyMap = buildTableMainKeyMap(mainTableKey, joinTableFieldMap);

        Map<String, DimensionTable> tablesMap = new HashMap<>();
        for (String tableName : tableList) {
            DimensionTable indeTable =
                    buildIndexTable(
                            tablesJoinFieldMap,
                            tableMainKeyMap,
                            joinTableFieldMap,
                            kpMap,
                            tableName,
                            mainTableKey,
                            tablesSelectFieldMap);
            tablesMap.put(tableName, indeTable);
        }

        String insert_flink_sql =
                globConf.getString(SqlInfoOptions.INSERT_FLINK_SQL) != null
                        ? globConf.getString(SqlInfoOptions.INSERT_FLINK_SQL)
                        : buildInsertFlinkSql(mainTableKey, selects, joins, joinTableSortList);
        globConf.set(SqlInfoOptions.INSERT_FLINK_SQL, insert_flink_sql);
        return tablesMap;
    }

    private static String buildInsertFlinkSql(
            String mainTableKey,
            String selectFields,
            String[] joins,
            List<String> joinTableSortList) {
        String mainTable = mainTableKey.split(Constant.REG_POINT)[0];
        String insert_flink_sql =
                "insert into " + SinkTable.SINK_TABLENAME + " select\n" + selectFields + ",\n(";

        insert_flink_sql = getSystemDelCol(joinTableSortList, insert_flink_sql);

        insert_flink_sql =
                insert_flink_sql
                        + ") as "
                        + Constant.IS_SYS_DEL
                        + " \n"
                        + "from (select main_key,PROCTIME() as proctime  from "
                        + Constant.CHANGELOG_TABLENAME
                        + " ) c  \n"
                        + "  join "
                        + mainTable
                        + " FOR SYSTEM_TIME AS OF c.proctime  \n"
                        + "on c.main_key = "
                        + mainTableKey
                        + "  \n";

        List<String> finishTable = new ArrayList<>();
        finishTable.add(mainTable);
        joinTableSortList.remove(mainTable);
        StringBuffer leftJoinSql = new StringBuffer();
        while (joinTableSortList.size() > 0) {
            leftJoinSql.append("  left join ");
            String currentTable = joinTableSortList.get(0);
            leftJoinSql.append(currentTable).append(" FOR SYSTEM_TIME AS OF c.proctime  \n on ");
            Set<String> onList = new HashSet<>();
            for (String join : joins) {
                String[] fields = join.split(Constant.DENG_HAO);
                String leftField = fields[0];
                String rightField = fields[1];
                String ltable = leftField.split(Constant.REG_POINT)[0];
                String rtable = rightField.split(Constant.REG_POINT)[0];
                if (finishTable.contains(ltable) && currentTable.equals(rtable)) {
                    onList.add(leftField + " " + Constant.DENG_HAO + " " + rightField);
                }
                if (finishTable.contains(rtable) && currentTable.equals(ltable)) {
                    onList.add(rightField + " " + Constant.DENG_HAO + " " + leftField);
                }
            }
            int j = 0;
            for (String onItem : onList) {
                if (j != 0) {
                    leftJoinSql.append(" and ");
                }
                leftJoinSql.append(onItem);
                j++;
            }
            leftJoinSql.append(" \n ");

            finishTable.add(currentTable);
            joinTableSortList.remove(currentTable);
        }
        insert_flink_sql = insert_flink_sql + leftJoinSql.toString();
        return insert_flink_sql;
    }

    private static String getSystemDelCol(List<String> joinTableSortList, String insert_flink_sql) {
        StringBuffer delCol = new StringBuffer();
        int i = 0;
        for (String table : joinTableSortList) {
            if (i != 0) {
                delCol.append(" + ");
            }
            delCol.append(table).append(Constant.POINT).append(Constant.IS_SYS_DEL);
            i++;
        }
        insert_flink_sql = insert_flink_sql + delCol.toString();
        return insert_flink_sql;
    }

    private static void buildJoinTableSortList(List<String> joinTableSort, String[] fields) {
        String leftField = fields[0];
        String rightField = fields[1];
        String ltable = leftField.split(Constant.REG_POINT)[0];
        String rtable = rightField.split(Constant.REG_POINT)[0];
        if (!joinTableSort.contains(ltable)) {
            joinTableSort.add(ltable);
        }
        if (!joinTableSort.contains(rtable)) {
            joinTableSort.add(rtable);
        }
    }

    private static void buildTableSelectFieldMap(
            Map<String, Set<String>> joinTableFieldMap,
            Map<String, Set<String>> tablesSelectFieldMap,
            String[] indexFields) {
        for (String indexField : indexFields) {
            String[] tableField = indexField.split(Constant.REG_POINT);
            String tableName = tableField[0];
            String field = tableField[1];
            // 如果关联字段已经包含 跳过保证不重复添加
            if (joinTableFieldMap.get(tableName).contains(field)) {
                continue;
            }
            if (!tablesSelectFieldMap.containsKey(tableName)) {
                tablesSelectFieldMap.put(tableName, new HashSet<>());
            }
            tablesSelectFieldMap.get(tableName).add(field);
        }
    }

    private static void buildTableJoinFieldMap(
            Map<String, Set<String>> joinTableFieldMap, String[] indexFields) {
        for (String indexField : indexFields) {
            String[] tableField = indexField.split(Constant.REG_POINT);
            String tableName = tableField[0];
            String field = tableField[1];

            if (!joinTableFieldMap.containsKey(tableName)) {
                joinTableFieldMap.put(tableName, new HashSet<>());
            }
            joinTableFieldMap.get(tableName).add(field);
        }
    }

    private static void buildLeftAndRightJoinTableFieldMap(
            Map<String, List<String>> indexFieldMap, String[] indexFields) {
        String leftField = indexFields[0];
        String rightField = indexFields[1];
        if (!indexFieldMap.containsKey(leftField)) {
            indexFieldMap.put(leftField, new ArrayList<>());
        }
        indexFieldMap.get(leftField).add(rightField);
        if (!indexFieldMap.containsKey(rightField)) {
            indexFieldMap.put(rightField, new ArrayList<>());
        }
        indexFieldMap.get(rightField).add(leftField);
    }

    private static Map<String, String> buildTableMainKeyMap(
            String mainTableKey, Map<String, List<String>> joinTableFieldMap) {
        List<String> mainKeyIndexFields = buildMainKeyIndexFields(mainTableKey, joinTableFieldMap);
        Map<String, String> tableMainKeyMap = new HashMap<>();
        for (String mainKeyTableField : mainKeyIndexFields) {
            String[] mainKeyTableFieldArr = mainKeyTableField.split(Constant.REG_POINT);
            String tableName = mainKeyTableFieldArr[0];
            String mainKeyField = mainKeyTableFieldArr[1];
            tableMainKeyMap.put(tableName, mainKeyField);
        }
        return tableMainKeyMap;
    }

    public static List<String> buildMainKeyIndexFields(
            String mainTableKey, Map<String, List<String>> joinTableFieldMap) {
        List<String> joinIndexFields = joinTableFieldMap.get(mainTableKey);
        List<String> mainKeyIndexFields = new ArrayList<>();
        mainKeyIndexFields.addAll(joinIndexFields);
        mainKeyIndexFields.add(mainTableKey);
        return mainKeyIndexFields;
    }

    private static DimensionTable buildIndexTable(
            Map<String, Set<String>> tablesJoinFieldMap,
            Map<String, String> tableMainKeyMap,
            Map<String, List<String>> joinTableFieldMap,
            Map<String, String> kpMap,
            String tableName,
            String mainTableKey,
            Map<String, Set<String>> tablesSelectFieldMap) {
        DimensionTable dtable = new DimensionTable();
        dtable.setTable_name(tableName);

        // setJoin_Fields
        dtable.buildJoinFields(tablesJoinFieldMap);

        // setSelect_Fields
        dtable.buildSelectFileds(tablesSelectFieldMap);

        // setPrimary_key
        dtable.buildPrimaryName(kpMap);

        // dtable.setCreate_table_sql
        dtable.buildCreateTableSql();

        // dtable.buildCreateTableFlinkSql
        dtable.buildCreateTableFlinkSql(url, username, password);

        // dtable.setMain_key
        String mainKey = tableMainKeyMap.get(tableName);
        if (mainKey != null) {
            dtable.buildMainKey(mainKey);
        } else {
            // dtable.setMain_key_sql

            dtable.buildMainKeySql(tablesJoinFieldMap, joinTableFieldMap, mainTableKey);
        }
        return dtable;
    }
}
