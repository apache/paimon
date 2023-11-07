package org.apache.paimon.flink.action.widetable.utils;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.*;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SQLParser {

    public static String parseAndJoinSelectFields(String sql) throws JSQLParserException {
        List<String> selectFields = parseSelectFields(sql);
        return String.join(", ", selectFields);
    }

    public static String parseAndJoinOnConditions(String sql) throws JSQLParserException {
        List<String> onConditions = parseOnConditions(sql);
        return String.join(", ", onConditions);
    }

    public static List<String> parseSelectFields(String sql) throws JSQLParserException {
        // 解析SQL语句
        Statement statement = CCJSqlParserUtil.parse(new StringReader(sql));
        List<String> selectFields = new ArrayList<>();

        if (statement instanceof Select) {
            Select select = (Select) statement;
            SelectBody selectBody = select.getSelectBody();

            if (selectBody instanceof PlainSelect) {
                PlainSelect plainSelect = (PlainSelect) selectBody;

                // 提取SELECT字段
                List selectItems = plainSelect.getSelectItems();
                for (Object selectItem : selectItems) {
                    String selectField = selectItem.toString();
                    selectFields.add(selectField);
                }
            }
        }

        return selectFields;
    }

    public static List<String> parseOnConditions(String sql) throws JSQLParserException {
        // 解析SQL语句
        Statement statement = CCJSqlParserUtil.parse(new StringReader(sql));
        List<String> onConditions = new ArrayList<>();

        if (statement instanceof Select) {
            Select select = (Select) statement;
            SelectBody selectBody = select.getSelectBody();

            if (selectBody instanceof PlainSelect) {
                PlainSelect plainSelect = (PlainSelect) selectBody;
                List<Join> joins = plainSelect.getJoins();

                if (joins != null) {
                    for (Join join : joins) {
                        String onCondition = join.getOnExpression().toString();
                        onConditions.add(onCondition);
                    }
                }
            }
        }

        return onConditions;
    }

    public static Map<String, String> findTableAliases(String sql) throws JSQLParserException {
        Map<String, String> tableAliasMap = new HashMap<>();
        Statement statement = CCJSqlParserUtil.parse(new StringReader(sql));

        if (statement instanceof Select) {
            Select select = (Select) statement;
            SelectBody selectBody = select.getSelectBody();

            if (selectBody instanceof PlainSelect) {
                PlainSelect plainSelect = (PlainSelect) selectBody;

                // 处理主查询表
                FromItem fromItem = plainSelect.getFromItem();
                if (fromItem instanceof Table) {
                    Table table = (Table) fromItem;
                    String tableName = table.getName();
                    String alias = table.getAlias() != null ? table.getAlias().getName() : "";
                    tableAliasMap.put(tableName, alias);
                }

                // 处理JOIN子句中的表
                if (plainSelect.getJoins() != null) {
                    for (Join join : plainSelect.getJoins()) {
                        FromItem joinFromItem = join.getRightItem();
                        if (joinFromItem instanceof Table) {
                            Table table = (Table) joinFromItem;
                            String tableName = table.getName();
                            String alias =
                                    table.getAlias() != null ? table.getAlias().getName() : "";
                            tableAliasMap.put(tableName, alias);
                        }
                    }
                }
            }
        }

        return tableAliasMap;
    }
}
