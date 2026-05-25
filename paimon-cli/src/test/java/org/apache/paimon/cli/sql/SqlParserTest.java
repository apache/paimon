/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.cli.sql;

import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.SqlSelect;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link SqlParser}. */
class SqlParserTest {

    private final SqlParser parser = new SqlParser();

    @Test
    void testSelectAll() {
        ParseResult result = parser.parse("SELECT * FROM mydb.users");
        assertThat(result.type()).isEqualTo(ParseResult.Type.QUERY);
        SqlSelect select = (SqlSelect) result.sqlNode();
        assertThat(select.getSelectList().get(0)).isInstanceOf(SqlIdentifier.class);
        assertThat(((SqlIdentifier) select.getSelectList().get(0)).isStar()).isTrue();
        assertThat(extractTableName(select)).isEqualTo("mydb.users");
    }

    @Test
    void testSelectColumns() {
        ParseResult result = parser.parse("SELECT id, name, age FROM mydb.users");
        SqlSelect select = (SqlSelect) result.sqlNode();
        SqlNodeList cols = select.getSelectList();
        assertThat(cols).hasSize(3);
        assertThat(((SqlIdentifier) cols.get(0)).getSimple()).isEqualTo("id");
        assertThat(((SqlIdentifier) cols.get(1)).getSimple()).isEqualTo("name");
        assertThat(((SqlIdentifier) cols.get(2)).getSimple()).isEqualTo("age");
    }

    @Test
    void testSelectWithWhere() {
        ParseResult result =
                parser.parse("SELECT * FROM mydb.users WHERE age > 18 AND city = 'Beijing'");
        SqlSelect select = (SqlSelect) result.sqlNode();
        assertThat(select.getWhere()).isNotNull();
        String where = SqlParser.unquoteIdentifiers(select.getWhere().toString());
        assertThat(where).contains("age").contains("18").contains("city").contains("Beijing");
    }

    @Test
    void testSelectWithLimit() {
        ParseResult result = parser.parse("SELECT * FROM mydb.users LIMIT 50");
        SqlNode node = result.sqlNode();
        if (node instanceof SqlOrderBy) {
            assertThat(((SqlOrderBy) node).fetch.toString()).isEqualTo("50");
        } else {
            SqlSelect select = (SqlSelect) node;
            assertThat(select.getFetch().toString()).isEqualTo("50");
        }
    }

    @Test
    void testSelectWithOrderBy() {
        ParseResult result = parser.parse("SELECT * FROM mydb.users ORDER BY age DESC LIMIT 10");
        SqlOrderBy orderBy = (SqlOrderBy) result.sqlNode();
        assertThat(orderBy.orderList).hasSize(1);
        SqlBasicCall descCall = (SqlBasicCall) orderBy.orderList.get(0);
        assertThat(descCall.getKind()).isEqualTo(SqlKind.DESCENDING);
        assertThat(orderBy.fetch.toString()).isEqualTo("10");
    }

    @Test
    void testSelectWithMultipleOrderBy() {
        ParseResult result =
                parser.parse(
                        "SELECT * FROM mydb.users ORDER BY city ASC, age DESC NULLS LAST LIMIT 20");
        SqlOrderBy orderBy = (SqlOrderBy) result.sqlNode();
        assertThat(orderBy.orderList).hasSize(2);
        assertThat(orderBy.fetch.toString()).isEqualTo("20");
    }

    @Test
    void testSelectWithGroupBy() {
        ParseResult result = parser.parse("SELECT city, COUNT(*) FROM mydb.users GROUP BY city");
        SqlSelect select = (SqlSelect) result.sqlNode();
        assertThat(select.getGroup()).isNotNull();
        assertThat(select.getGroup()).hasSize(1);
        assertThat(((SqlIdentifier) select.getGroup().get(0)).getSimple()).isEqualTo("city");
    }

    @Test
    void testSelectWithAggregates() {
        ParseResult result =
                parser.parse(
                        "SELECT city, SUM(age), AVG(age), MIN(age), MAX(age) "
                                + "FROM mydb.users GROUP BY city");
        SqlSelect select = (SqlSelect) result.sqlNode();
        SqlNodeList selectList = select.getSelectList();
        assertThat(selectList).hasSize(5);
        assertThat(((SqlBasicCall) selectList.get(1)).getOperator().getName()).isEqualTo("SUM");
        assertThat(((SqlBasicCall) selectList.get(2)).getOperator().getName()).isEqualTo("AVG");
        assertThat(((SqlBasicCall) selectList.get(3)).getOperator().getName()).isEqualTo("MIN");
        assertThat(((SqlBasicCall) selectList.get(4)).getOperator().getName()).isEqualTo("MAX");
    }

    @Test
    void testSelectDistinct() {
        ParseResult result = parser.parse("SELECT DISTINCT city FROM mydb.users");
        SqlSelect select = (SqlSelect) result.sqlNode();
        assertThat(select.isDistinct()).isTrue();
    }

    @Test
    void testSelectWithHaving() {
        ParseResult result =
                parser.parse(
                        "SELECT city, COUNT(*) FROM mydb.users GROUP BY city HAVING COUNT(*) > 5");
        SqlSelect select = (SqlSelect) result.sqlNode();
        assertThat(select.getHaving()).isNotNull();
    }

    @Test
    void testShowDatabases() {
        ParseResult result = parser.parse("SHOW DATABASES");
        assertThat(result.type()).isEqualTo(ParseResult.Type.SHOW_DATABASES);
    }

    @Test
    void testShowDatabasesWithSemicolon() {
        ParseResult result = parser.parse("SHOW DATABASES;");
        assertThat(result.type()).isEqualTo(ParseResult.Type.SHOW_DATABASES);
    }

    @Test
    void testShowTables() {
        ParseResult result = parser.parse("SHOW TABLES");
        assertThat(result.type()).isEqualTo(ParseResult.Type.SHOW_TABLES);
        assertThat(result.database()).isNull();
    }

    @Test
    void testShowTablesInDatabase() {
        ParseResult result = parser.parse("SHOW TABLES IN mydb");
        assertThat(result.type()).isEqualTo(ParseResult.Type.SHOW_TABLES);
        assertThat(result.database()).isEqualTo("mydb");
    }

    @Test
    void testUseDatabase() {
        ParseResult result = parser.parse("USE mydb");
        assertThat(result.type()).isEqualTo(ParseResult.Type.USE_DATABASE);
        assertThat(result.database()).isEqualTo("mydb");
    }

    @Test
    void testCaseInsensitive() {
        ParseResult result = parser.parse("select * from mydb.users where age > 10 limit 5");
        assertThat(result.type()).isEqualTo(ParseResult.Type.QUERY);
    }

    @Test
    void testBacktickIdentifier() {
        ParseResult result = parser.parse("SELECT `order`, `from` FROM mydb.`my-table`");
        SqlSelect select = (SqlSelect) result.sqlNode();
        SqlNodeList cols = select.getSelectList();
        assertThat(((SqlIdentifier) cols.get(0)).getSimple()).isEqualTo("order");
        assertThat(((SqlIdentifier) cols.get(1)).getSimple()).isEqualTo("from");
    }

    @Test
    void testInvalidSyntax() {
        assertThatThrownBy(() -> parser.parse("SELECTT * FROMM mydb.users"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("parse error");
    }

    @Test
    void testUnquoteIdentifiers() {
        assertThat(SqlParser.unquoteIdentifiers("`age` > 18")).isEqualTo("age > 18");
        assertThat(SqlParser.unquoteIdentifiers("`name` = 'O''Brien'"))
                .isEqualTo("name = 'O''Brien'");
        assertThat(SqlParser.unquoteIdentifiers("plain text")).isEqualTo("plain text");
    }

    private static String extractTableName(SqlSelect select) {
        SqlIdentifier id = (SqlIdentifier) select.getFrom();
        if (id.names.size() == 2) {
            return id.names.get(0) + "." + id.names.get(1);
        }
        return id.getSimple();
    }
}
