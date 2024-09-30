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

package org.apache.paimon.flink;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.stats.ColStats;
import org.apache.paimon.stats.Statistics;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.sql.Timestamp;
import java.util.Map;

/** IT cases for analyze table. */
public class FlinkAnalyzeTableITCase extends CatalogITCaseBase {

    @Test
    public void testAnalyzeTable() throws Catalog.TableNotExistException {
        sql(
                "CREATE TABLE T ("
                        + " id STRING"
                        + ", name STRING"
                        + ", i INT"
                        + ", l bigint"
                        + ", PRIMARY KEY (id) NOT ENFORCED"
                        + " ) WITH ("
                        + " 'bucket' = '2'"
                        + " )");
        sql("INSERT INTO T VALUES ('1', 'a', 1, 1)");
        sql("INSERT INTO T VALUES ('2', 'aaa', 1, 2)");
        sql("ANALYZE TABLE T COMPUTE STATISTICS");
        Statistics stats = paimonTable("T").statistics().get();

        Assertions.assertEquals(2L, stats.mergedRecordCount().getAsLong());
        Assertions.assertTrue(stats.mergedRecordSize().isPresent());
        Assertions.assertTrue(stats.colStats().isEmpty());
    }

    @Test
    public void testAnalyzeTableColumn() throws Catalog.TableNotExistException {

        sql(
                "CREATE TABLE T ("
                        + "id STRING, name STRING, bytes_col BYTES, int_col INT, long_col bigint,\n"
                        + "float_col FLOAT, double_col DOUBLE, decimal_col DECIMAL(10, 5), boolean_col BOOLEAN, date_col DATE,\n"
                        + "timestamp_col TIMESTAMP_LTZ, binary_col BINARY, varbinary_col VARBINARY, char_col CHAR(20), varchar_col VARCHAR(20),\n"
                        + "tinyint_col TINYINT, smallint_col SMALLINT"
                        + ", PRIMARY KEY (id) NOT ENFORCED"
                        + " ) WITH ("
                        + " 'bucket' = '2'"
                        + " )");
        sql(
                "INSERT INTO T VALUES ('1', 'a', CAST('your_binary_data' AS BYTES), 1, 1, 1.0, 1.0, 13.12345, true, cast('2020-01-01' as date), cast('2024-01-01 00:00:00' as TIMESTAMP_LTZ), CAST('example binary1' AS BINARY), CAST('example binary1' AS VARBINARY), 'a', 'a',CAST(1 AS TINYINT), CAST(2 AS SMALLINT))");
        sql(
                "INSERT INTO T VALUES ('2', 'aaa', CAST('your_binary_data' AS BYTES), 1, 1, 1.0, 5.0, 12.12345, true, cast('2021-01-02' as date), cast('2024-01-02 00:00:00' as TIMESTAMP_LTZ), CAST('example binary1' AS BINARY), CAST('example binary1' AS VARBINARY), 'aaa', 'aaa', CAST(2 AS TINYINT), CAST(4 AS SMALLINT))");

        sql(
                "INSERT INTO T VALUES ('3', 'bbbb', CAST('data' AS BYTES), 4, 19, 7.0, 1.0, 14.12345, true, cast(NULL as date), cast('2024-01-02 05:00:00' as TIMESTAMP_LTZ), CAST(NULL AS BINARY), CAST('example binary1' AS VARBINARY), 'aaa', 'aaa', CAST(NULL AS TINYINT), CAST(4 AS SMALLINT))");

        sql(
                "INSERT INTO T VALUES ('4', 'aa', CAST(NULL AS BYTES), 1, 1, 1.0, 1.0, 14.12345, false, cast(NULL as date), cast(NULL as TIMESTAMP_LTZ), CAST(NULL AS BINARY), CAST('example' AS VARBINARY), 'aba', 'aaab', CAST(NULL AS TINYINT), CAST(4 AS SMALLINT))");

        sql("ANALYZE TABLE T COMPUTE STATISTICS FOR ALL COLUMNS");

        Statistics stats = paimonTable("T").statistics().get();

        Assertions.assertEquals(4L, stats.mergedRecordCount().getAsLong());

        Map<String, ColStats<?>> colStats = stats.colStats();
        Assertions.assertEquals(
                ColStats.newColStats(0, 4L, null, null, 0L, 1L, 1L), colStats.get("id"));
        Assertions.assertEquals(
                ColStats.newColStats(1, 4L, null, null, 0L, 2L, 4L), colStats.get("name"));

        Assertions.assertEquals(
                ColStats.newColStats(2, null, null, null, 1L, null, null),
                colStats.get("bytes_col"));

        Assertions.assertEquals(
                ColStats.newColStats(3, 2L, new Integer(1), new Integer(4), 0L, null, null),
                colStats.get("int_col"));

        Assertions.assertEquals(
                ColStats.newColStats(4, 2L, 1L, 19L, 0L, null, null), colStats.get("long_col"));

        Assertions.assertEquals(
                ColStats.newColStats(5, 2L, 1.0f, 7.0f, 0L, null, null), colStats.get("float_col"));

        Assertions.assertEquals(
                ColStats.newColStats(6, 2L, 1.0d, 5.0d, 0L, null, null),
                colStats.get("double_col"));

        Assertions.assertEquals(
                ColStats.newColStats(
                        7,
                        3L,
                        Decimal.fromBigDecimal(new java.math.BigDecimal("12.12345"), 10, 5),
                        Decimal.fromBigDecimal(new java.math.BigDecimal("14.12345"), 10, 5),
                        0L,
                        null,
                        null),
                colStats.get("decimal_col"));

        Assertions.assertEquals(
                ColStats.newColStats(8, 2L, null, null, 0L, null, null),
                colStats.get("boolean_col"));

        Assertions.assertEquals(
                ColStats.newColStats(9, 2L, 18262, 18629, 2L, null, null),
                colStats.get("date_col"));

        Assertions.assertEquals(
                ColStats.newColStats(
                        10,
                        3L,
                        org.apache.paimon.data.Timestamp.fromSQLTimestamp(
                                new Timestamp(1704038400000L)),
                        org.apache.paimon.data.Timestamp.fromSQLTimestamp(
                                new Timestamp(1704142800000L)),
                        1L,
                        null,
                        null),
                colStats.get("timestamp_col"));

        Assertions.assertEquals(
                ColStats.newColStats(11, null, null, null, 2L, null, null),
                colStats.get("binary_col"));

        Assertions.assertEquals(
                ColStats.newColStats(12, null, null, null, 0L, null, null),
                colStats.get("varbinary_col"));

        Assertions.assertEquals(
                ColStats.newColStats(13, 3L, null, null, 0L, 20L, 20L), colStats.get("char_col"));

        Assertions.assertEquals(
                ColStats.newColStats(14, 3L, null, null, 0L, 2L, 4L), colStats.get("varchar_col"));

        Assertions.assertEquals(
                ColStats.newColStats(
                        15,
                        2L,
                        new Integer(1).byteValue(),
                        new Integer(2).byteValue(),
                        2L,
                        null,
                        null),
                colStats.get("tinyint_col"));

        Assertions.assertEquals(
                ColStats.newColStats(
                        16,
                        2L,
                        new Integer(2).shortValue(),
                        new Integer(4).shortValue(),
                        0L,
                        null,
                        null),
                colStats.get("smallint_col"));
    }
}
