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
package org.apache.paimon.spark.sql

import org.junit.jupiter.api.Assertions

class AnalyzeTableTest extends AnalyzeTableTestBase {

  test("Paimon analyze: spark use col stats") {
    spark.sql(s"""
                 |CREATE TABLE T (id STRING, name STRING, i INT, l LONG)
                 |USING PAIMON
                 |TBLPROPERTIES ('primary-key'='id')
                 |""".stripMargin)

    spark.sql(s"INSERT INTO T VALUES ('1', 'a', 1, 1)")
    spark.sql(s"INSERT INTO T VALUES ('2', 'aaa', 1, 2)")
    spark.sql(s"ANALYZE TABLE T COMPUTE STATISTICS FOR ALL COLUMNS")

    val stats = getScanStatistic("SELECT * FROM T")
    Assertions.assertEquals(2L, stats.rowCount.get.longValue())
    // spark 33' v2 table not support col stats
    Assertions.assertEquals(0, stats.attributeStats.size)
  }

  test("Paimon analyze: partition filter push down hit") {
    spark.sql(s"""
                 |CREATE TABLE T (id INT, name STRING, pt INT)
                 |TBLPROPERTIES ('primary-key'='id, pt', 'bucket'='2')
                 |PARTITIONED BY (pt)
                 |""".stripMargin)

    spark.sql("INSERT INTO T VALUES (1, 'a', 1), (2, 'b', 1), (3, 'c', 2), (4, 'd', 3)")
    spark.sql(s"ANALYZE TABLE T COMPUTE STATISTICS FOR ALL COLUMNS")

    // partition push down hit
    var sql = "SELECT * FROM T WHERE pt < 1"
    // spark 33' v2 table not support col stats
    Assertions.assertEquals(4L, getScanStatistic(sql).rowCount.get.longValue())
    checkAnswer(spark.sql(sql), Nil)

    // partition push down not hit
    sql = "SELECT * FROM T WHERE id < 1"
    Assertions.assertEquals(4L, getScanStatistic(sql).rowCount.get.longValue())
    checkAnswer(spark.sql(sql), Nil)
  }
}
