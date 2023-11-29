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
package org.apache.paimon.spark.procedure

import org.apache.paimon.spark.PaimonHiveTestBase

import org.apache.spark.sql.Row

class MigrateTableProcedureTest extends PaimonHiveTestBase {
  Seq("parquet", "orc").foreach(
    format => {
      test(s"Paimon migrate table procedure: migrate $format non-partitioned table") {
        val hiveTbl = s"${format}_hive_tbl"
        withTable(hiveTbl) {
          // create hive table
          spark.sql(s"""
                       |CREATE TABLE $hiveTbl (id STRING, name STRING, pt STRING)
                       |USING $format
                       |""".stripMargin)

          spark.sql(s"INSERT INTO $hiveTbl VALUES ('1', 'a', 'p1'), ('2', 'b', 'p2')")

          checkAnswer(
            spark.sql(s"SELECT * FROM $hiveTbl ORDER BY id"),
            Row("1", "a", "p1") :: Row("2", "b", "p2") :: Nil)

          spark.sql(
            s"CALL sys.migrate_table(format => 'hive', table => '$hiveDbName.$hiveTbl', tblproperties => 'file.format=$format')")

          checkAnswer(
            spark.sql(s"SELECT * FROM $hiveTbl ORDER BY id"),
            Row("1", "a", "p1") :: Row("2", "b", "p2") :: Nil)
        }
      }
    })

  Seq("parquet", "orc").foreach(
    format => {
      test(s"Paimon migrate table procedure: migrate $format partitioned table") {
        val hiveTbl = s"${format}_hive_pt_tbl"
        withTable(hiveTbl) {
          // create hive table
          spark.sql(s"""
                       |CREATE TABLE $hiveTbl (id STRING, name STRING, pt STRING)
                       |USING $format
                       |PARTITIONED BY (pt)
                       |""".stripMargin)

          spark.sql(s"INSERT INTO $hiveTbl VALUES ('1', 'a', 'p1'), ('2', 'b', 'p2')")

          checkAnswer(
            spark.sql(s"SELECT * FROM $hiveTbl ORDER BY id"),
            Row("1", "a", "p1") :: Row("2", "b", "p2") :: Nil)

          spark.sql(
            s"CALL sys.migrate_table(format => 'hive', table => '$hiveDbName.$hiveTbl', tblproperties => 'file.format=$format')")

          checkAnswer(
            spark.sql(s"SELECT * FROM $hiveTbl ORDER BY id"),
            Row("1", "a", "p1") :: Row("2", "b", "p2") :: Nil)
        }
      }
    })
}
