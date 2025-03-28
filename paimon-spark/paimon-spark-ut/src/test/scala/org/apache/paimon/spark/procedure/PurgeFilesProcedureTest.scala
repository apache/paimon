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

import org.apache.paimon.spark.PaimonSparkTestBase

import org.apache.spark.sql.Row

class PurgeFilesProcedureTest extends PaimonSparkTestBase {

  test("Paimon procedure: purge files test") {
    spark.sql(s"""
                 |CREATE TABLE T (id STRING, name STRING)
                 |USING PAIMON
                 |""".stripMargin)

    spark.sql("insert into T select '1', 'aa'");
    checkAnswer(spark.sql("select * from test.T"), Row("1", "aa") :: Nil)

    spark.sql("CALL paimon.sys.purge_files(table => 'test.T')")
    checkAnswer(spark.sql("select * from test.T"), Nil)

    spark.sql("insert into T select '2', 'aa'");
    checkAnswer(spark.sql("select * from test.T"), Row("2", "aa") :: Nil)
  }

  test("Paimon procedure: purge files test with dry run.") {
    spark.sql(s"""
                 |CREATE TABLE T (id STRING, name STRING)
                 |USING PAIMON
                 |""".stripMargin)

    // There are no dir to be deleted.
    checkAnswer(
      spark.sql("CALL paimon.sys.purge_files(table => 'test.T')"),
      Row("There are no dir to be deleted.") :: Nil
    )

    spark.sql("insert into T select '1', 'aa'");
    checkAnswer(spark.sql("select * from test.T"), Row("1", "aa") :: Nil)

    // dry run.
    checkAnswer(
      spark.sql("CALL paimon.sys.purge_files(table => 'test.T', dry_run => true)"),
      Row("snapshot") :: Row("bucket-0") :: Row("manifest") :: Nil
    )
    checkAnswer(spark.sql("select * from test.T"), Row("1", "aa") :: Nil)

    // Do delete.
    spark.sql("CALL paimon.sys.purge_files(table => 'test.T')")
    checkAnswer(spark.sql("select * from test.T"), Nil)

    // insert new data.
    spark.sql("insert into T select '2', 'aa'");
    checkAnswer(spark.sql("select * from test.T"), Row("2", "aa") :: Nil)
  }

}
