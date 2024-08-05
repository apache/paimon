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

import org.apache.paimon.spark.PaimonSparkTestBase

import org.apache.spark.sql.Row

abstract class PaimonShowColumnsTestBase extends PaimonSparkTestBase {

  test("Show columns from Paimon test") {
    spark.sql(s"""
                 |CREATE TABLE T (id STRING, name STRING, i INT, l LONG)
                 |USING PAIMON
                 |TBLPROPERTIES ('primary-key'='id')
                 |""".stripMargin)

    spark.sql(
      s"""
         |INSERT INTO T SELECT '1', 'aa', 12, 22
         |""".stripMargin
    )

    checkAnswer(
      spark.sql("SHOW COLUMNS FROM T"),
      Row("id") :: Row("name") :: Row("i") :: Row("l") :: Nil)

  }

}
