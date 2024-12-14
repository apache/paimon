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

abstract class PaimonPurgeTableTestBase extends PaimonSparkTestBase {

  test("purge table test") {
    spark.sql("""CREATE TABLE T (id INT, name STRING)
                |USING PAIMON
                |TBLPROPERTIES ('primary-key'='id', 'file.format'='orc')""".stripMargin)

    spark.sql(
      "insert into T select 1, 'aa'"
    )

    checkAnswer(
      spark.sql("select * from `T`"),
      Row(1, "aa") :: Nil
    )

    // test for purge table
    spark.sql(
      "PURGE TABLE T"
    )

    spark.sql(
      "REFRESH TABLE T"
    )

    checkAnswer(
      spark.sql("select * from `T`"),
      Nil
    )

    spark.sql(
      "insert into T select 2, 'bb'"
    )

    checkAnswer(
      spark.sql("select * from `T`"),
      Row(2, "bb") :: Nil
    )

  }

}
