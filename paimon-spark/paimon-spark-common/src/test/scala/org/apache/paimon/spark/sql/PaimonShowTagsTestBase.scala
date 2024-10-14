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

abstract class PaimonShowTagsTestBase extends PaimonSparkTestBase {

  test("Paimon DDL: show tags for table") {
    spark.sql("""CREATE TABLE T (id INT, name STRING)
                |USING PAIMON
                |TBLPROPERTIES ('primary-key'='id')""".stripMargin)

    spark.sql("insert into T values(1, 'a')")
    spark.sql("insert into T values(2, 'b')")

    spark.sql("CALL paimon.sys.create_tag(table => 'test.T', tag => '2024-10-12')")
    spark.sql("CALL paimon.sys.create_tag(table => 'test.T', tag => '2024-10-11')")
    spark.sql("CALL paimon.sys.create_tag(table => 'test.T', tag => '2024-10-13')")

    checkAnswer(
      spark.sql("show tags T"),
      Row("2024-10-11") :: Row("2024-10-12") :: Row("2024-10-13") :: Nil)
  }
}
