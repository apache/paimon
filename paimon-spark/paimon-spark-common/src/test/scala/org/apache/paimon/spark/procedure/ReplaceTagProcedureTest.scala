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

class ReplaceTagProcedureTest extends PaimonSparkTestBase {
  test("Paimon Procedure: replace tag to update tag meta") {
    spark.sql(s"""
                 |CREATE TABLE T (a INT, b STRING)
                 |TBLPROPERTIES ('primary-key'='a', 'bucket'='3')
                 |""".stripMargin)
    spark.sql("insert into T values(1, 'a')")
    spark.sql("insert into T values(2, 'b')")
    assertResult(2)(loadTable("T").snapshotManager().snapshotCount())

    // throw exception "Tag test_tag does not exist"
    assertThrows[IllegalArgumentException] {
      spark.sql("CALL paimon.sys.replace_tag(table => 'test.T', tag => 'test_tag')")
    }

    spark.sql("CALL paimon.sys.create_tag(table => 'test.T', tag => 'test_tag')")
    checkAnswer(
      spark.sql("select tag_name,snapshot_id,time_retained from `T$tags`"),
      Row("test_tag", 2, null) :: Nil)

    // replace tag with new time_retained
    spark.sql(
      "CALL paimon.sys.replace_tag(table => 'test.T', tag => 'test_tag', time_retained => '1 d')")
    checkAnswer(
      spark.sql("select tag_name,snapshot_id,time_retained from `T$tags`"),
      Row("test_tag", 2, "PT24H") :: Nil)

    // replace tag with new snapshot and time_retained
    spark.sql(
      "CALL paimon.sys.replace_tag(table => 'test.T', tag => 'test_tag', snapshot => 1, time_retained => '2 d')")
    checkAnswer(
      spark.sql("select tag_name,snapshot_id,time_retained from `T$tags`"),
      Row("test_tag", 1, "PT48H") :: Nil)
  }
}
