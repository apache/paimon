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

abstract class PaimonTagDdlTestBase extends PaimonSparkTestBase {
  test("Tag ddl: show tags syntax") {
    spark.sql("""CREATE TABLE T (id INT, name STRING)
                |USING PAIMON
                |TBLPROPERTIES ('primary-key'='id')""".stripMargin)

    spark.sql("insert into T values(1, 'a')")

    spark.sql("alter table T create tag `2024-10-12`")
    spark.sql("alter table T create tag `2024-10-11`")
    spark.sql("alter table T create tag `2024-10-13`")

    checkAnswer(
      spark.sql("show tags T"),
      Row("2024-10-11") :: Row("2024-10-12") :: Row("2024-10-13") :: Nil)
  }

  test("Tag ddl: alter table t crete tag syntax") {
    spark.sql("""CREATE TABLE T (id INT, name STRING)
                |USING PAIMON
                |TBLPROPERTIES ('primary-key'='id')""".stripMargin)

    spark.sql("insert into T values(1, 'a')")
    spark.sql("insert into T values(2, 'b')")
    spark.sql("insert into T values(3, 'c')")
    val table = loadTable("T")
    assertResult(3)(table.snapshotManager().snapshotCount())

    spark.sql("alter table T create tag `tag-1`")
    spark.sql("alter table T create tag `tag-2` RETAIN 2 DAYS")
    spark.sql("alter table T create tag `tag-3` as of version 1")
    spark.sql("alter table T create tag `tag-4` as of version 2 RETAIN 3 HOURS")
    assertResult(4)(spark.sql("show tags T").count())

    checkAnswer(
      spark.sql("select tag_name,snapshot_id,time_retained from `T$tags`"),
      Row("tag-1", 3, null) :: Row("tag-2", 3, "PT48H") :: Row("tag-3", 1, null) :: Row(
        "tag-4",
        2,
        "PT3H") :: Nil
    )

    // not update tag with 'if not exists' syntax
    spark.sql("alter table T create tag if not exists `tag-1` RETAIN 10 HOURS")
    checkAnswer(
      spark.sql("select tag_name,snapshot_id,time_retained from `T$tags` where tag_name='tag-1'"),
      Row("tag-1", 3, "PT1H"))
  }

  test("Tag ddl: alter table t delete tag syntax") {
    spark.sql("""CREATE TABLE T (id INT, name STRING)
                |USING PAIMON
                |TBLPROPERTIES ('primary-key'='id')""".stripMargin)

    spark.sql("insert into T values(1, 'a')")
    assertResult(1)(loadTable("T").snapshotManager().snapshotCount())

    spark.sql("alter table T create tag `2024-10-12`")
    spark.sql("alter table T create tag `2024-10-15`")
    spark.sql("alter table T create tag `2024-10-13`")
    spark.sql("alter table T create tag `2024-10-14`")
    checkAnswer(
      spark.sql("show tags T"),
      Row("2024-10-12") :: Row("2024-10-13") :: Row("2024-10-14") :: Row("2024-10-15") :: Nil)

    spark.sql("alter table T delete tag `2024-10-12`")
    checkAnswer(
      spark.sql("show tags T"),
      Row("2024-10-13") :: Row("2024-10-14") :: Row("2024-10-15") :: Nil)

    spark.sql("alter table T delete tag `2024-10-13, 2024-10-14`")
    checkAnswer(spark.sql("show tags T"), Row("2024-10-15") :: Nil)

    spark.sql("alter table T delete tag if EXISTS `2024-10-18`")
    checkAnswer(spark.sql("show tags T"), Row("2024-10-15") :: Nil)
  }

  test("Tag ddl: alter table t rename tag syntax") {
    spark.sql("""CREATE TABLE T (id INT, name STRING)
                |USING PAIMON
                |TBLPROPERTIES ('primary-key'='id')""".stripMargin)

    spark.sql("insert into T values(1, 'a')")
    assertResult(1)(loadTable("T").snapshotManager().snapshotCount())

    spark.sql("alter table T create tag `tag-1`")
    checkAnswer(spark.sql("show tags T"), Row("tag-1"))

    spark.sql("alter table T rename tag `tag-1` to `tag-2`")
    checkAnswer(spark.sql("show tags T"), Row("tag-2"))
  }
}
