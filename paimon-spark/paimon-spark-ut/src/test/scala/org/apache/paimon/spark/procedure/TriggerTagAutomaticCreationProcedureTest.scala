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
import org.assertj.core.api.Assertions.assertThat

class TriggerTagAutomaticCreationProcedureTest extends PaimonSparkTestBase {

  test("Paimon procedure: trigger tag automatic creation test") {
    spark.sql("""CREATE TABLE T_FORCE_AUTO_TAG (id INT, name STRING)
                |USING PAIMON
                |TBLPROPERTIES (
                |'primary-key'='id'
                |)""".stripMargin)

    spark.sql("insert into T_FORCE_AUTO_TAG values(1, 'a')")

    val table = loadTable("T_FORCE_AUTO_TAG")
    assertResult(1)(table.snapshotManager().snapshotCount())

    assertResult(0)(spark.sql("show tags T_FORCE_AUTO_TAG").count())

    spark.sql("""alter table T_FORCE_AUTO_TAG set tblproperties(
                |'tag.automatic-creation'='process-time',
                |'tag.creation-period'='daily',
                |'tag.creation-delay'='10 m',
                |'tag.num-retained-max'='90'
                |)""".stripMargin)

    spark.sql("CALL paimon.sys.trigger_tag_automatic_creation(table => 'test.T_FORCE_AUTO_TAG')")
    assertResult(1)(spark.sql("show tags T_FORCE_AUTO_TAG").count())
    assertResult(
      spark
        .sql("select date_format(date_sub(current_date(), 1), 'yyyy-MM-dd')")
        .head()
        .getString(0))(loadTable("T_FORCE_AUTO_TAG").tagManager().tagObjects().get(0).getRight)
  }

  test("Paimon procedure: trigger tag automatic creation ignoring delay test") {
    spark.sql("""CREATE TABLE T_FORCE_AUTO_TAG_IGNORE_DELAY (id INT, name STRING)
                |USING PAIMON
                |TBLPROPERTIES (
                |'primary-key'='id'
                |)""".stripMargin)

    spark.sql("insert into T_FORCE_AUTO_TAG_IGNORE_DELAY values(1, 'a')")

    val table = loadTable("T_FORCE_AUTO_TAG_IGNORE_DELAY")
    assertResult(1)(table.snapshotManager().snapshotCount())

    assertResult(0)(spark.sql("show tags T_FORCE_AUTO_TAG_IGNORE_DELAY").count())

    spark.sql(
      """alter table T_FORCE_AUTO_TAG_IGNORE_DELAY set tblproperties(
        |'tag.automatic-creation'='process-time',
        |'tag.creation-period'='daily',
        |'tag.creation-delay'='2 d',  -- make sure it's before delay when triggering auto tag creation
        |'tag.num-retained-max'='90'
        |)""".stripMargin)

    // Triggering before delay should create no auto-tag
    spark.sql(
      "CALL paimon.sys.trigger_tag_automatic_creation(table => 'test.T_FORCE_AUTO_TAG_IGNORE_DELAY')")
    assertResult(0)(spark.sql("show tags T_FORCE_AUTO_TAG_IGNORE_DELAY").count())

    // Triggering with ignoring delay should create an auto-tag
    spark.sql("set `spark.paimon.tag.automatic-creation-ignore-delay`=true")
    spark.sql(
      "CALL paimon.sys.trigger_tag_automatic_creation(table => 'test.T_FORCE_AUTO_TAG_IGNORE_DELAY')")
    assertResult(1)(spark.sql("show tags T_FORCE_AUTO_TAG_IGNORE_DELAY").count())
    assertResult(
      spark
        .sql("select date_format(date_sub(current_date(), 1), 'yyyy-MM-dd')")
        .head()
        .getString(0))(
      loadTable("T_FORCE_AUTO_TAG_IGNORE_DELAY").tagManager().tagObjects().get(0).getRight)
  }

  test("Paimon procedure: trigger tag automatic creation without snapshot test") {
    spark.sql("""CREATE TABLE T_FORCE_AUTO_TAG_NS (id INT, name STRING)
                |USING PAIMON
                |TBLPROPERTIES (
                |'primary-key'='id',
                |'snapshot.ignore-empty-commit'='false',
                |'tag.automatic-creation'='process-time',
                |'tag.creation-period'='daily',
                |'tag.creation-delay'='10 m',
                |'tag.num-retained-max'='90'
                |)""".stripMargin)

    val table = loadTable("T_FORCE_AUTO_TAG_NS")
    assertResult(0)(table.snapshotManager().snapshotCount())
    assertResult(0)(spark.sql("show tags T_FORCE_AUTO_TAG_NS").count())

    spark.sql("CALL paimon.sys.trigger_tag_automatic_creation(table => 'test.T_FORCE_AUTO_TAG_NS')")
    assertResult(1)(table.snapshotManager().snapshotCount())
    assertResult(1)(spark.sql("show tags T_FORCE_AUTO_TAG_NS").count())
    assertResult(
      spark
        .sql("select date_format(date_sub(current_date(), 1), 'yyyy-MM-dd')")
        .head()
        .getString(0))(loadTable("T_FORCE_AUTO_TAG_NS").tagManager().tagObjects().get(0).getRight)
  }

}
