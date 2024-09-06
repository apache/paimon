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

package org.apache.paimon.spark.procedure;

import org.apache.paimon.data.Timestamp
import org.apache.paimon.spark.PaimonSparkTestBase
import org.apache.paimon.utils.SnapshotManager

import org.apache.spark.sql.Row
import org.assertj.core.api.Assertions.assertThat

class ExpireTagsProcedureTest extends PaimonSparkTestBase {

  test("Paimon procedure: expire tags that reached its timeRetained") {
    spark.sql(s"""
                 |CREATE TABLE T (id STRING, name STRING)
                 |USING PAIMON
                 |""".stripMargin)

    val table = loadTable("T")
    val snapshotManager = table.snapshotManager()

    // generate 5 snapshots
    for (i <- 1 to 5) {
      spark.sql(s"INSERT INTO T VALUES($i, '$i')")
    }
    checkSnapshots(snapshotManager, 1, 5)

    spark.sql("CALL paimon.sys.create_tag(table => 'test.T', tag => 'tag-1', snapshot => 1)")
    spark.sql(
      "CALL paimon.sys.create_tag(table => 'test.T', tag => 'tag-2', snapshot => 2, time_retained => '1h')")

    // no tags expired
    checkAnswer(
      spark.sql("CALL paimon.sys.expire_tags(table => 'test.T')"),
      Row("No expired tags.") :: Nil)

    spark.sql(
      "CALL paimon.sys.create_tag(table => 'test.T', tag => 'tag-3', snapshot => 3, time_retained => '1s')")
    spark.sql(
      "CALL paimon.sys.create_tag(table => 'test.T', tag => 'tag-4', snapshot => 4, time_retained => '1s')")
    checkAnswer(spark.sql("select count(tag_name) from `T$tags`"), Row(4) :: Nil)

    Thread.sleep(2000)
    // tag-3,tag-4 expired
    checkAnswer(
      spark.sql("CALL paimon.sys.expire_tags(table => 'test.T')"),
      Row("tag-3") :: Row("tag-4") :: Nil)

    checkAnswer(spark.sql("select tag_name from `T$tags`"), Row("tag-1") :: Row("tag-2") :: Nil)
  }

  test("Paimon procedure: expire tags that createTime less than specified expirationTime") {
    spark.sql(s"""
                 |CREATE TABLE T (id STRING, name STRING)
                 |USING PAIMON
                 |""".stripMargin)

    val table = loadTable("T")
    val snapshotManager = table.snapshotManager()

    // generate 5 snapshots
    for (i <- 1 to 5) {
      spark.sql(s"INSERT INTO T VALUES($i, '$i')")
    }
    checkSnapshots(snapshotManager, 1, 5)

    spark.sql(
      "CALL paimon.sys.create_tag(table => 'test.T', tag => 'tag-1', snapshot => 1, time_retained => '1d')")
    spark.sql(
      "CALL paimon.sys.create_tag(table => 'test.T', tag => 'tag-2', snapshot => 2, time_retained => '1d')")
    spark.sql(
      "CALL paimon.sys.create_tag(table => 'test.T', tag => 'tag-3', snapshot => 3, time_retained => '1d')")
    spark.sql(
      "CALL paimon.sys.create_tag(table => 'test.T', tag => 'tag-4', snapshot => 4, time_retained => '1d')")
    checkAnswer(spark.sql("select count(tag_name) from `T$tags`"), Row(4) :: Nil)

    // tag-4 as the base expiration_time
    val expirationTime = table.tagManager().tag("tag-4").getTagCreateTime
    val timestamp =
      new java.sql.Timestamp(Timestamp.fromLocalDateTime(expirationTime).getMillisecond)
    checkAnswer(
      spark.sql(
        s"CALL paimon.sys.expire_tags(table => 'test.T', expiration_time => '${timestamp.toString}')"),
      Row("tag-1") :: Row("tag-2") :: Row("tag-3") :: Nil
    )
  }

  private def checkSnapshots(sm: SnapshotManager, earliest: Int, latest: Int): Unit = {
    assertThat(sm.snapshotCount).isEqualTo(latest - earliest + 1)
    assertThat(sm.earliestSnapshotId).isEqualTo(earliest)
    assertThat(sm.latestSnapshotId).isEqualTo(latest)
  }
}
