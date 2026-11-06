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
import org.apache.paimon.utils.ExceptionUtils

import org.apache.spark.sql.Row

class DeleteOrderingGuardValidationTest extends PaimonSparkTestBase {

  test("delete ordering guards reject the partial-update merge engine") {
    withTable("T") {
      assertGuardsRejected {
        createPartialUpdateTable(Some(true))
      }
    }
  }

  Seq(None, Some(false)).foreach {
    enabled =>
      test(s"partial-update remains supported with guard capture $enabled") {
        withTable("T") {
          createPartialUpdateTable(enabled)
          spark.sql("INSERT INTO T (id, data, event_ts, op_type) VALUES (1, 'one', 1, '+I')")
          spark.sql("INSERT INTO T (id, data2, event_ts2, op_type) VALUES (1, 'two', 1, '+I')")
          spark.sql("INSERT INTO T (id, data2, event_ts2, op_type) VALUES (1, 'two+', 10, '+I')")
          checkAnswer(spark.sql("SELECT * FROM T"), Row(1, "one", 1, "two+", 10, "+I"))

          assertGuardsRejected {
            spark.sql("ALTER TABLE T SET TBLPROPERTIES ('delete-ordering-guards.enabled' = 'true')")
          }
          checkAnswer(spark.sql("SELECT * FROM T"), Row(1, "one", 1, "two+", 10, "+I"))
        }
      }
  }

  private def assertGuardsRejected(action: => Unit): Unit = {
    val error = intercept[Exception](action)
    val validation = ExceptionUtils.findThrowable(error, classOf[IllegalArgumentException])
    assert(validation.isPresent, error.toString)
    assert(validation.get().getMessage.contains("only the deduplicate merge engine"))
  }

  private def createPartialUpdateTable(enabled: Option[Boolean]): Unit = {
    val guardOption = enabled
      .map(value => s"'delete-ordering-guards.enabled' = '$value',")
      .getOrElse("")
    spark.sql(s"""
                 |CREATE TABLE T (
                 |  id INT, data STRING, event_ts INT, data2 STRING, event_ts2 INT, op_type STRING)
                 |TBLPROPERTIES (
                 |  'primary-key' = 'id',
                 |  'rowkind.field' = 'op_type',
                 |  'merge-engine' = 'partial-update',
                 |  'fields.event_ts.sequence-group' = 'data',
                 |  'fields.event_ts2.sequence-group' = 'data2',
                 |  'changelog-producer' = 'lookup',
                 |  'changelog-producer.row-deduplicate' = 'true',
                 |  'changelog-producer.ignore-update-before' = 'true',
                 |  $guardOption
                 |  'bucket' = '1',
                 |  'partial-update.remove-record-on-sequence-group' = 'event_ts')
                 |""".stripMargin)
  }
}
