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
import org.junit.jupiter.api.Assertions

import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.DurationInt

class VisibilityCallbackTest extends PaimonSparkTestBase {

  Seq((true, false), (false, true)).foreach {
    case (dv, postpone) =>
      test(s"Visibility callback with deletion-vectors $dv and postpone-bucket $postpone") {
        withTable("T") {
          val bucket = if (postpone) -2 else 1
          sql(s"""
                 |CREATE TABLE T (id INT, name STRING)
                 |TBLPROPERTIES (
                 | 'bucket' = '$bucket',
                 | 'primary-key' = 'id',
                 | 'deletion-vectors.enabled' = '$dv',
                 | 'write-only' = 'true'
                 |)
                 |""".stripMargin)

          sql("INSERT INTO T VALUES (1, '1'), (2, '1'), (3, '1')")
          sql("ALTER TABLE T SET TBLPROPERTIES ('visibility-callback.enabled' = 'true')")
          sql("ALTER TABLE T SET TBLPROPERTIES ('visibility-callback.check-interval' = '1s')")
          if (postpone) {
            sql("ALTER TABLE T SET TBLPROPERTIES ('postpone.batch-write-fixed-bucket' = 'false')")
          }

          val table = loadTable("T")
          val firstSnapshot = table.latestSnapshot().get()

          // Async thread to insert and query
          val queryResult = Future {
            // Insert data in async thread
            sql("INSERT INTO T VALUES (1, '2'), (2, '2'), (3, '2')").collect()

            // Try to query immediately - should wait for compaction due to visibility callback
            val result = sql("SELECT * FROM T ORDER BY id").collect()
            result
          }

          val compactFuture = Future {
            while (table.latestSnapshot().get().id == firstSnapshot.id) {
              println("wait INSERT INTO executed...")
              Thread.sleep(1000)
            }

            Thread.sleep(2000)
            sql("CALL sys.compact('T')")
          }

          Await.result(compactFuture, 60.seconds)
          val result = Await.result(queryResult, 60.seconds)
          Assertions.assertEquals(result.toList, Row(1, "2") :: Row(2, "2") :: Row(3, "2") :: Nil)
        }
      }
  }
}
