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

import org.apache.paimon.CoreOptions
import org.apache.paimon.Snapshot.CommitKind
import org.apache.paimon.manifest.ManifestCommittable
import org.apache.paimon.spark.PaimonSparkTestBase
import org.apache.paimon.table.sink.CommitCallback

import org.apache.spark.sql.Row

import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConverters._

class CompactOptionsMapTest extends PaimonSparkTestBase {

  private val first = classOf[FirstCompactCommitCallback].getName
  private val second = classOf[SecondCompactCommitCallback].getName
  private val callbacksKey = CoreOptions.COMMIT_CALLBACKS.key()

  Seq(
    (
      "legacy positional options",
      s"'T', '', 'full', '', '', '', '$callbacksKey=$first', '', '0'",
      Set(first)),
    (
      "options_map preserves comma-separated callbacks",
      s"table => 'T', options_map => map('$callbacksKey', '$first,$second')",
      Set(first, second)),
    (
      "options_map overrides options",
      s"table => 'T', options => '$callbacksKey=missing.Callback', " +
        s"options_map => map('$callbacksKey', '$first,$second')",
      Set(first, second))
  ).foreach {
    case (name, arguments, expectedCallbacks) =>
      test(s"compact: $name") {
        withTable("T") {
          spark.sql("""
                      |CREATE TABLE T (id INT, value STRING) USING paimon
                      |TBLPROPERTIES ('primary-key' = 'id', 'bucket' = '1', 'write-only' = 'true')
                      |""".stripMargin)
          spark.sql("INSERT INTO T VALUES (1, 'a')")
          spark.sql("INSERT INTO T VALUES (2, 'b')")
          val schemaManager = loadTable("T").schemaManager()
          val storedOptions = schemaManager.latest().get().options().asScala.toMap
          CompactOptionsMapTest.calls.clear()

          try {
            checkAnswer(spark.sql(s"CALL sys.compact($arguments)"), Seq(Row(true)))

            assert(
              CompactOptionsMapTest.calls.asScala.toMap ==
                expectedCallbacks.map(_ -> CommitKind.COMPACT).toMap)
            assert(schemaManager.latest().get().options().asScala.toMap == storedOptions)
            checkAnswer(spark.sql("SELECT * FROM T"), Seq(Row(1, "a"), Row(2, "b")))
          } finally {
            CompactOptionsMapTest.calls.clear()
          }
        }
      }
  }
}

object CompactOptionsMapTest {
  val calls = new ConcurrentHashMap[String, CommitKind]()
}

class FirstCompactCommitCallback extends CommitCallback {
  override def call(context: CommitCallback.Context): Unit = {
    CompactOptionsMapTest.calls.put(getClass.getName, context.snapshot.commitKind())
  }

  override def retry(committable: ManifestCommittable): Unit = {}

  override def close(): Unit = {}
}

class SecondCompactCommitCallback extends FirstCompactCommitCallback
