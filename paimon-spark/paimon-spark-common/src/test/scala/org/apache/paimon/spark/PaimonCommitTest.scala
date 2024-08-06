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

package org.apache.paimon.spark

import org.apache.paimon.CoreOptions
import org.apache.paimon.manifest.{ManifestCommittable, ManifestEntry}
import org.apache.paimon.table.sink.CommitCallback

import org.junit.jupiter.api.Assertions

import java.lang
import java.util.List

class PaimonCommitTest extends PaimonSparkTestBase {

  test("test commit callback parameter compatibility") {
    withTable("tb") {
      spark.sql("""
                  |CREATE TABLE tb (id int, dt string) using paimon
                  |TBLPROPERTIES ('file.format'='parquet', 'primary-key'='id', 'bucket'='1')
                  |""".stripMargin)

      val table = loadTable("tb")
      val location = table.location().toString

      val _spark = spark
      import _spark.implicits._
      val df = Seq((1, "a"), (2, "b")).toDF("a", "b")
      df.write
        .format("paimon")
        .option(CoreOptions.COMMIT_CALLBACKS.key(), classOf[CustomCommitCallback].getName)
        .option(
          CoreOptions.COMMIT_CALLBACK_PARAM
            .key()
            .replace("#", classOf[CustomCommitCallback].getName),
          "testid-100")
        .mode("append")
        .save(location)

      Assertions.assertEquals(PaimonCommitTest.id, "testid-100")
    }
  }
}

object PaimonCommitTest {
  var id = ""
}

case class CustomCommitCallback(testId: String) extends CommitCallback {

  override def call(
      committedEntries: List[ManifestEntry],
      identifier: Long,
      watermark: lang.Long): Unit = {
    PaimonCommitTest.id = testId
  }

  override def retry(committable: ManifestCommittable): Unit = {}

  override def close(): Unit = {}
}
