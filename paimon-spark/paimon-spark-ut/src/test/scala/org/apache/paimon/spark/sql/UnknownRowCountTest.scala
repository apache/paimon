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

import org.apache.paimon.data.{BinaryString, GenericRow}
import org.apache.paimon.io.{DataFileMeta, DataIncrement}
import org.apache.paimon.manifest.FileSource
import org.apache.paimon.spark.PaimonSparkTestBase
import org.apache.paimon.stats.SimpleStats
import org.apache.paimon.table.FileStoreTable
import org.apache.paimon.table.sink.{CommitMessage, CommitMessageImpl}

import org.apache.spark.sql.Row

import java.util.Collections

import scala.collection.JavaConverters._

class UnknownRowCountTest extends PaimonSparkTestBase {

  Seq("parquet", "orc", "avro").foreach {
    format =>
      test(s"query and analyze $format files with unknown row counts") {
        spark.sql(s"""
                     |CREATE TABLE T (id INT, dt STRING)
                     |PARTITIONED BY (dt)
                     |TBLPROPERTIES ('bucket'='-1', 'file.format'='$format')
                     |""".stripMargin)
        spark.sql("INSERT INTO T VALUES (4, 'p1'), (5, 'p2')")
        commitUnknownRows(loadTable("T"))
        spark.catalog.refreshTable("T")

        checkAnswer(spark.sql("SELECT COUNT(*) FROM T"), Row(5L))
        checkAnswer(
          spark.sql("SELECT dt, COUNT(*) FROM T GROUP BY dt"),
          Seq(Row("p1", 4L), Row("p2", 1L)))
        checkAnswer(
          spark.sql("SELECT id FROM T WHERE id > 2 ORDER BY id LIMIT 2"),
          Seq(Row(3), Row(4)))
        assert(spark.sql("SELECT * FROM T LIMIT 3").collect().length == 3)
        spark.sql("ALTER TABLE T ADD COLUMN added INT")
        checkAnswer(spark.sql("SELECT COUNT(*) FROM T WHERE added IS NULL"), Row(5L))
        spark.sql("ANALYZE TABLE T COMPUTE STATISTICS FOR COLUMNS id")
        checkAnswer(spark.sql("SELECT COUNT(*) FROM T"), Row(5L))
      }
  }

  private def commitUnknownRows(table: FileStoreTable): Unit = {
    val builder = table.newBatchWriteBuilder()
    val write = builder.newWrite()
    val commit = builder.newCommit()
    try {
      (1 to 3).foreach(id => write.write(GenericRow.of(Int.box(id), BinaryString.fromString("p1"))))
      val messages: java.util.List[CommitMessage] = write
        .prepareCommit()
        .asScala
        .map {
          message =>
            val m = message.asInstanceOf[CommitMessageImpl]
            val files = m
              .newFilesIncrement()
              .newFiles()
              .asScala
              .map {
                file =>
                  DataFileMeta.forAppend(
                    file.fileName(),
                    file.fileSize(),
                    DataFileMeta.UNKNOWN_ROW_COUNT,
                    SimpleStats.EMPTY_STATS,
                    file.minSequenceNumber(),
                    file.maxSequenceNumber(),
                    file.schemaId(),
                    file.extraFiles(),
                    null,
                    FileSource.APPEND,
                    Collections.emptyList[String](),
                    file.externalPath().orElse(null),
                    null,
                    null
                  )
              }
              .asJava
            new CommitMessageImpl(
              m.partition(),
              m.bucket(),
              m.totalBuckets(),
              new DataIncrement(
                files,
                Collections.emptyList[DataFileMeta](),
                Collections.emptyList[DataFileMeta]()),
              m.compactIncrement()
            ): CommitMessage
        }
        .asJava
      commit.commit(messages)
    } finally {
      write.close()
      commit.close()
    }
  }
}
