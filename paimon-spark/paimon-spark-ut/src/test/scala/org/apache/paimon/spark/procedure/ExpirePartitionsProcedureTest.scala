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

import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.streaming.StreamTest

/** IT Case for [[ExpirePartitionsProcedure]]. */
class ExpirePartitionsProcedureTest extends PaimonSparkTestBase with StreamTest {

  import testImplicits._

  test("Paimon Procedure: expire partitions") {
    failAfter(streamingTimeout) {
      withTempDir {
        checkpointDir =>
          spark.sql(s"""
                       |CREATE TABLE T (k STRING, pt STRING)
                       |TBLPROPERTIES ('primary-key'='k,pt', 'bucket'='1')
                       | PARTITIONED BY (pt)
                       |""".stripMargin)
          val location = loadTable("T").location().toString

          val inputData = MemoryStream[(String, String)]
          val stream = inputData
            .toDS()
            .toDF("k", "pt")
            .writeStream
            .option("checkpointLocation", checkpointDir.getCanonicalPath)
            .foreachBatch {
              (batch: Dataset[Row], _: Long) =>
                batch.write.format("paimon").mode("append").save(location)
            }
            .start()

          val query = () => spark.sql("SELECT * FROM T")

          try {
            // snapshot-1
            inputData.addData(("a", "2024-06-01"))
            stream.processAllAvailable()

            // This partition never expires.
            inputData.addData(("Never-expire", "9999-09-09"))
            stream.processAllAvailable()

            checkAnswer(query(), Row("a", "2024-06-01") :: Row("Never-expire", "9999-09-09") :: Nil)
            // call expire_partitions.
            checkAnswer(
              spark.sql(
                "CALL paimon.sys.expire_partitions(table => 'test.T', expiration_time => '1 d'" +
                  ", timestamp_formatter => 'yyyy-MM-dd')"),
              Row("pt=2024-06-01") :: Nil
            )

            checkAnswer(query(), Row("Never-expire", "9999-09-09") :: Nil)

          } finally {
            stream.stop()
          }
      }
    }
  }

  test("Paimon procedure : expire partitions show a list of expired partitions.") {
    failAfter(streamingTimeout) {
      withTempDir {
        checkpointDir =>
          spark.sql(s"""
                       |CREATE TABLE T (k STRING, pt STRING, hm STRING)
                       |TBLPROPERTIES ('primary-key'='k,pt,hm', 'bucket'='1')
                       | PARTITIONED BY (pt,hm)
                       |""".stripMargin)
          val location = loadTable("T").location().toString

          val inputData = MemoryStream[(String, String, String)]
          val stream = inputData
            .toDS()
            .toDF("k", "pt", "hm")
            .writeStream
            .option("checkpointLocation", checkpointDir.getCanonicalPath)
            .foreachBatch {
              (batch: Dataset[Row], _: Long) =>
                batch.write.format("paimon").mode("append").save(location)
            }
            .start()

          val query = () => spark.sql("SELECT * FROM T")

          try {
            // Show results : There are no expired partitions.
            checkAnswer(
              spark.sql(
                "CALL paimon.sys.expire_partitions(table => 'test.T', expiration_time => '1 d'" +
                  ", timestamp_formatter => 'yyyy-MM-dd')"),
              Row("No expired partitions.") :: Nil
            )

            // snapshot-1
            inputData.addData(("a", "2024-06-01", "01:00"))
            stream.processAllAvailable()
            // snapshot-2
            inputData.addData(("b", "2024-06-02", "02:00"))
            stream.processAllAvailable()
            // snapshot-3, never expires.
            inputData.addData(("Never-expire", "9999-09-09", "99:99"))
            stream.processAllAvailable()

            checkAnswer(
              query(),
              Row("a", "2024-06-01", "01:00") :: Row("b", "2024-06-02", "02:00") :: Row(
                "Never-expire",
                "9999-09-09",
                "99:99") :: Nil)

            // Show a list of expired partitions.
            checkAnswer(
              spark.sql(
                "CALL paimon.sys.expire_partitions(table => 'test.T'" +
                  ", expiration_time => '1 d'" +
                  ", timestamp_formatter => 'yyyy-MM-dd')"),
              Row("pt=2024-06-01, hm=01:00") :: Row("pt=2024-06-02, hm=02:00") :: Nil
            )

            checkAnswer(query(), Row("Never-expire", "9999-09-09", "99:99") :: Nil)

          } finally {
            stream.stop()
          }
      }
    }
  }

  test("Paimon Procedure: expire partitions with values-time strategy.") {
    failAfter(streamingTimeout) {
      withTempDir {
        checkpointDir =>
          spark.sql(s"""
                       |CREATE TABLE T (k STRING, pt STRING)
                       |TBLPROPERTIES ('primary-key'='k,pt', 'bucket'='1')
                       | PARTITIONED BY (pt)
                       |""".stripMargin)
          val location = loadTable("T").location().toString

          val inputData = MemoryStream[(String, String)]
          val stream = inputData
            .toDS()
            .toDF("k", "pt")
            .writeStream
            .option("checkpointLocation", checkpointDir.getCanonicalPath)
            .foreachBatch {
              (batch: Dataset[Row], _: Long) =>
                batch.write.format("paimon").mode("append").save(location)
            }
            .start()

          val query = () => spark.sql("SELECT * FROM T")

          try {
            // snapshot-1
            inputData.addData(("HXH", "2024-06-01"))
            stream.processAllAvailable()

            // Never expire.
            inputData.addData(("Never-expire", "9999-09-09"))
            stream.processAllAvailable()

            checkAnswer(
              query(),
              Row("HXH", "2024-06-01") :: Row("Never-expire", "9999-09-09") :: Nil)
            // expire
            checkAnswer(
              spark.sql(
                "CALL paimon.sys.expire_partitions(table => 'test.T'," +
                  " expiration_time => '1 d'" +
                  ", timestamp_formatter => 'yyyy-MM-dd'" +
                  ",expire_strategy => 'values-time')"),
              Row("pt=2024-06-01") :: Nil
            )

            checkAnswer(query(), Row("Never-expire", "9999-09-09") :: Nil)

          } finally {
            stream.stop()
          }
      }
    }
  }

  test("Paimon Procedure: expire partitions with update-time strategy.") {
    failAfter(streamingTimeout) {
      withTempDir {
        checkpointDir =>
          spark.sql(s"""
                       |CREATE TABLE T (k STRING, pt STRING)
                       |TBLPROPERTIES ('primary-key'='k,pt', 'bucket'='1')
                       | PARTITIONED BY (pt)
                       |""".stripMargin)
          val location = loadTable("T").location().toString

          val inputData = MemoryStream[(String, String)]
          val stream = inputData
            .toDS()
            .toDF("k", "pt")
            .writeStream
            .option("checkpointLocation", checkpointDir.getCanonicalPath)
            .foreachBatch {
              (batch: Dataset[Row], _: Long) =>
                batch.write.format("paimon").mode("append").save(location)
            }
            .start()

          val query = () => spark.sql("SELECT * FROM T")

          try {
            // This partition will expire.
            inputData.addData(("HXH", "9999-09-09"))
            stream.processAllAvailable()
            // Waiting for partition 'pt=9999-09-09' to expire.
            Thread.sleep(2500L)
            // snapshot-2
            inputData.addData(("HXH", "2024-06-01"))
            stream.processAllAvailable()

            // Partitions that are updated within 2 second would be retained.
            checkAnswer(
              spark.sql(
                "CALL paimon.sys.expire_partitions(" +
                  "table => 'test.T'," +
                  " expiration_time => '2 s'" +
                  ",expire_strategy => 'update-time')"),
              Row("pt=9999-09-09") :: Nil
            )

            checkAnswer(query(), Row("HXH", "2024-06-01") :: Nil)

            // Waiting for all partitions to expire.
            Thread.sleep(1500)
            // All partition will expire.
            checkAnswer(
              spark.sql(
                "CALL paimon.sys.expire_partitions(" +
                  "table => 'test.T'," +
                  " expiration_time => '1 s'" +
                  ",expire_strategy => 'update-time')"),
              Row("pt=2024-06-01") :: Nil
            )

            checkAnswer(query(), Nil)

          } finally {
            stream.stop()
          }
      }
    }
  }

  test("Paimon Procedure: expire partitions with update-time strategy in same partition.") {
    failAfter(streamingTimeout) {
      withTempDir {
        checkpointDir =>
          spark.sql(s"""
                       |CREATE TABLE T (k STRING, pt STRING, hm STRING)
                       |TBLPROPERTIES ('primary-key'='k,pt,hm', 'bucket'='1')
                       | PARTITIONED BY (pt,hm)
                       |""".stripMargin)
          val location = loadTable("T").location().toString

          val inputData = MemoryStream[(String, String, String)]
          val stream = inputData
            .toDS()
            .toDF("k", "pt", "hm")
            .writeStream
            .option("checkpointLocation", checkpointDir.getCanonicalPath)
            .foreachBatch {
              (batch: Dataset[Row], _: Long) =>
                batch.write.format("paimon").mode("append").save(location)
            }
            .start()

          val query = () => spark.sql("SELECT * FROM T")

          try {
            // This partition will not expire.
            inputData.addData(("HXH", "2024-06-01", "01:00"))
            stream.processAllAvailable()
            // Waiting for 'pt=9999-09-09, hm=99:99' partitions to expire.
            Thread.sleep(2500L)
            // Updating the same partition data will update partition last update time, then this partition will not expire.
            inputData.addData(("HXH", "2024-06-01", "01:00"))
            stream.processAllAvailable()

            // The last update time of the 'pt=9999-09-09, hm=99:99' partition is updated so the partition would not expire.
            checkAnswer(
              spark.sql(
                "CALL paimon.sys.expire_partitions(table => 'test.T'," +
                  " expiration_time => '2 s'" +
                  ",expire_strategy => 'update-time')"),
              Row("No expired partitions.") :: Nil
            )

            checkAnswer(query(), Row("HXH", "2024-06-01", "01:00") :: Nil)
            // Waiting for all partitions to expire.
            Thread.sleep(1500)

            // The partition 'dt=2024-06-01, hm=01:00' will expire.
            checkAnswer(
              spark.sql(
                "CALL paimon.sys.expire_partitions(table => 'test.T'," +
                  " expiration_time => '1 s'" +
                  ",expire_strategy => 'update-time')"),
              Row("pt=2024-06-01, hm=01:00") :: Nil
            )

            checkAnswer(query(), Nil)

          } finally {
            stream.stop()
          }
      }
    }
  }

  test("Paimon Procedure: expire partitions with non-date format partition.") {
    failAfter(streamingTimeout) {
      withTempDir {
        checkpointDir =>
          spark.sql(s"""
                       |CREATE TABLE T (k STRING, pt STRING)
                       |TBLPROPERTIES ('primary-key'='k,pt', 'bucket'='1')
                       | PARTITIONED BY (pt)
                       |""".stripMargin)
          val location = loadTable("T").location().toString

          val inputData = MemoryStream[(String, String)]
          val stream = inputData
            .toDS()
            .toDF("k", "pt")
            .writeStream
            .option("checkpointLocation", checkpointDir.getCanonicalPath)
            .foreachBatch {
              (batch: Dataset[Row], _: Long) =>
                batch.write.format("paimon").mode("append").save(location)
            }
            .start()

          val query = () => spark.sql("SELECT * FROM T")

          try {
            // This partition will expire.
            inputData.addData(("HXH", "pt-1"))
            stream.processAllAvailable()
            Thread.sleep(2500L)
            // snapshot-2
            inputData.addData(("HXH", "pt-2"))
            stream.processAllAvailable()

            // Only update-time strategy support non date format partition to expire.
            checkAnswer(
              spark.sql(
                "CALL paimon.sys.expire_partitions(table => 'test.T'," +
                  " expiration_time => '2 s'" +
                  ",expire_strategy => 'update-time')"),
              Row("pt=pt-1") :: Nil
            )

            checkAnswer(query(), Row("HXH", "pt-2") :: Nil)

            // Waiting for all partitions to expire.
            Thread.sleep(1500)
            // call expire_partitions.
            checkAnswer(
              spark.sql(
                "CALL paimon.sys.expire_partitions(table => 'test.T'," +
                  " expiration_time => '1 s'" +
                  ",expire_strategy => 'update-time')"),
              Row("pt=pt-2") :: Nil
            )

            checkAnswer(query(), Nil)

          } finally {
            stream.stop()
          }
      }
    }
  }

  test("Paimon procedure : expire partitions with specified time-pattern partitions.") {
    failAfter(streamingTimeout) {
      withTempDir {
        checkpointDir =>
          spark.sql(s"""
                       |CREATE TABLE T (k STRING, pt STRING, hm STRING)
                       |TBLPROPERTIES ('primary-key'='k,pt,hm', 'bucket'='1')
                       | PARTITIONED BY (hm, pt)
                       |""".stripMargin)
          val location = loadTable("T").location().toString

          val inputData = MemoryStream[(String, String, String)]
          val stream = inputData
            .toDS()
            .toDF("k", "pt", "hm")
            .writeStream
            .option("checkpointLocation", checkpointDir.getCanonicalPath)
            .foreachBatch {
              (batch: Dataset[Row], _: Long) =>
                batch.write.format("paimon").mode("append").save(location)
            }
            .start()

          val query = () => spark.sql("SELECT * FROM T")

          try {
            // Show results : There are no expired partitions.
            checkAnswer(
              spark.sql(
                "CALL paimon.sys.expire_partitions(table => 'test.T', expiration_time => '1 d'" +
                  ", timestamp_formatter => 'yyyy-MM-dd', timestamp_pattern => '$pt')"),
              Row("No expired partitions.") :: Nil
            )

            // snapshot-1
            inputData.addData(("a", "2024-06-01", "01:00"))
            stream.processAllAvailable()
            // snapshot-2
            inputData.addData(("b", "2024-06-02", "02:00"))
            stream.processAllAvailable()
            // snapshot-3, never expires.
            inputData.addData(("Never-expire", "9999-09-09", "99:99"))
            stream.processAllAvailable()

            checkAnswer(
              query(),
              Row("a", "2024-06-01", "01:00") :: Row("b", "2024-06-02", "02:00") :: Row(
                "Never-expire",
                "9999-09-09",
                "99:99") :: Nil)

            // Show a list of expired partitions.
            checkAnswer(
              spark.sql(
                "CALL paimon.sys.expire_partitions(table => 'test.T'" +
                  ", expiration_time => '1 d'" +
                  ", timestamp_formatter => 'yyyy-MM-dd HH:mm'" +
                  ", timestamp_pattern => '$pt $hm')"),
              Row("hm=01:00, pt=2024-06-01") :: Row("hm=02:00, pt=2024-06-02") :: Nil
            )

            checkAnswer(query(), Row("Never-expire", "9999-09-09", "99:99") :: Nil)

          } finally {
            stream.stop()
          }
      }
    }
  }

  test("Paimon procedure : sorted the expired partitions with max_expires.") {
    failAfter(streamingTimeout) {
      withTempDir {
        checkpointDir =>
          spark.sql(s"""
                       |CREATE TABLE T (k STRING, pt STRING, hm STRING)
                       |TBLPROPERTIES ('primary-key'='k,pt,hm', 'bucket'='1')
                       | PARTITIONED BY (pt,hm)
                       |""".stripMargin)
          val location = loadTable("T").location().toString

          val inputData = MemoryStream[(String, String, String)]
          val stream = inputData
            .toDS()
            .toDF("k", "pt", "hm")
            .writeStream
            .option("checkpointLocation", checkpointDir.getCanonicalPath)
            .foreachBatch {
              (batch: Dataset[Row], _: Long) =>
                batch.write.format("paimon").mode("append").save(location)
            }
            .start()

          val query = () => spark.sql("SELECT * FROM T")

          try {
            // Show results : There are no expired partitions.
            checkAnswer(
              spark.sql(
                "CALL paimon.sys.expire_partitions(table => 'test.T', expiration_time => '1 d'" +
                  ", timestamp_formatter => 'yyyy-MM-dd')"),
              Row("No expired partitions.") :: Nil
            )

            inputData.addData(("a", "2024-06-02", "02:00"))
            stream.processAllAvailable()
            inputData.addData(("b", "2024-06-02", "01:00"))
            stream.processAllAvailable()
            inputData.addData(("d", "2024-06-03", "01:00"))
            stream.processAllAvailable()
            inputData.addData(("c", "2024-06-01", "01:00"))
            stream.processAllAvailable()
            // this snapshot never expires.
            inputData.addData(("Never-expire", "9999-09-09", "99:99"))
            stream.processAllAvailable()

            checkAnswer(
              query(),
              Row("a", "2024-06-02", "02:00") :: Row("b", "2024-06-02", "01:00") :: Row(
                "d",
                "2024-06-03",
                "01:00") :: Row("c", "2024-06-01", "01:00") :: Row(
                "Never-expire",
                "9999-09-09",
                "99:99") :: Nil
            )

            // sorted result of limited expired partitions.
            checkAnswer(
              spark.sql(
                "CALL paimon.sys.expire_partitions(table => 'test.T'" +
                  ", expiration_time => '1 d'" +
                  ", timestamp_formatter => 'yyyy-MM-dd', max_expires => 3)"),
              Row("pt=2024-06-01, hm=01:00") :: Row("pt=2024-06-02, hm=01:00") :: Row(
                "pt=2024-06-02, hm=02:00") :: Nil
            )

            checkAnswer(
              query(),
              Row("d", "2024-06-03", "01:00") :: Row("Never-expire", "9999-09-09", "99:99") :: Nil)
          } finally {
            stream.stop()
          }
      }
    }
  }

  test("Paimon Procedure: expire partitions with default num") {
    failAfter(streamingTimeout) {
      withTempDir {
        checkpointDir =>
          spark.sql(
            s"""
               |CREATE TABLE T (k STRING, pt STRING)
               |TBLPROPERTIES ('primary-key'='k,pt', 'bucket'='1', 'partition.expiration-max-num'='2')
               |PARTITIONED BY (pt)
               |""".stripMargin)
          val location = loadTable("T").location().toString

          val inputData = MemoryStream[(String, String)]
          val stream = inputData
            .toDS()
            .toDF("k", "pt")
            .writeStream
            .option("checkpointLocation", checkpointDir.getCanonicalPath)
            .foreachBatch {
              (batch: Dataset[Row], _: Long) =>
                batch.write.format("paimon").mode("append").save(location)
            }
            .start()

          val query = () => spark.sql("SELECT * FROM T")

          try {
            // snapshot-1
            inputData.addData(("a", "2024-06-01"))
            stream.processAllAvailable()

            // snapshot-2
            inputData.addData(("b", "2024-06-02"))
            stream.processAllAvailable()

            // snapshot-3
            inputData.addData(("c", "2024-06-03"))
            stream.processAllAvailable()

            // This partition never expires.
            inputData.addData(("Never-expire", "9999-09-09"))
            stream.processAllAvailable()

            checkAnswer(
              query(),
              Row("a", "2024-06-01") :: Row("b", "2024-06-02") :: Row("c", "2024-06-03") :: Row(
                "Never-expire",
                "9999-09-09") :: Nil)
            // call expire_partitions.
            checkAnswer(
              spark.sql(
                "CALL paimon.sys.expire_partitions(table => 'test.T', expiration_time => '1 d'" +
                  ", timestamp_formatter => 'yyyy-MM-dd')"),
              Row("pt=2024-06-01") :: Row("pt=2024-06-02") :: Nil
            )

            checkAnswer(query(), Row("c", "2024-06-03") :: Row("Never-expire", "9999-09-09") :: Nil)

          } finally {
            stream.stop()
          }
      }
    }
  }
}
