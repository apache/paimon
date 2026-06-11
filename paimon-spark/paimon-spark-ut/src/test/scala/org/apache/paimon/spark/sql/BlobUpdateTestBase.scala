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

import org.apache.spark.SparkConf
import org.apache.spark.sql.Row

class BlobUpdateTestBase extends PaimonSparkTestBase {

  override def sparkConf: SparkConf = {
    super.sparkConf.set("spark.paimon.write.use-v2-write", "false")
  }

  test("Blob: merge-into updates raw-data BLOB column") {
    withTable("s", "t") {
      sql("CREATE TABLE t (id INT, name STRING, picture BINARY) TBLPROPERTIES " +
        "('row-tracking.enabled'='true', 'data-evolution.enabled'='true', 'blob-field'='picture')")
      sql(
        "INSERT INTO t VALUES " +
          "(1, 'name1', X'48656C6C6F'), " +
          "(2, 'name2', X'5945'), " +
          "(3, 'name3', X'414243')")

      sql("CREATE TABLE s (id INT, picture BINARY)")
      sql("INSERT INTO s VALUES (1, X'4E4557')")

      sql("""
            |MERGE INTO t
            |USING s
            |ON t.id = s.id
            |WHEN MATCHED THEN UPDATE SET t.picture = s.picture
            |""".stripMargin)

      checkAnswer(
        sql("SELECT id, picture FROM t ORDER BY id"),
        Seq(
          Row(1, Array[Byte](78, 69, 87)),
          Row(2, Array[Byte](89, 69)),
          Row(3, Array[Byte](65, 66, 67)))
      )
    }
  }

  test("Blob: merge-into updates raw-data BLOB column to null") {
    withTable("s", "s2", "t") {
      sql("CREATE TABLE t (id INT, name STRING, picture BINARY) TBLPROPERTIES " +
        "('row-tracking.enabled'='true', 'data-evolution.enabled'='true', 'blob-field'='picture')")
      sql(
        "INSERT INTO t VALUES " +
          "(1, 'name1', X'48656C6C6F'), " +
          "(2, 'name2', X'5945')")

      sql("CREATE TABLE s (id INT, picture BINARY)")
      sql("INSERT INTO s VALUES (1, CAST(NULL AS BINARY))")

      sql("""
            |MERGE INTO t
            |USING s
            |ON t.id = s.id
            |WHEN MATCHED THEN UPDATE SET t.picture = s.picture
            |""".stripMargin)

      checkAnswer(
        sql("SELECT id, picture FROM t ORDER BY id"),
        Seq(Row(1, null), Row(2, Array[Byte](89, 69)))
      )

      sql("CREATE TABLE s2 (id INT, picture BINARY)")
      sql("INSERT INTO s2 VALUES (2, X'4E4557')")
      sql("""
            |MERGE INTO t
            |USING s2
            |ON t.id = s2.id
            |WHEN MATCHED THEN UPDATE SET t.picture = s2.picture
            |""".stripMargin)

      checkAnswer(
        sql("SELECT id, picture FROM t ORDER BY id"),
        Seq(Row(1, null), Row(2, Array[Byte](78, 69, 87)))
      )
    }
  }

  test("Blob: merge-into raw-data BLOB marker name does not collide with target column") {
    withTable("s", "t") {
      sql(
        "CREATE TABLE t (id INT, `__paimon_raw_blob_placeholder_0` STRING, picture BINARY) " +
          "TBLPROPERTIES ('row-tracking.enabled'='true', 'data-evolution.enabled'='true', " +
          "'blob-field'='picture')")
      sql("INSERT INTO t VALUES (1, 'old_marker_name', X'01'), (2, 'kept', X'02')")

      sql("CREATE TABLE s (id INT, `__paimon_raw_blob_placeholder_0` STRING, picture BINARY)")
      sql("INSERT INTO s VALUES (1, 'new_marker_name', X'4E4557')")

      sql("""
            |MERGE INTO t
            |USING s
            |ON t.id = s.id
            |WHEN MATCHED THEN UPDATE SET
            |  t.`__paimon_raw_blob_placeholder_0` = s.`__paimon_raw_blob_placeholder_0`,
            |  t.picture = s.picture
            |""".stripMargin)

      checkAnswer(
        sql("SELECT id, `__paimon_raw_blob_placeholder_0`, picture FROM t ORDER BY id"),
        Seq(Row(1, "new_marker_name", Array[Byte](78, 69, 87)), Row(2, "kept", Array[Byte](2)))
      )
    }
  }

  test("Blob: self merge updates raw-data BLOB column") {
    withTable("t") {
      sql("CREATE TABLE t (id INT, name STRING, picture BINARY) TBLPROPERTIES " +
        "('row-tracking.enabled'='true', 'data-evolution.enabled'='true', 'blob-field'='picture')")
      sql(
        "INSERT INTO t VALUES " +
          "(1, 'name1', X'48656C6C6F'), " +
          "(2, 'name2', X'5945'), " +
          "(3, 'name3', X'414243')")

      sql("""
            |MERGE INTO t
            |USING t AS source
            |ON t._ROW_ID = source._ROW_ID
            |WHEN MATCHED AND source.id = 1 THEN
            |  UPDATE SET t.picture = unhex(concat(hex(source.picture), '01'))
            |""".stripMargin)

      checkAnswer(
        sql("SELECT id, picture FROM t ORDER BY id"),
        Seq(
          Row(1, Array[Byte](72, 101, 108, 108, 111, 1)),
          Row(2, Array[Byte](89, 69)),
          Row(3, Array[Byte](65, 66, 67)))
      )
    }
  }

  test("Blob: merge-into updates multiple raw-data BLOB columns with split blob files") {
    withTable("s", "t") {
      def bytesHex(value: Int, length: Int): String = {
        Seq.fill(length)(f"$value%02X").mkString
      }

      def bytes(value: Int, length: Int): Array[Byte] = {
        Array.fill[Byte](length)(value.toByte)
      }

      def blobFileRanges(sequenceFilter: String): Map[String, Seq[(Long, Long)]] = {
        val blobFiles = sql(
          "SELECT first_row_id, record_count, write_cols FROM `t$files` " +
            s"WHERE file_path LIKE '%.blob' AND $sequenceFilter")
          .collect()
        blobFiles
          .groupBy(row => row.getSeq[String](2).head)
          .map {
            case (field, rows) =>
              field -> rows
                .map(row => row.getLong(0) -> row.getLong(1))
                .sortBy(_._1)
                .toSeq
          }
      }

      sql(
        "CREATE TABLE t (id INT, pic1 BINARY, pic2 BINARY) TBLPROPERTIES (" +
          "'row-tracking.enabled'='true', 'data-evolution.enabled'='true', " +
          "'blob-field'='pic1,pic2', 'blob.target-file-size'='30 b')")
      sql(
        "INSERT INTO t " +
          "SELECT /*+ REPARTITION(1) */ id, pic1, pic2 FROM VALUES " +
          s"(1, X'${bytesHex(1, 20)}', X'${bytesHex(31, 20)}'), " +
          s"(2, X'${bytesHex(2, 20)}', X'${bytesHex(32, 20)}'), " +
          s"(3, X'${bytesHex(3, 20)}', X'${bytesHex(33, 20)}'), " +
          s"(4, X'${bytesHex(4, 20)}', X'${bytesHex(34, 20)}') " +
          "AS v(id, pic1, pic2)")

      val oldRanges = blobFileRanges("max_sequence_number = 1")
      val oneBlobPerFile = Seq(0L -> 1L, 1L -> 1L, 2L -> 1L, 3L -> 1L)
      assert(oldRanges == Map("pic1" -> oneBlobPerFile, "pic2" -> oneBlobPerFile))
      val dataFileRanges = sql(
        "SELECT first_row_id, record_count FROM `t$files` " +
          "WHERE file_path NOT LIKE '%.blob' AND max_sequence_number = 1")
        .collect()
        .map(row => row.getLong(0) -> row.getLong(1))
        .sortBy(_._1)
        .toSeq
      assert(dataFileRanges == Seq(0L -> 4L))

      sql("CREATE TABLE s (id INT, pic1 BINARY, pic2 BINARY)")
      sql(
        "INSERT INTO s VALUES " +
          s"(1, X'${bytesHex(11, 1)}', X'${bytesHex(41, 20)}'), " +
          s"(2, X'${bytesHex(12, 1)}', X'${bytesHex(42, 20)}'), " +
          s"(3, X'${bytesHex(13, 1)}', X'${bytesHex(43, 20)}'), " +
          s"(4, X'${bytesHex(14, 1)}', X'${bytesHex(44, 20)}')")

      sql("""
            |MERGE INTO t
            |USING s
            |ON t.id = s.id
            |WHEN MATCHED THEN UPDATE SET t.pic1 = s.pic1, t.pic2 = s.pic2
            |""".stripMargin)

      checkAnswer(
        sql("SELECT id, pic1, pic2 FROM t ORDER BY id"),
        Seq(
          Row(1, bytes(11, 1), bytes(41, 20)),
          Row(2, bytes(12, 1), bytes(42, 20)),
          Row(3, bytes(13, 1), bytes(43, 20)),
          Row(4, bytes(14, 1), bytes(44, 20)))
      )

      val updatedRanges = blobFileRanges("max_sequence_number > 1")
      assert(updatedRanges("pic1") == Seq(0L -> 2L, 2L -> 2L))
      assert(updatedRanges("pic2") == oneBlobPerFile)
      assert(updatedRanges("pic1") != oldRanges("pic1"))
      assert(updatedRanges("pic1") != updatedRanges("pic2"))
    }
  }

  test("Blob: merge-into updates non-blob column on raw blob table with split blob files") {
    withTable("s", "t") {
      sql(
        "CREATE TABLE t (id INT, name STRING, picture BINARY) TBLPROPERTIES " +
          "('row-tracking.enabled'='true', 'data-evolution.enabled'='true', " +
          "'blob-field'='picture', 'blob.target-file-size'='1 b')")
      sql(
        "INSERT INTO t VALUES " +
          "(1, 'name1', X'48656C6C6F'), " +
          "(2, 'name2', X'5945'), " +
          "(3, 'name3', X'414243')")

      sql("CREATE TABLE s (id INT, name STRING)")
      sql("INSERT INTO s VALUES (1, 'updated_name1')")

      sql("""
            |MERGE INTO t
            |USING s
            |ON t.id = s.id
            |WHEN MATCHED THEN UPDATE SET t.name = s.name
            |""".stripMargin)

      checkAnswer(
        sql("SELECT id, name, picture FROM t ORDER BY id"),
        Seq(
          Row(1, "updated_name1", Array[Byte](72, 101, 108, 108, 111)),
          Row(2, "name2", Array[Byte](89, 69)),
          Row(3, "name3", Array[Byte](65, 66, 67)))
      )
    }
  }

  test("Blob: merge-into matched actions update different non-blob columns") {
    withTable("s", "t") {
      sql(
        "CREATE TABLE t (id INT, name STRING, label STRING, picture BINARY) TBLPROPERTIES " +
          "('row-tracking.enabled'='true', 'data-evolution.enabled'='true', " +
          "'blob-field'='picture')")
      sql(
        "INSERT INTO t VALUES " +
          "(1, 'name1', 'label1', X'01'), " +
          "(2, 'name2', 'label2', X'02'), " +
          "(3, 'name3', 'label3', X'03')")

      sql("CREATE TABLE s (id INT, action INT, new_name STRING, new_label STRING)")
      sql(
        "INSERT INTO s VALUES " +
          "(1, 1, 'updated_name1', 'unused_label1'), " +
          "(2, 2, 'unused_name2', 'updated_label2')")

      sql("""
            |MERGE INTO t
            |USING s
            |ON t.id = s.id
            |WHEN MATCHED AND s.action = 1 THEN UPDATE SET t.name = s.new_name
            |WHEN MATCHED AND s.action = 2 THEN UPDATE SET t.label = s.new_label
            |""".stripMargin)

      checkAnswer(
        sql("SELECT id, name, label, picture FROM t ORDER BY id"),
        Seq(
          Row(1, "updated_name1", "label1", Array[Byte](1)),
          Row(2, "name2", "updated_label2", Array[Byte](2)),
          Row(3, "name3", "label3", Array[Byte](3)))
      )
    }
  }

}

class BlobUpdateTestWithV2Write extends BlobUpdateTestBase {
  override def sparkConf: SparkConf = {
    super.sparkConf.set("spark.paimon.write.use-v2-write", "true")
  }
}
