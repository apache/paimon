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

import org.apache.paimon.CoreOptions
import org.apache.paimon.catalog.CatalogContext
import org.apache.paimon.data.{Blob, BlobDescriptor}
import org.apache.paimon.fs.{IsolatedDirectoryFileIO, Path}
import org.apache.paimon.fs.local.LocalFileIO
import org.apache.paimon.options.Options
import org.apache.paimon.spark.{PaimonSparkTestBase, SparkCatalog}
import org.apache.paimon.utils.UriReaderFactory

import org.apache.spark.SparkConf
import org.apache.spark.sql.Row

import java.util
import java.util.Random

class BlobTestBase extends PaimonSparkTestBase {

  private val RANDOM = new Random

  override def sparkConf: SparkConf = {
    super.sparkConf.set("spark.paimon.write.use-v2-write", "false")
  }

  test("Blob: test basic") {
    withTable("t") {
      sql(
        "CREATE TABLE t (id INT, data STRING, picture BINARY) TBLPROPERTIES ('row-tracking.enabled'='true', 'data-evolution.enabled'='true', 'blob-field'='picture')")
      sql("INSERT INTO t VALUES (1, 'paimon', X'48656C6C6F')")

      checkAnswer(
        sql("SELECT *, _ROW_ID, _SEQUENCE_NUMBER FROM t"),
        Seq(Row(1, "paimon", Array[Byte](72, 101, 108, 108, 111), 0, 1))
      )

      checkAnswer(
        sql("SELECT COUNT(*) FROM `t$files`"),
        Seq(Row(2))
      )
    }
  }

  test("Blob: test multiple blobs") {
    withTable("t") {
      sql("CREATE TABLE t (id INT, data STRING, pic1 BINARY, pic2 BINARY) TBLPROPERTIES (" +
        "'row-tracking.enabled'='true', 'data-evolution.enabled'='true', 'blob-field'='pic1,pic2')")
      sql("INSERT INTO t VALUES (1, 'paimon', X'48656C6C6F', X'5945')")

      checkAnswer(
        sql("SELECT *, _ROW_ID, _SEQUENCE_NUMBER FROM t"),
        Seq(Row(1, "paimon", Array[Byte](72, 101, 108, 108, 111), Array[Byte](89, 69), 0, 1))
      )

      checkAnswer(
        sql("SELECT COUNT(*) FROM `t$files`"),
        Seq(Row(3))
      )
    }
  }

  test("Blob: test write blob descriptor") {
    withTable("t") {
      val blobData = new Array[Byte](1024 * 1024)
      RANDOM.nextBytes(blobData)
      val fileIO = new LocalFileIO
      val uri = "file://" + tempDBDir.toString + "/external_blob"
      try {
        val outputStream = fileIO.newOutputStream(new Path(uri), true)
        try outputStream.write(blobData)
        finally if (outputStream != null) outputStream.close()
      }

      val blobDescriptor = new BlobDescriptor(uri, 0, blobData.length)

      sql(
        "CREATE TABLE t (id INT, data STRING, picture BINARY) TBLPROPERTIES ('row-tracking.enabled'='true', 'data-evolution.enabled'='true', 'blob-field'='picture')")
      sql(
        "INSERT INTO t VALUES (1, 'paimon', X'" + bytesToHex(blobDescriptor.serialize()) + "'),"
          + "(5, 'paimon', X'" + bytesToHex(blobDescriptor.serialize()) + "'),"
          + "(2, 'paimon', X'" + bytesToHex(blobDescriptor.serialize()) + "'),"
          + "(3, 'paimon', X'" + bytesToHex(blobDescriptor.serialize()) + "'),"
          + "(4, 'paimon', X'" + bytesToHex(blobDescriptor.serialize()) + "')")

      sql("ALTER TABLE t SET TBLPROPERTIES ('blob-as-descriptor'='true')")
      val newDescriptorBytes =
        sql("SELECT picture FROM t WHERE id = 1").collect()(0).get(0).asInstanceOf[Array[Byte]]
      val newBlobDescriptor = BlobDescriptor.deserialize(newDescriptorBytes)
      val options = new Options()
      options.set("warehouse", tempDBDir.toString)
      val catalogContext = CatalogContext.create(options)
      val uriReaderFactory = new UriReaderFactory(catalogContext)
      val blob = Blob.fromDescriptor(uriReaderFactory.create(newBlobDescriptor.uri), blobDescriptor)
      assert(util.Arrays.equals(blobData, blob.toData))

      sql("ALTER TABLE t SET TBLPROPERTIES ('blob-as-descriptor'='false')")
      checkAnswer(
        sql("SELECT *, _ROW_ID, _SEQUENCE_NUMBER FROM t WHERE id = 1"),
        Seq(Row(1, "paimon", blobData, 0, 1))
      )
    }
  }

  test("Blob: test write blob descriptor with partition") {
    withTable("t") {
      val blobData = new Array[Byte](1024 * 1024)
      RANDOM.nextBytes(blobData)
      val fileIO = new LocalFileIO
      val uri = "file://" + tempDBDir.toString + "/external_blob"
      try {
        val outputStream = fileIO.newOutputStream(new Path(uri), true)
        try outputStream.write(blobData)
        finally if (outputStream != null) outputStream.close()
      }

      val blobDescriptor = new BlobDescriptor(uri, 0, blobData.length)
      sql(
        "CREATE TABLE IF NOT EXISTS t (\n" + "id STRING,\n" + "name STRING,\n" + "file_size STRING,\n" + "crc64 STRING,\n" + "modified_time STRING,\n" + "content BINARY\n" + ") \n" +
          "PARTITIONED BY (ds STRING, batch STRING) \n" +
          "TBLPROPERTIES ('comment' = 'blob table','partition.expiration-time' = '365 d','row-tracking.enabled' = 'true','data-evolution.enabled' = 'true','blob-field' = 'content')")
      sql(
        "INSERT OVERWRITE TABLE t\nPARTITION(ds= '1017',batch = 'test') VALUES \n('1','paimon','1024','12345678','20241017',X'" + bytesToHex(
          blobDescriptor.serialize()) + "')")
      sql("ALTER TABLE t SET TBLPROPERTIES ('blob-as-descriptor'='true')")
      val newDescriptorBytes =
        sql("SELECT content FROM t WHERE id = '1'").collect()(0).get(0).asInstanceOf[Array[Byte]]
      val newBlobDescriptor = BlobDescriptor.deserialize(newDescriptorBytes)
      val options = new Options()
      options.set("warehouse", tempDBDir.toString)
      val catalogContext = CatalogContext.create(options)
      val uriReaderFactory = new UriReaderFactory(catalogContext)
      val blob = Blob.fromDescriptor(uriReaderFactory.create(newBlobDescriptor.uri), blobDescriptor)
      assert(util.Arrays.equals(blobData, blob.toData))

      sql("ALTER TABLE t SET TBLPROPERTIES ('blob-as-descriptor'='false')")
      checkAnswer(
        sql("SELECT id, name, content, _ROW_ID, _SEQUENCE_NUMBER FROM t WHERE id = 1"),
        Seq(Row("1", "paimon", blobData, 0, 1))
      )
    }
  }

  test("Blob: test write blob descriptor with built-in function") {
    withTable("t") {
      val blobData = new Array[Byte](1024 * 1024)
      RANDOM.nextBytes(blobData)
      val fileIO = new LocalFileIO
      val uri = "file://" + tempDBDir.toString + "/external_blob"
      try {
        val outputStream = fileIO.newOutputStream(new Path(uri), true)
        try outputStream.write(blobData)
        finally if (outputStream != null) outputStream.close()
      }

      val blobDescriptor = new BlobDescriptor(uri, 0, blobData.length)
      sql(
        "CREATE TABLE IF NOT EXISTS t (\n" + "id STRING,\n" + "name STRING,\n" + "file_size STRING,\n" + "crc64 STRING,\n" + "modified_time STRING,\n" + "content BINARY\n" + ") \n" +
          "PARTITIONED BY (ds STRING, batch STRING) \n" +
          "TBLPROPERTIES ('comment' = 'blob table','partition.expiration-time' = '365 d','row-tracking.enabled' = 'true','data-evolution.enabled' = 'true','blob-field' = 'content','blob-as-descriptor' = 'true')")
      sql(
        "INSERT OVERWRITE TABLE t\nPARTITION(ds= '1017',batch = 'test') VALUES \n('1','paimon','1024','12345678','20241017', sys.path_to_descriptor('" + uri + "'))")
      val newDescriptorBytes =
        sql("SELECT content FROM t WHERE id = '1'").collect()(0).get(0).asInstanceOf[Array[Byte]]
      val newBlobDescriptor = BlobDescriptor.deserialize(newDescriptorBytes)
      val options = new Options()
      options.set("warehouse", tempDBDir.toString)
      val catalogContext = CatalogContext.create(options)
      val uriReaderFactory = new UriReaderFactory(catalogContext)
      val blob = Blob.fromDescriptor(uriReaderFactory.create(newBlobDescriptor.uri), blobDescriptor)
      assert(util.Arrays.equals(blobData, blob.toData))

      checkAnswer(
        sql("SELECT sys.descriptor_to_string(content) FROM t"),
        Seq(Row(newBlobDescriptor.toString))
      )

      sql("ALTER TABLE t SET TBLPROPERTIES ('blob-as-descriptor'='false')")
      checkAnswer(
        sql("SELECT id, name, content, _ROW_ID, _SEQUENCE_NUMBER FROM t WHERE id = 1"),
        Seq(Row("1", "paimon", blobData, 0, 1))
      )
    }
  }

  test("Blob: test write blob descriptor from external storage") {
    val catalogName = "isolated_paimon"
    val databaseName = "external_blob_db"
    val paimonRoot = tempDBDir.getCanonicalPath + "/paimon-isolated-root"
    val externalRoot = tempDBDir.getCanonicalPath + "/external-blob-root"
    val isolatedPaimonRoot = "isolated://" + paimonRoot
    val isolatedExternalRoot = "isolated://" + externalRoot
    spark.conf.set(s"spark.sql.catalog.$catalogName", classOf[SparkCatalog].getName)
    spark.conf.set(s"spark.sql.catalog.$catalogName.warehouse", isolatedPaimonRoot)
    spark.conf.set(
      s"spark.sql.catalog.$catalogName.${IsolatedDirectoryFileIO.ROOT_DIR}",
      isolatedPaimonRoot)

    try {
      sql(s"CREATE DATABASE IF NOT EXISTS $catalogName.$databaseName")
      sql(s"USE $catalogName.$databaseName")

      val blobData = new Array[Byte](1024 * 1024)
      RANDOM.nextBytes(blobData)
      val blobPath = externalRoot + "/external_blob"
      val fileIO = new LocalFileIO
      val outputStream = fileIO.newOutputStream(new Path("file://" + blobPath), true)
      try outputStream.write(blobData)
      finally outputStream.close()

      val isolatedPath = "isolated://" + blobPath

      withTable("t") {
        sql(
          "CREATE TABLE t (id INT, data STRING, picture BINARY) TBLPROPERTIES (" +
            "'row-tracking.enabled'='true', " +
            "'data-evolution.enabled'='true', " +
            "'blob-field'='picture', " +
            "'blob-as-descriptor'='true')")

        // 1. directly writing raise expected errors.
        val error = intercept[Exception] {
          sql("INSERT INTO t VALUES (1, 'paimon', sys.path_to_descriptor('" + isolatedPath + "'))")
        }
        assert(
          exceptionContains(
            error,
            "Isolated file io only supports reading child of root directory") &&
            exceptionContains(error, "paimon-isolated-root") &&
            exceptionContains(error, "external-blob-root/external_blob"),
          exceptionMessages(error)
        )

        // 2. inject blob-descriptor io info through dynamic params.
        // this time writing should success.
        val descriptorRootOption =
          s"spark.paimon.$catalogName.$databaseName.t." +
            CoreOptions.BLOB_DESCRIPTOR_PREFIX + IsolatedDirectoryFileIO.ROOT_DIR
        withSparkSQLConf(descriptorRootOption -> isolatedExternalRoot) {
          sql("INSERT INTO t VALUES (2, 'paimon', sys.path_to_descriptor('" + isolatedPath + "'))")

          val newDescriptorBytes =
            sql("SELECT picture FROM t WHERE id = 2").collect()(0).get(0).asInstanceOf[Array[Byte]]
          val newBlobDescriptor = BlobDescriptor.deserialize(newDescriptorBytes)
          val options = new Options()
          options.set(IsolatedDirectoryFileIO.ROOT_DIR, isolatedPaimonRoot)
          val catalogContext = CatalogContext.create(options)
          val uriReaderFactory = new UriReaderFactory(catalogContext)
          val blob =
            Blob.fromDescriptor(uriReaderFactory.create(newBlobDescriptor.uri), newBlobDescriptor)
          assert(util.Arrays.equals(blobData, blob.toData))
        }
      }
    } finally {
      sql(s"USE paimon.$dbName0")
      sql(s"DROP DATABASE IF EXISTS $catalogName.$databaseName CASCADE")
    }
  }

  private def exceptionContains(throwable: Throwable, message: String): Boolean = {
    if (throwable == null) {
      false
    } else if (throwable.getMessage != null && throwable.getMessage.contains(message)) {
      true
    } else {
      exceptionContains(throwable.getCause, message)
    }
  }

  private def exceptionMessages(throwable: Throwable): String = {
    if (throwable == null) {
      ""
    } else {
      throwable.getClass.getName + ": " + throwable.getMessage + "\n" +
        exceptionMessages(throwable.getCause)
    }
  }

  test("Blob: test compaction") {
    withTable("t") {
      sql(
        "CREATE TABLE t (id INT, data STRING, picture BINARY) TBLPROPERTIES ('row-tracking.enabled'='true', 'data-evolution.enabled'='true', 'blob-field'='picture')")
      for (i <- 1 to 10) {
        sql("INSERT INTO t VALUES (" + i + ", 'paimon', X'48656C6C6F')")
      }
      sql("INSERT INTO t VALUES (1, 'paimon', X'48656C6C6F')")

      checkAnswer(
        sql("SELECT COUNT(*) FROM `t$files`"),
        Seq(Row(22))
      )
      sql("CALL paimon.sys.compact('t')").collect()
      checkAnswer(
        sql("SELECT COUNT(*) FROM `t$files`"),
        Seq(Row(12))
      )
      checkAnswer(
        sql("SELECT *, _ROW_ID, _SEQUENCE_NUMBER FROM t LIMIT 1"),
        Seq(Row(1, "paimon", Array[Byte](72, 101, 108, 108, 111), 0, 11))
      )
    }
  }

  test("Blob: merge-into rejects updating raw-data BLOB column") {
    withTable("s", "t") {
      sql("CREATE TABLE t (id INT, name STRING, picture BINARY) TBLPROPERTIES " +
        "('row-tracking.enabled'='true', 'data-evolution.enabled'='true', 'blob-field'='picture')")
      sql("INSERT INTO t VALUES (1, 'name1', X'48656C6C6F')")

      sql("CREATE TABLE s (id INT, picture BINARY)")
      sql("INSERT INTO s VALUES (1, X'4E4557')")

      val e = intercept[UnsupportedOperationException] {
        sql("""
              |MERGE INTO t
              |USING s
              |ON t.id = s.id
              |WHEN MATCHED THEN UPDATE SET t.picture = s.picture
              |""".stripMargin)
      }
      assert(e.getMessage.contains("raw-data BLOB"))
    }
  }

  test("Blob: merge-into updates non-blob column on descriptor blob table") {
    withTable("s", "t") {
      sql(
        "CREATE TABLE t (id INT, name STRING, picture BINARY) TBLPROPERTIES " +
          "('row-tracking.enabled'='true', 'data-evolution.enabled'='true', " +
          "'blob-field'='picture', 'blob-descriptor-field'='picture')")

      // Insert with a descriptor pointing to a real file
      val blobData = new Array[Byte](256)
      RANDOM.nextBytes(blobData)
      val fileIO = new LocalFileIO()
      val uri = "file://" + tempDBDir.getCanonicalPath + "/external_desc_blob"
      val out = fileIO.newOutputStream(new Path(uri), true)
      try { out.write(blobData) }
      finally { out.close() }
      val desc = new BlobDescriptor(uri, 0, blobData.length)
      sql(s"INSERT INTO t VALUES (1, 'name1', X'${bytesToHex(desc.serialize())}')")
      sql(s"INSERT INTO t VALUES (2, 'name2', X'${bytesToHex(desc.serialize())}')")

      sql("CREATE TABLE s (id INT, name STRING)")
      sql("INSERT INTO s VALUES (1, 'updated_name1')")

      // Update only the 'name' column — should succeed for descriptor-based blob table
      sql("""
            |MERGE INTO t
            |USING s
            |ON t.id = s.id
            |WHEN MATCHED THEN UPDATE SET t.name = s.name
            |""".stripMargin)

      checkAnswer(
        sql("SELECT id, name FROM t ORDER BY id"),
        Seq(Row(1, "updated_name1"), Row(2, "name2"))
      )
    }
  }

  test("Blob: merge-into updates descriptor blob column with external storage end-to-end") {
    withTable("s", "t") {
      val externalStoragePath = tempDBDir.getCanonicalPath + "/external-storage-blob-merge-path"
      sql(
        s"CREATE TABLE t (id INT, name STRING, picture BINARY) TBLPROPERTIES " +
          s"('row-tracking.enabled'='true', 'data-evolution.enabled'='true', " +
          s"'blob-field'='picture', 'blob-descriptor-field'='picture', " +
          s"'blob-external-storage-field'='picture', " +
          s"'blob-external-storage-path'='$externalStoragePath')")

      // Insert initial row (writes raw data to external storage and stores descriptor bytes)
      sql("INSERT INTO t VALUES (1, 'name1', X'48656C6C6F')")
      sql("INSERT INTO t VALUES (2, 'name2', X'5945')")

      sql("CREATE TABLE s (id INT, name STRING)")
      sql("INSERT INTO s VALUES (1, 'updated_name1')")

      // Update the 'name' column via MERGE INTO
      sql("""
            |MERGE INTO t
            |USING s
            |ON t.id = s.id
            |WHEN MATCHED THEN UPDATE SET t.name = s.name
            |""".stripMargin)

      checkAnswer(
        sql("SELECT id, name FROM t ORDER BY id"),
        Seq(Row(1, "updated_name1"), Row(2, "name2"))
      )
    }
  }

  private val HEX_ARRAY = "0123456789ABCDEF".toCharArray

  def bytesToHex(bytes: Array[Byte]): String = {
    val hexChars = new Array[Char](bytes.length * 2)
    for (j <- bytes.indices) {
      val v = bytes(j) & 0xff
      hexChars(j * 2) = HEX_ARRAY(v >>> 4)
      hexChars(j * 2 + 1) = HEX_ARRAY(v & 0x0f)
    }
    new String(hexChars)
  }
}

class BlobTestWithV2Write extends BlobTestBase {
  override def sparkConf: SparkConf = {
    super.sparkConf.set("spark.paimon.write.use-v2-write", "true")
  }
}
