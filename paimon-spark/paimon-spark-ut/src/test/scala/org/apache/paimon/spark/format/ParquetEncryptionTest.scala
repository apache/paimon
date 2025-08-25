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

package org.apache.paimon.spark.format

import org.apache.paimon.format.parquet.ParquetUtil
import org.apache.paimon.fs.Path
import org.apache.paimon.spark.PaimonSparkTestBase

import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.Row

class ParquetEncryptionTest extends PaimonSparkTestBase {

  import testImplicits._

  val encryptionConf = Map(
    "parquet.encryption.kms.client.class" -> "org.apache.paimon.format.parquet.crypto.InMemoryKMS",
    "parquet.encryption.key.list" -> "footerKey:AAECAwQFBgcICQoLDA0ODw==, columnKey:AAECAwQFBgcICQoLDA0ODw==",
    "parquet.crypto.factory.class" -> "org.apache.parquet.crypto.keytools.PropertiesDrivenCryptoFactory"
  )

  val hadoopConf: Configuration = {
    val conf = new Configuration()
    encryptionConf.foreach(kv => conf.set(kv._1, kv._2))
    conf
  }

  def withEncryptionConf(f: => Unit): Unit = {
    withSparkSQLConf(encryptionConf.map(kv => (s"spark.paimon.${kv._1}", kv._2)).toList: _*)(f)
  }

  test("Paimon encryption: read and write with encryption") {
    withEncryptionConf {
      withTable("t") {
        sql("""
              |CREATE TABLE t (a INT, b STRING) TBLPROPERTIES (
              |'file.format' = 'parquet',
              |'parquet.encryption.footer.key' = 'footerKey',
              |'parquet.encryption.column.keys' = 'columnKey:a,b'
              |)
              |""".stripMargin)

        sql("INSERT INTO t VALUES (1, 'a')")
        checkAnswer(sql("SELECT * FROM t"), Seq(Row(1, "a")))

        val filePath = new Path(
          sql("SELECT __paimon_file_path FROM t LIMIT 1")
            .select("__paimon_file_path")
            .as[String]
            .head)
        assertFooterEncryptionType(filePath, "ENCRYPTED_FOOTER")
      }
    }
  }

  test("Paimon encryption: read and write dataframe with encryption") {
    withEncryptionConf {
      withTable("t") {
        val data = Seq((1, "a")).toDF("a", "b")

        data.write
          .format("paimon")
          .option("parquet.encryption.footer.key", "footerKey")
          .option("parquet.encryption.column.keys", "columnKey:a,b")
          .saveAsTable("t")

        checkAnswer(
          spark.read
            .format("paimon")
            .table("t"),
          Seq(Row(1, "a")))

        val filePath = new Path(
          sql("SELECT __paimon_file_path FROM t LIMIT 1")
            .select("__paimon_file_path")
            .as[String]
            .head)
        assertFooterEncryptionType(filePath, "ENCRYPTED_FOOTER")
      }
    }
  }

  private def assertFooterEncryptionType(filePath: Path, expert: String): Unit = {
    val reader =
      ParquetUtil.getParquetReader(fileIO, filePath, fileIO.getFileSize(filePath), hadoopConf)
    val encryptionType = reader.getFooter.getFileMetaData.getEncryptionType
    assert(encryptionType.toString == expert)
    reader.close()
  }
}
