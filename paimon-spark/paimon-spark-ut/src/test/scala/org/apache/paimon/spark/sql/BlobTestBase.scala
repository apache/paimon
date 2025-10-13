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

import org.apache.paimon.catalog.CatalogContext
import org.apache.paimon.data.Blob
import org.apache.paimon.data.BlobDescriptor
import org.apache.paimon.fs.Path
import org.apache.paimon.fs.local.LocalFileIO
import org.apache.paimon.options.Options
import org.apache.paimon.spark.PaimonSparkTestBase
import org.apache.paimon.utils.UriReaderFactory

import org.apache.spark.sql.Row

import java.util
import java.util.Random

class BlobTestBase extends PaimonSparkTestBase {

  private val RANDOM = new Random

  test("Blob: test basic") {
    withTable("t") {
      sql(
        "CREATE TABLE t (id INT, data STRING, picture BINARY) TBLPROPERTIES ('row-tracking.enabled'='true', 'data-evolution.enabled'='true', 'blob-field'='picture')")
      sql("INSERT INTO t VALUES (1, 'paimon', X'48656C6C6F')")

      checkAnswer(
        sql("SELECT *, _ROW_ID, _SEQUENCE_NUMBER FROM t"),
        Seq(Row(1, "paimon", Array[Byte](72, 101, 108, 108, 111), 0, 1))
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
        "CREATE TABLE t (id INT, data STRING, picture BINARY) TBLPROPERTIES ('row-tracking.enabled'='true', 'data-evolution.enabled'='true', 'blob-field'='picture', 'blob-as-descriptor'='true')")
      sql("INSERT INTO t VALUES (1, 'paimon', X'" + bytesToHex(blobDescriptor.serialize()) + "')")

      val newDescriptorBytes =
        sql("SELECT picture FROM t").collect()(0).get(0).asInstanceOf[Array[Byte]]
      val newBlobDescriptor = BlobDescriptor.deserialize(newDescriptorBytes)
      val options = new Options()
      options.set("warehouse", tempDBDir.toString)
      val catalogContext = CatalogContext.create(options)
      val uriReaderFactory = new UriReaderFactory(catalogContext)
      val blob = Blob.fromDescriptor(uriReaderFactory.create(newBlobDescriptor.uri), blobDescriptor)
      assert(util.Arrays.equals(blobData, blob.toData))

      sql("ALTER TABLE t SET TBLPROPERTIES ('blob-as-descriptor'='false')")
      checkAnswer(
        sql("SELECT *, _ROW_ID, _SEQUENCE_NUMBER FROM t"),
        Seq(Row(1, "paimon", blobData, 0, 1))
      )
    }
  }

  private val HEX_ARRAY = "0123456789ABCDEF".toCharArray

  def bytesToHex(bytes: Array[Byte]): String = {
    val hexChars = new Array[Char](bytes.length * 2)
    for (j <- 0 until bytes.length) {
      val v = bytes(j) & 0xff
      hexChars(j * 2) = HEX_ARRAY(v >>> 4)
      hexChars(j * 2 + 1) = HEX_ARRAY(v & 0x0f)
    }
    new String(hexChars)
  }
}
