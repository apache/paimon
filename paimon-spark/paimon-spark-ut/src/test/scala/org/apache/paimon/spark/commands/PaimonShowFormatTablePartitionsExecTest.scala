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

package org.apache.paimon.spark.commands

import org.apache.paimon.catalog.{CatalogContext, Identifier}
import org.apache.paimon.fs.Path
import org.apache.paimon.fs.local.LocalFileIO
import org.apache.paimon.options.Options
import org.apache.paimon.spark.PaimonSparkTestBase
import org.apache.paimon.spark.format.{FormatTablePartitionCatalog, FormatTablePartitionPage, PaimonFormatTable}
import org.apache.paimon.table.FormatTable
import org.apache.paimon.types.DataTypes

import org.apache.spark.sql.catalyst.analysis.ResolvedPartitionSpec
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, GenericInternalRow}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.types.StringType
import org.apache.spark.unsafe.types.UTF8String

import java.nio.file.Files
import java.util.{Collections, LinkedHashMap, List => JList, Map => JMap}

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

class PaimonShowFormatTablePartitionsExecTest extends PaimonSparkTestBase {

  test("managed Format Table SHOW PARTITIONS has a bounded catalog executor") {
    Class.forName("org.apache.paimon.spark.commands.PaimonShowFormatTablePartitionsExec")
  }

  test("managed Format Table SHOW PARTITIONS exposes its default Driver cap") {
    assert(PaimonShowFormatTablePartitionsExec.DEFAULT_MAX_RESULTS == 10000)
  }

  test("managed Format Table SHOW PARTITIONS pages catalog results and uses Spark formatting") {
    val calls = ArrayBuffer.empty[(Map[String, String], String, Int)]
    val gateway = new FormatTablePartitionCatalog {
      override def createPartitions(
          partitions: JList[JMap[String, String]],
          ignoreIfExists: Boolean): Unit = unsupported()

      override def dropPartitions(partitions: JList[JMap[String, String]]): Unit = unsupported()

      override def listPartitionsByNames(
          partitions: JList[JMap[String, String]]): JList[JMap[String, String]] = unsupported()

      override def listPartitions(
          prefix: JMap[String, String],
          pageToken: String,
          pageSize: Int): FormatTablePartitionPage = {
        calls += ((prefix.asScala.toMap, pageToken, pageSize))
        pageToken match {
          case null => FormatTablePartitionPage(List(partition("z")).asJava, "page-2")
          case "page-2" => FormatTablePartitionPage(List(partition("a=1")).asJava, null)
          case token => fail(s"Unexpected page token $token")
        }
      }
    }
    val table = new PaimonFormatTable(managedFormatTable(), gateway)
    val spec = ResolvedPartitionSpec(
      Seq("dt"),
      new GenericInternalRow(Array[Any](UTF8String.fromString("2026/07/15"))))
    val output = Seq(AttributeReference("partition", StringType, nullable = false)())
    val constructor = classOf[PaimonShowFormatTablePartitionsExec].getConstructors
      .find(_.getParameterCount == 4)
      .getOrElse(fail("The catalog SHOW executor constructor is missing"))
    val exec = constructor
      .newInstance(output, table, Some(spec), Int.box(3))
      .asInstanceOf[SparkPlan]

    val actual = exec.executeCollect().map(_.getUTF8String(0).toString).toSeq

    assert(actual == Seq("dt=2026%2F07%2F15/region=a%3D1", "dt=2026%2F07%2F15/region=z"))
    assert(
      calls.toSeq == Seq(
        (Map("dt" -> "2026/07/15"), null, 4),
        (Map("dt" -> "2026/07/15"), "page-2", 3)))
  }

  test("managed Format Table SHOW PARTITIONS rejects results above the Driver cap") {
    val gateway = new FormatTablePartitionCatalog {
      override def createPartitions(
          partitions: JList[JMap[String, String]],
          ignoreIfExists: Boolean): Unit = unsupported()

      override def dropPartitions(partitions: JList[JMap[String, String]]): Unit = unsupported()

      override def listPartitionsByNames(
          partitions: JList[JMap[String, String]]): JList[JMap[String, String]] = unsupported()

      override def listPartitions(
          prefix: JMap[String, String],
          pageToken: String,
          pageSize: Int): FormatTablePartitionPage = {
        assert(pageSize == 3)
        FormatTablePartitionPage(List(partition("a"), partition("b"), partition("c")).asJava, null)
      }
    }
    val exec = PaimonShowFormatTablePartitionsExec(
      Seq(AttributeReference("partition", StringType, nullable = false)()),
      new PaimonFormatTable(managedFormatTable(), gateway),
      None,
      maxResults = 2)

    val error = intercept[RuntimeException] {
      exec.executeCollect()
    }

    assert(error.getMessage.contains("2"))
    assert(error.getMessage.contains("CALL sys.list_format_table_partitions"))
  }

  test("managed Format Table SHOW PARTITIONS continues after an empty catalog page") {
    val tokens = ArrayBuffer.empty[String]
    val gateway = new FormatTablePartitionCatalog {
      override def createPartitions(
          partitions: JList[JMap[String, String]],
          ignoreIfExists: Boolean): Unit = unsupported()

      override def dropPartitions(partitions: JList[JMap[String, String]]): Unit = unsupported()

      override def listPartitionsByNames(
          partitions: JList[JMap[String, String]]): JList[JMap[String, String]] = unsupported()

      override def listPartitions(
          prefix: JMap[String, String],
          pageToken: String,
          pageSize: Int): FormatTablePartitionPage = {
        tokens += pageToken
        pageToken match {
          case null => FormatTablePartitionPage(Collections.emptyList(), "page-2")
          case "page-2" => FormatTablePartitionPage(List(partition("a")).asJava, null)
          case token => fail(s"Unexpected page token $token")
        }
      }
    }
    val exec = PaimonShowFormatTablePartitionsExec(
      Seq(AttributeReference("partition", StringType, nullable = false)()),
      new PaimonFormatTable(managedFormatTable(), gateway),
      None,
      maxResults = 2)

    val actual = exec.executeCollect().map(_.getUTF8String(0).toString).toSeq

    assert(actual == Seq("dt=2026%2F07%2F15/region=a"))
    assert(tokens.toSeq == Seq(null, "page-2"))
  }

  test("managed Format Table SHOW PARTITIONS treats an empty next-page token as terminal") {
    var calls = 0
    val gateway = new FormatTablePartitionCatalog {
      override def createPartitions(
          partitions: JList[JMap[String, String]],
          ignoreIfExists: Boolean): Unit = unsupported()

      override def dropPartitions(partitions: JList[JMap[String, String]]): Unit = unsupported()

      override def listPartitionsByNames(
          partitions: JList[JMap[String, String]]): JList[JMap[String, String]] = unsupported()

      override def listPartitions(
          prefix: JMap[String, String],
          pageToken: String,
          pageSize: Int): FormatTablePartitionPage = {
        calls += 1
        if (calls > 1) {
          fail("An empty page token must not be fetched")
        }
        FormatTablePartitionPage(List(partition("a")).asJava, "")
      }
    }
    val exec = PaimonShowFormatTablePartitionsExec(
      Seq(AttributeReference("partition", StringType, nullable = false)()),
      new PaimonFormatTable(managedFormatTable(), gateway),
      None,
      maxResults = 2)

    val actual = exec.executeCollect().map(_.getUTF8String(0).toString).toSeq

    assert(actual == Seq("dt=2026%2F07%2F15/region=a"))
    assert(calls == 1)
  }

  private def partition(region: String): JMap[String, String] = {
    val result = new LinkedHashMap[String, String]()
    result.put("region", region)
    result.put("dt", "2026/07/15")
    result
  }

  private def managedFormatTable(): FormatTable = {
    FormatTable
      .builder()
      .fileIO(LocalFileIO.create)
      .identifier(Identifier.create("test_db", "format_table"))
      .rowType(
        DataTypes.ROW(
          DataTypes.FIELD(0, "id", DataTypes.INT()),
          DataTypes.FIELD(1, "dt", DataTypes.STRING()),
          DataTypes.FIELD(2, "region", DataTypes.STRING())))
      .partitionKeys(Seq("dt", "region").asJava)
      .location(new Path(Files.createTempDirectory("format_table_show").toUri).toString)
      .format(FormatTable.Format.CSV)
      .options(Map("metastore.partitioned-table" -> "true").asJava)
      .catalogContext(CatalogContext.create(new Options))
      .build()
  }

  private def unsupported[T](): T = {
    throw new UnsupportedOperationException("Not used by SHOW PARTITIONS")
  }
}
