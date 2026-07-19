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

import org.apache.paimon.PagedList
import org.apache.paimon.catalog.{Catalog, Identifier}
import org.apache.paimon.partition.Partition

import org.apache.spark.SparkFunSuite

import java.lang.reflect.{InvocationHandler, Method, Proxy}
import java.util.{Arrays, LinkedHashMap, List => JList, Map => JMap}

import scala.collection.JavaConverters._

class CatalogFormatTablePartitionCatalogTest extends SparkFunSuite {

  private val identifier = Identifier.create("db", "format_table")

  test("ADD forwards the complete batch and ignoreIfExists to the catalog") {
    var forwardedIdentifier: Identifier = null
    var forwardedPartitions = Seq.empty[JMap[String, String]]
    var forwardedFlags = Seq.empty[Boolean]
    val catalog = proxyCatalog {
      (_, method, args) =>
        method.getName match {
          case "createPartitions" if args.length == 3 =>
            forwardedIdentifier = args(0).asInstanceOf[Identifier]
            forwardedPartitions = args(1).asInstanceOf[JList[JMap[String, String]]].asScala.toSeq
            forwardedFlags :+= args(2).asInstanceOf[java.lang.Boolean].booleanValue()
            null
          case _ => null
        }
    }
    val partitions = Arrays.asList(spec("dt", "20260715"), spec("dt", "20260716"))

    // Strict-ADD atomicity is the catalog service's job (the managed ADD command skips Spark's
    // client-side existence precheck), so the flag must reach the catalog unchanged.
    adapter(catalog).createPartitions(partitions, ignoreIfExists = true)
    adapter(catalog).createPartitions(partitions, ignoreIfExists = false)

    assert(forwardedFlags == Seq(true, false))
    assert(forwardedIdentifier == identifier)
    assert(forwardedPartitions == partitions.asScala)
  }

  test("DROP unregisters oversized expansions in bounded batches") {
    var batchSizes = Seq.empty[Int]
    var dropped = Seq.empty[JMap[String, String]]
    val catalog = proxyCatalog {
      (_, method, args) =>
        method.getName match {
          case "dropPartitions" =>
            val batch = args(1).asInstanceOf[JList[JMap[String, String]]]
            batchSizes :+= batch.size()
            dropped ++= batch.asScala
            null
          case _ => null
        }
    }
    val partitions = new java.util.ArrayList[JMap[String, String]]()
    (0 until 2500).foreach(i => partitions.add(spec("dt", f"d$i%04d")))

    adapter(catalog).dropPartitions(partitions)

    assert(batchSizes == Seq(1000, 1000, 500))
    assert(dropped == partitions.asScala)
  }

  test("map catalog list-by-names and paged prefix results to partition specs") {
    var namesRequest = Seq.empty[JMap[String, String]]
    var pageToken: String = null
    var pageSize = 0
    var namePattern: String = null
    val catalog = proxyCatalog {
      (_, method, args) =>
        method.getName match {
          case "listPartitionsByNames" =>
            namesRequest = args(1).asInstanceOf[JList[JMap[String, String]]].asScala.toSeq
            Arrays.asList(partition(spec("dt", "20260715")))
          case "listPartitionsPaged" =>
            pageSize = args(1).asInstanceOf[Integer]
            pageToken = args(2).asInstanceOf[String]
            namePattern = args(3).asInstanceOf[String]
            new PagedList[Partition](
              Arrays.asList(partition(spec("dt", "20260715")), partition(spec("dt", "20260716"))),
              "next")
          case _ => null
        }
    }
    val gateway = adapter(catalog)
    val requested = Arrays.asList(spec("dt", "20260715"), spec("dt", "missing"))

    assert(
      gateway.listPartitionsByNames(requested).asScala.map(_.asScala.toMap) ==
        Seq(Map("dt" -> "20260715")))
    assert(namesRequest == requested.asScala)

    val page = gateway.listPartitions(spec("dt", "20260715"), "from", 17)
    assert(page.partitions.asScala.map(_.asScala.toMap) == Seq(Map("dt" -> "20260715")))
    assert(page.nextPageToken == "next")
    assert(pageToken == "from")
    assert(pageSize == 17)
    assert(namePattern == null)
  }

  test("partial partition specs are enforced client-side, not as server patterns") {
    var serverPattern: String = "not-called"
    val catalog = proxyCatalog {
      (_, method, args) =>
        method.getName match {
          case "listPartitionsPaged" =>
            serverPattern = args(3).asInstanceOf[String]
            new PagedList[Partition](
              Arrays.asList(
                partition(multiSpec("dt" -> "20260715", "hh" -> "10")),
                partition(multiSpec("dt" -> "20260715", "hh" -> "11"))),
              null)
          case _ => null
        }
    }

    val page = adapter(catalog).listPartitions(spec("hh", "10"), null, 17)

    assert(serverPattern == null)
    assert(
      page.partitions.asScala.map(_.asScala.toMap) ==
        Seq(Map("dt" -> "20260715", "hh" -> "10")))
  }

  test("managed reads propagate catalog listing failures") {
    val catalog = proxyCatalog {
      (_, method, _) =>
        method.getName match {
          case "listPartitionsByNames" | "listPartitionsPaged" =>
            throw new UnsupportedOperationException("managed listing unavailable")
          case _ => null
        }
    }
    val gateway = adapter(catalog)

    val pointError = intercept[UnsupportedOperationException] {
      gateway.listPartitionsByNames(Arrays.asList(spec("dt", "20260715")))
    }
    val pageError = intercept[UnsupportedOperationException] {
      gateway.listPartitions(new LinkedHashMap[String, String](), null, 10)
    }

    assert(pointError.getMessage.contains("managed listing unavailable"))
    assert(pageError.getMessage.contains("managed listing unavailable"))
  }

  private def adapter(catalog: Catalog): FormatTablePartitionCatalog =
    CatalogFormatTablePartitionCatalog(catalog, identifier)

  private def proxyCatalog(handler: (Any, Method, Array[AnyRef]) => AnyRef): Catalog = {
    Proxy
      .newProxyInstance(
        getClass.getClassLoader,
        Array(classOf[Catalog]),
        new InvocationHandler {
          override def invoke(proxy: Any, method: Method, args: Array[AnyRef]): AnyRef = {
            handler(proxy, method, Option(args).getOrElse(Array.empty[AnyRef]))
          }
        }
      )
      .asInstanceOf[Catalog]
  }

  private def spec(key: String, value: String): JMap[String, String] = {
    val result = new LinkedHashMap[String, String]()
    result.put(key, value)
    result
  }

  private def multiSpec(entries: (String, String)*): JMap[String, String] = {
    val result = new LinkedHashMap[String, String]()
    entries.foreach { case (key, value) => result.put(key, value) }
    result
  }

  private def partition(spec: JMap[String, String]): Partition =
    new Partition(spec, 0L, 0L, 0L, 0L, 0, false)
}
