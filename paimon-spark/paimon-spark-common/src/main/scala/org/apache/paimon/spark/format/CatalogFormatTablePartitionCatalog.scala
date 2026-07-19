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

import org.apache.paimon.catalog.{Catalog, Identifier}
import org.apache.paimon.partition.Partition

import java.util.{ArrayList, List => JList, Map => JMap}

import scala.collection.JavaConverters._

/** Catalog-backed partition gateway for a single Format Table. */
case class CatalogFormatTablePartitionCatalog(catalog: Catalog, identifier: Identifier)
  extends FormatTablePartitionCatalog {

  override def createPartitions(
      partitions: JList[JMap[String, String]],
      ignoreIfExists: Boolean): Unit = {
    // The flag reaches the server unchanged: the managed ADD command skips Spark's client-side
    // existence precheck, so strict-ADD atomicity (reject the whole batch when any partition
    // exists) is enforced by the catalog service.
    catalog.createPartitions(identifier, partitions, ignoreIfExists)
  }

  override def dropPartitions(partitions: JList[JMap[String, String]]): Unit = {
    // Catalog contract: dropPartitions ignores non-existent partitions and, for managed format
    // tables, unregisters metadata only. Data deletion is performed by the caller afterwards.
    // Unregister in bounded batches: a partial-spec DROP can expand to more leaf partitions than
    // a catalog service accepts in one request (e.g. dropping a year of a minute-partitioned
    // table). Dropping ignores missing partitions, so a mid-way failure leaves a state a retry
    // converges from.
    var start = 0
    while (start < partitions.size()) {
      val end =
        math.min(start + CatalogFormatTablePartitionCatalog.MUTATION_BATCH_SIZE, partitions.size())
      catalog.dropPartitions(identifier, partitions.subList(start, end))
      start = end
    }
  }

  override def listPartitionsByNames(
      partitions: JList[JMap[String, String]]): JList[JMap[String, String]] = {
    toSpecs(catalog.listPartitionsByNames(identifier, partitions))
  }

  override def listPartitions(
      prefix: JMap[String, String],
      pageToken: String,
      pageSize: Int): FormatTablePartitionPage = {
    val page = catalog.listPartitionsPaged(identifier, Int.box(pageSize), pageToken, null)
    val partitions = Option(page.getElements)
      .map(_.asScala)
      .getOrElse(Seq.empty)
      .filter(partition => matchesPrefix(partition, prefix))
      .asJava
    FormatTablePartitionPage(toSpecs(partitions), page.getNextPageToken)
  }

  private def matchesPrefix(partition: Partition, prefix: JMap[String, String]): Boolean = {
    prefix == null || prefix.asScala.forall {
      case (key, value) => value == partition.spec().get(key)
    }
  }

  private def toSpecs(partitions: JList[Partition]): JList[JMap[String, String]] = {
    val result = new ArrayList[JMap[String, String]](partitions.size())
    partitions.asScala.foreach(partition => result.add(partition.spec()))
    result
  }
}

object CatalogFormatTablePartitionCatalog {

  /** Mirror of the catalog page size; keeps a single mutation request bounded. */
  val MUTATION_BATCH_SIZE = 1000
}
