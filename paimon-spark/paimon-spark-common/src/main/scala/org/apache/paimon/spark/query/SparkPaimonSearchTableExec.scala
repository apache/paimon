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

package org.apache.paimon.spark.query

import org.apache.paimon.globalindex.{EsIndexSearch, EsSearchRequest}
import org.apache.paimon.globalindex.EsIndexSearch.Hit
import org.apache.paimon.spark.SparkTable
import org.apache.paimon.table.FileStoreTable

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, GenericInternalRow, UnsafeProjection}
import org.apache.spark.sql.connector.catalog.{Identifier, TableCatalog}
import org.apache.spark.sql.execution.LeafExecNode
import org.apache.spark.unsafe.types.UTF8String

import scala.collection.JavaConverters._

/** Index shard queries run on executors; the driver merges a bounded global score window. */
case class SparkPaimonSearchTableExec(
    catalogName: String,
    envelope: String,
    override val output: Seq[Attribute])(@transient private val sparkSession: SparkSession)
  extends LeafExecNode {

  override protected def otherCopyArgs: Seq[AnyRef] = Seq(sparkSession)

  override protected def doExecute(): RDD[InternalRow] = {
    val request = EsSearchRequest.parse(envelope)
    val catalog = sparkSession.sessionState.catalogManager.catalog(catalogName) match {
      case value: TableCatalog => value
      case _ => throw new IllegalArgumentException("Expected a Paimon table catalog")
    }
    val original = catalog.loadTable(Identifier.of(Array(request.database), request.table)) match {
      case SparkTable(value: FileStoreTable) => value
      case _ =>
        throw new IllegalArgumentException("paimon_es_search requires a Paimon file-store table")
    }
    val execution = EsIndexSearch.prepare(original, request)
    val plan = execution.plan
    val query = execution.query
    val sc = sparkSession.sparkContext
    val shards = plan.shards.asScala.toVector
    implicit val order: Ordering[Hit] = Ordering.by(hit => (-hit.score, hit.rowId))
    val selected = sc
      .parallelize(shards, math.min(shards.size, math.max(1, sc.defaultParallelism)))
      .flatMap(shard => shard.search(query).asScala)
      .takeOrdered(query.limit())
    val projection = UnsafeProjection.create(output.map(_.dataType).toArray)
    val rows = execution
      .finish(selected.toSeq.asJava)
      .records()
      .asScala
      .map {
        record =>
          val values = record.map {
            case value: String => UTF8String.fromString(value)
            case value => value
          }
          projection(new GenericInternalRow(values.asInstanceOf[Array[Any]])).copy(): InternalRow
      }
      .toVector
    sc.parallelize(rows, 1)
  }
}
