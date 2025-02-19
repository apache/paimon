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

package org.apache.paimon.spark.catalyst.analysis

import org.apache.paimon.spark.{DataConverter, SparkTable, SparkTypeUtils, SparkUtils}
import org.apache.paimon.spark.catalog.SparkBaseCatalog
import org.apache.paimon.utils.{InternalRowUtils, TypeUtils}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{ApplyFunctionExpression, Cast, Expression, Literal}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.connector.catalog.PaimonCatalogImplicits._
import org.apache.spark.sql.types.StringType
import org.apache.spark.unsafe.types.UTF8String

import scala.jdk.CollectionConverters._

/** A rule to replace Paimon functions with literal values. */
case class ReplacePaimonFunctions(spark: SparkSession) extends Rule[LogicalPlan] {
  private def replaceMaxPt(func: ApplyFunctionExpression): Expression = {
    assert(func.children.size == 1)
    assert(func.children.head.dataType == StringType)
    if (!func.children.head.isInstanceOf[Literal]) {
      throw new UnsupportedOperationException("Table name must be a literal")
    }
    val tableName = func.children.head.eval().asInstanceOf[UTF8String]
    if (tableName == null) {
      throw new UnsupportedOperationException("Table name cannot be null")
    }
    val catalogAndIdentifier = SparkUtils
      .catalogAndIdentifier(
        spark,
        tableName.toString,
        spark.sessionState.catalogManager.currentCatalog)
    if (!catalogAndIdentifier.catalog().isInstanceOf[SparkBaseCatalog]) {
      throw new UnsupportedOperationException(
        s"${catalogAndIdentifier.catalog()} is not a Paimon catalog")
    }

    val table =
      catalogAndIdentifier.catalog.asTableCatalog.loadTable(catalogAndIdentifier.identifier())
    assert(table.isInstanceOf[SparkTable])
    val sparkTable = table.asInstanceOf[SparkTable]
    if (sparkTable.table.partitionKeys().size() == 0) {
      throw new UnsupportedOperationException(s"$table is not a partitioned table")
    }

    val toplevelPartitionType =
      TypeUtils.project(sparkTable.table.rowType, sparkTable.table.partitionKeys()).getTypeAt(0)
    val partitionValues = sparkTable.table.newReadBuilder.newScan
      .listPartitionEntries()
      .asScala
      .filter(_.fileCount() > 0)
      .map {
        partitionEntry => InternalRowUtils.get(partitionEntry.partition(), 0, toplevelPartitionType)
      }
      .sortWith(InternalRowUtils.compare(_, _, toplevelPartitionType.getTypeRoot) < 0)
      .map(DataConverter.fromPaimon(_, toplevelPartitionType))
    if (partitionValues.isEmpty) {
      throw new UnsupportedOperationException(
        s"$table has no partitions or none of the partitions have any data")
    }

    val sparkType = SparkTypeUtils.fromPaimonType(toplevelPartitionType)
    val literal = Literal(partitionValues.last, sparkType)
    Cast(literal, func.dataType)
  }

  override def apply(plan: LogicalPlan): LogicalPlan = {
    plan.resolveExpressions {
      case func: ApplyFunctionExpression
          if func.function.name() == "max_pt" &&
            func.function.canonicalName().startsWith("paimon") =>
        replaceMaxPt(func)
    }
  }
}
