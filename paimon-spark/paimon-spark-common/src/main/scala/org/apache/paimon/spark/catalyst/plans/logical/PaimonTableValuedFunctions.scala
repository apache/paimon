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

package org.apache.paimon.spark.catalyst.plans.logical

import org.apache.paimon.CoreOptions
import org.apache.paimon.spark.SparkCatalog
import org.apache.paimon.spark.catalog.Catalogs

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.analysis.FunctionRegistryBase
import org.apache.spark.sql.catalyst.analysis.TableFunctionRegistry.TableFunctionBuilder
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, ExpressionInfo}
import org.apache.spark.sql.catalyst.plans.logical.{LeafNode, LogicalPlan}
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import scala.collection.JavaConverters._

object PaimonTableValuedFunctions {

  val INCREMENTAL_QUERY = "paimon_incremental_query"

  val supportedFnNames: Seq[String] = Seq(INCREMENTAL_QUERY)

  type TableFunctionDescription = (FunctionIdentifier, ExpressionInfo, TableFunctionBuilder)

  def getTableValueFunctionInjection(fnName: String): TableFunctionDescription = {
    val (info, builder) = fnName match {
      case INCREMENTAL_QUERY =>
        FunctionRegistryBase.build[IncrementalQuery](fnName, since = None)
      case _ =>
        throw new Exception(s"Function $fnName isn't a supported table valued function.")
    }
    val ident = FunctionIdentifier(fnName)
    (ident, info, builder)
  }

  def resolvePaimonTableValuedFunction(
      spark: SparkSession,
      tvf: PaimonTableValueFunction): LogicalPlan = {
    val args = tvf.expressions

    val sessionState = spark.sessionState
    val catalogManager = sessionState.catalogManager

    val sparkCatalog = new SparkCatalog()
    val currentCatalog = catalogManager.currentCatalog.name()
    sparkCatalog.initialize(
      currentCatalog,
      Catalogs.catalogOptions(currentCatalog, spark.sessionState.conf))

    val tableId = sessionState.sqlParser.parseTableIdentifier(args.head.eval().toString)
    val namespace = tableId.database.map(Array(_)).getOrElse(catalogManager.currentNamespace)
    val ident = Identifier.of(namespace, tableId.table)
    val sparkTable = sparkCatalog.loadTable(ident)
    val options = tvf.parseArgs(args.tail)
    DataSourceV2Relation.create(
      sparkTable,
      Some(sparkCatalog),
      Some(ident),
      new CaseInsensitiveStringMap(options.asJava))
  }
}

/**
 * Represents an unresolved Paimon Table Valued Function
 *
 * @param fnName
 *   can be one of [[PaimonTableValuedFunctions.supportedFnNames]].
 */
abstract class PaimonTableValueFunction(val fnName: String) extends LeafNode {

  override def output: Seq[Attribute] = Nil

  override lazy val resolved = false

  val args: Seq[Expression]

  def parseArgs(args: Seq[Expression]): Map[String, String]

}

/** Plan for the "paimon_incremental_query" function */
case class IncrementalQuery(override val args: Seq[Expression])
  extends PaimonTableValueFunction(PaimonTableValuedFunctions.INCREMENTAL_QUERY) {

  override def parseArgs(args: Seq[Expression]): Map[String, String] = {
    assert(
      args.size >= 1 && args.size <= 2,
      "paimon_incremental_query needs two parameters: startSnapshotId, and endSnapshotId.")

    val start = args.head.eval().toString
    val end = args.last.eval().toString
    Map(CoreOptions.INCREMENTAL_BETWEEN.key -> s"$start,$end")
  }
}
