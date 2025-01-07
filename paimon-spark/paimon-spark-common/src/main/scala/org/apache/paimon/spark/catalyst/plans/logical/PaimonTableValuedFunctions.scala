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
import org.apache.paimon.spark.catalyst.plans.logical.PaimonTableValuedFunctions._

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.analysis.FunctionRegistryBase
import org.apache.spark.sql.catalyst.analysis.TableFunctionRegistry.TableFunctionBuilder
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, ExpressionInfo}
import org.apache.spark.sql.catalyst.plans.logical.{LeafNode, LogicalPlan}
import org.apache.spark.sql.connector.catalog.{Identifier, TableCatalog}
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import scala.collection.JavaConverters._

object PaimonTableValuedFunctions {

  val INCREMENTAL_QUERY = "paimon_incremental_query"
  val INCREMENTAL_BETWEEN_TIMESTAMP = "paimon_incremental_between_timestamp"
  val INCREMENTAL_TO_AUTO_TAG = "paimon_incremental_to_auto_tag"

  val supportedFnNames: Seq[String] =
    Seq(INCREMENTAL_QUERY, INCREMENTAL_BETWEEN_TIMESTAMP, INCREMENTAL_TO_AUTO_TAG)

  private type TableFunctionDescription = (FunctionIdentifier, ExpressionInfo, TableFunctionBuilder)

  def getTableValueFunctionInjection(fnName: String): TableFunctionDescription = {
    val (info, builder) = fnName match {
      case INCREMENTAL_QUERY =>
        FunctionRegistryBase.build[IncrementalQuery](fnName, since = None)
      case INCREMENTAL_BETWEEN_TIMESTAMP =>
        FunctionRegistryBase.build[IncrementalBetweenTimestamp](fnName, since = None)
      case INCREMENTAL_TO_AUTO_TAG =>
        FunctionRegistryBase.build[IncrementalToAutoTag](fnName, since = None)
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

    val identifier = args.head.eval().toString
    val (catalogName, dbName, tableName) = {
      sessionState.sqlParser.parseMultipartIdentifier(identifier) match {
        case Seq(table) =>
          (catalogManager.currentCatalog.name(), catalogManager.currentNamespace.head, table)
        case Seq(db, table) => (catalogManager.currentCatalog.name(), db, table)
        case Seq(catalog, db, table) => (catalog, db, table)
        case _ => throw new RuntimeException(s"Invalid table identifier: $identifier")
      }
    }

    val sparkCatalog = catalogManager.catalog(catalogName).asInstanceOf[TableCatalog]
    val ident: Identifier = Identifier.of(Array(dbName), tableName)
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

/** Plan for the [[INCREMENTAL_QUERY]] function */
case class IncrementalQuery(override val args: Seq[Expression])
  extends PaimonTableValueFunction(INCREMENTAL_QUERY) {

  override def parseArgs(args: Seq[Expression]): Map[String, String] = {
    assert(
      args.size == 2,
      s"$INCREMENTAL_QUERY needs two parameters: startSnapshotId, and endSnapshotId.")

    val start = args.head.eval().toString
    val end = args.last.eval().toString
    Map(CoreOptions.INCREMENTAL_BETWEEN.key -> s"$start,$end")
  }
}

/** Plan for the [[INCREMENTAL_BETWEEN_TIMESTAMP]] function */
case class IncrementalBetweenTimestamp(override val args: Seq[Expression])
  extends PaimonTableValueFunction(INCREMENTAL_BETWEEN_TIMESTAMP) {

  override def parseArgs(args: Seq[Expression]): Map[String, String] = {
    assert(
      args.size == 2,
      s"$INCREMENTAL_BETWEEN_TIMESTAMP needs two parameters: startTimestamp, and endTimestamp.")

    val start = args.head.eval().toString
    val end = args.last.eval().toString
    Map(CoreOptions.INCREMENTAL_BETWEEN_TIMESTAMP.key -> s"$start,$end")
  }
}

/** Plan for the [[INCREMENTAL_TO_AUTO_TAG]] function */
case class IncrementalToAutoTag(override val args: Seq[Expression])
  extends PaimonTableValueFunction(INCREMENTAL_TO_AUTO_TAG) {

  override def parseArgs(args: Seq[Expression]): Map[String, String] = {
    assert(args.size == 1, s"$INCREMENTAL_TO_AUTO_TAG needs one parameter: endTagName.")

    val endTagName = args.head.eval().toString
    Map(CoreOptions.INCREMENTAL_TO_AUTO_TAG.key -> endTagName)
  }
}
