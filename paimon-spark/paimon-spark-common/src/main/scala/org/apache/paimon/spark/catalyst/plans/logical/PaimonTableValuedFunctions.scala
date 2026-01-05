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
import org.apache.paimon.predicate.VectorSearch
import org.apache.paimon.spark.SparkTable
import org.apache.paimon.spark.catalyst.plans.logical.PaimonTableValuedFunctions._
import org.apache.paimon.table.{DataTable, InnerTable, VectorSearchTable}
import org.apache.paimon.table.source.snapshot.TimeTravelUtil.InconsistentTagBucketException

import org.apache.spark.sql.PaimonUtils.createDataset
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.analysis.FunctionRegistryBase
import org.apache.spark.sql.catalyst.analysis.TableFunctionRegistry.TableFunctionBuilder
import org.apache.spark.sql.catalyst.expressions.{Attribute, CreateArray, Expression, ExpressionInfo, Literal}
import org.apache.spark.sql.catalyst.plans.logical.{LeafNode, LogicalPlan}
import org.apache.spark.sql.connector.catalog.{Identifier, Table, TableCatalog}
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import scala.collection.JavaConverters._

object PaimonTableValuedFunctions {

  val INCREMENTAL_QUERY = "paimon_incremental_query"
  val INCREMENTAL_BETWEEN_TIMESTAMP = "paimon_incremental_between_timestamp"
  val INCREMENTAL_TO_AUTO_TAG = "paimon_incremental_to_auto_tag"
  val VECTOR_SEARCH = "vector_search"

  val supportedFnNames: Seq[String] =
    Seq(INCREMENTAL_QUERY, INCREMENTAL_BETWEEN_TIMESTAMP, INCREMENTAL_TO_AUTO_TAG, VECTOR_SEARCH)

  private type TableFunctionDescription = (FunctionIdentifier, ExpressionInfo, TableFunctionBuilder)

  def getTableValueFunctionInjection(fnName: String): TableFunctionDescription = {
    val (info, builder) = fnName match {
      case INCREMENTAL_QUERY =>
        FunctionRegistryBase.build[IncrementalQuery](fnName, since = None)
      case INCREMENTAL_BETWEEN_TIMESTAMP =>
        FunctionRegistryBase.build[IncrementalBetweenTimestamp](fnName, since = None)
      case INCREMENTAL_TO_AUTO_TAG =>
        FunctionRegistryBase.build[IncrementalToAutoTag](fnName, since = None)
      case VECTOR_SEARCH =>
        FunctionRegistryBase.build[VectorSearchQuery](fnName, since = None)
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

    // Handle vector_search specially
    tvf match {
      case vsq: VectorSearchQuery =>
        resolveVectorSearchQuery(sparkTable, sparkCatalog, ident, vsq, args.tail)
      case _ =>
        val options = tvf.parseArgs(args.tail)
        usingSparkIncrementQuery(tvf, sparkTable, options) match {
          case Some(snapshotIdPair: (Long, Long)) =>
            sparkIncrementQuery(spark, sparkTable, sparkCatalog, ident, options, snapshotIdPair)
          case _ =>
            DataSourceV2Relation.create(
              sparkTable,
              Some(sparkCatalog),
              Some(ident),
              new CaseInsensitiveStringMap(options.asJava))
        }
    }
  }

  private def resolveVectorSearchQuery(
      sparkTable: Table,
      sparkCatalog: TableCatalog,
      ident: Identifier,
      vsq: VectorSearchQuery,
      argsWithoutTable: Seq[Expression]): LogicalPlan = {
    sparkTable match {
      case st @ SparkTable(innerTable: InnerTable) =>
        val vectorSearch = vsq.createVectorSearch(argsWithoutTable)
        val vectorSearchTable = VectorSearchTable.create(innerTable, vectorSearch)
        DataSourceV2Relation.create(
          st.copy(table = vectorSearchTable),
          Some(sparkCatalog),
          Some(ident),
          CaseInsensitiveStringMap.empty())
      case _ =>
        throw new RuntimeException(
          s"vector_search only supports Paimon tables, got ${sparkTable.getClass.getName}")
    }
  }

  private def usingSparkIncrementQuery(
      tvf: PaimonTableValueFunction,
      sparkTable: Table,
      options: Map[String, String]): Option[(Long, Long)] = {
    tvf.fnName match {
      case INCREMENTAL_QUERY | INCREMENTAL_TO_AUTO_TAG =>
        sparkTable match {
          case SparkTable(fileStoreTable: DataTable) =>
            try {
              fileStoreTable.copy(options.asJava).asInstanceOf[DataTable].newScan().plan()
              None
            } catch {
              case e: InconsistentTagBucketException =>
                Some((e.startSnapshotId, e.endSnapshotId))
            }
          case _ => None
        }
      case _ => None
    }
  }

  private def sparkIncrementQuery(
      spark: SparkSession,
      sparkTable: Table,
      sparkCatalog: TableCatalog,
      ident: Identifier,
      options: Map[String, String],
      snapshotIdPair: (Long, Long)): LogicalPlan = {
    val filteredOptions =
      options - CoreOptions.INCREMENTAL_BETWEEN.key - CoreOptions.INCREMENTAL_TO_AUTO_TAG.key

    def datasetOfSnapshot(snapshotId: Long) = {
      val updatedOptions = filteredOptions + (CoreOptions.SCAN_VERSION.key() -> snapshotId.toString)
      createDataset(
        spark,
        DataSourceV2Relation.create(
          sparkTable,
          Some(sparkCatalog),
          Some(ident),
          new CaseInsensitiveStringMap(updatedOptions.asJava)
        ))
    }

    datasetOfSnapshot(snapshotIdPair._2)
      .except(datasetOfSnapshot(snapshotIdPair._1))
      .queryExecution
      .logical
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

/**
 * Plan for the [[VECTOR_SEARCH]] table-valued function.
 *
 * Usage: vector_search(table_name, column_name, query_vector, limit)
 *   - table_name: the Paimon table to search
 *   - column_name: the vector column name
 *   - query_vector: array of floats representing the query vector
 *   - limit: the number of top results to return
 *
 * Example: SELECT * FROM vector_search('T', 'v', array(50.0f, 51.0f, 52.0f), 5)
 */
case class VectorSearchQuery(override val args: Seq[Expression])
  extends PaimonTableValueFunction(VECTOR_SEARCH) {

  override def parseArgs(args: Seq[Expression]): Map[String, String] = {
    // This method is not used for VectorSearchQuery as we handle it specially
    Map.empty
  }

  def createVectorSearch(argsWithoutTable: Seq[Expression]): VectorSearch = {
    assert(
      argsWithoutTable.size == 3,
      s"$VECTOR_SEARCH needs four parameters: table_name, column_name, query_vector, limit. " +
        s"Got ${argsWithoutTable.size + 1} parameters."
    )

    val columnName = argsWithoutTable.head.eval().toString
    val queryVector = extractQueryVector(argsWithoutTable(1))
    val limit = argsWithoutTable(2).eval() match {
      case i: Int => i
      case l: Long => l.toInt
      case other => throw new RuntimeException(s"Invalid limit type: ${other.getClass.getName}")
    }

    new VectorSearch(queryVector, limit, columnName)
  }

  private def extractQueryVector(expr: Expression): Array[Float] = {
    expr match {
      case Literal(arrayData, _) if arrayData != null =>
        val arr = arrayData.asInstanceOf[org.apache.spark.sql.catalyst.util.ArrayData]
        arr.toFloatArray()
      case CreateArray(elements, _) =>
        elements.map {
          case Literal(v: Float, _) => v
          case Literal(v: Double, _) => v.toFloat
          case Literal(v: java.lang.Float, _) if v != null => v.floatValue()
          case Literal(v: java.lang.Double, _) if v != null => v.floatValue()
          case other => throw new RuntimeException(s"Cannot extract float from: $other")
        }.toArray
      case _ =>
        throw new RuntimeException(s"Cannot extract query vector from expression: $expr")
    }
  }
}
