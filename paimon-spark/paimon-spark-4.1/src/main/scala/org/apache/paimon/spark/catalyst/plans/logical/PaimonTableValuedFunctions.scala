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
import org.apache.paimon.predicate.{FullTextSearch, VectorSearch}
import org.apache.paimon.spark.SparkTable
import org.apache.paimon.spark.catalyst.plans.logical.PaimonTableValuedFunctions._
import org.apache.paimon.table.{DataTable, FullTextSearchTable, InnerTable, VectorSearchTable}
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
  val FULL_TEXT_SEARCH = "full_text_search"

  val supportedFnNames: Seq[String] =
    Seq(
      INCREMENTAL_QUERY,
      INCREMENTAL_BETWEEN_TIMESTAMP,
      INCREMENTAL_TO_AUTO_TAG,
      VECTOR_SEARCH,
      FULL_TEXT_SEARCH)

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
      case FULL_TEXT_SEARCH =>
        FunctionRegistryBase.build[FullTextSearchQuery](fnName, since = None)
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

    // Handle vector_search and full_text_search specially
    tvf match {
      case vsq: VectorSearchQuery =>
        resolveVectorSearchQuery(sparkTable, sparkCatalog, ident, vsq, args.tail)
      case ftsq: FullTextSearchQuery =>
        resolveFullTextSearchQuery(sparkTable, sparkCatalog, ident, ftsq, args.tail)
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
        val vectorSearch = vsq.createVectorSearch(innerTable, argsWithoutTable)
        val vectorSearchTable = VectorSearchTable.create(innerTable, vectorSearch)
        DataSourceV2Relation.create(
          st.copy(table = vectorSearchTable),
          Some(sparkCatalog),
          Some(ident),
          CaseInsensitiveStringMap.empty())
      case _ =>
        throw new RuntimeException(
          "vector_search only supports Paimon SparkTable backed by InnerTable, " +
            s"but got table implementation: ${sparkTable.getClass.getName}")
    }
  }

  private def resolveFullTextSearchQuery(
      sparkTable: Table,
      sparkCatalog: TableCatalog,
      ident: Identifier,
      ftsq: FullTextSearchQuery,
      argsWithoutTable: Seq[Expression]): LogicalPlan = {
    sparkTable match {
      case st @ SparkTable(innerTable: InnerTable) =>
        val fullTextSearch = ftsq.createFullTextSearch(innerTable, argsWithoutTable)
        val fullTextSearchTable = FullTextSearchTable.create(innerTable, fullTextSearch)
        DataSourceV2Relation.create(
          st.copy(table = fullTextSearchTable),
          Some(sparkCatalog),
          Some(ident),
          CaseInsensitiveStringMap.empty())
      case _ =>
        throw new RuntimeException(
          "full_text_search only supports Paimon SparkTable backed by InnerTable, " +
            s"but got table implementation: ${sparkTable.getClass.getName}")
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
