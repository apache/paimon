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
import org.apache.paimon.predicate.{FullTextSearch, MultiVectorSearch, MultiVectorSearchRoute, VectorSearch}
import org.apache.paimon.spark.SparkTable
import org.apache.paimon.spark.catalyst.plans.logical.PaimonTableValuedFunctions._
import org.apache.paimon.table.{DataTable, FullTextSearchTable, InnerTable, VectorSearchTable}
import org.apache.paimon.table.source.snapshot.TimeTravelUtil.InconsistentTagBucketException

import org.apache.spark.sql.PaimonUtils.createDataset
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.analysis.FunctionRegistryBase
import org.apache.spark.sql.catalyst.analysis.TableFunctionRegistry.TableFunctionBuilder
import org.apache.spark.sql.catalyst.expressions.{Attribute, CreateArray, CreateMap, CreateNamedStruct, Expression, ExpressionInfo, Literal}
import org.apache.spark.sql.catalyst.plans.logical.{LeafNode, LogicalPlan}
import org.apache.spark.sql.catalyst.util.MapData
import org.apache.spark.sql.connector.catalog.{Identifier, Table, TableCatalog}
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.unsafe.types.UTF8String

import scala.collection.JavaConverters._

object PaimonTableValuedFunctions {

  val INCREMENTAL_QUERY = "paimon_incremental_query"
  val INCREMENTAL_BETWEEN_TIMESTAMP = "paimon_incremental_between_timestamp"
  val INCREMENTAL_TO_AUTO_TAG = "paimon_incremental_to_auto_tag"
  val VECTOR_SEARCH = "vector_search"
  val MULTI_VECTOR_SEARCH = "multi_vector_search"
  val FULL_TEXT_SEARCH = "full_text_search"

  val supportedFnNames: Seq[String] =
    Seq(
      INCREMENTAL_QUERY,
      INCREMENTAL_BETWEEN_TIMESTAMP,
      INCREMENTAL_TO_AUTO_TAG,
      VECTOR_SEARCH,
      MULTI_VECTOR_SEARCH,
      FULL_TEXT_SEARCH)

  def parsePositiveLimit(value: Any): Int = {
    val limit = value match {
      case i: Int => i
      case l: Long if l <= Int.MaxValue => l.toInt
      case l: Long =>
        throw new IllegalArgumentException(
          s"Limit must be no greater than ${Int.MaxValue}, but got: $l")
      case other => throw new RuntimeException(s"Invalid limit type: ${other.getClass.getName}")
    }
    if (limit <= 0) {
      throw new IllegalArgumentException(
        s"Limit must be a positive integer, but got: $limit"
      )
    }
    limit
  }

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
      case MULTI_VECTOR_SEARCH =>
        FunctionRegistryBase.build[MultiVectorSearchQuery](fnName, since = None)
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
      case mvsq: MultiVectorSearchQuery =>
        resolveMultiVectorSearchQuery(sparkTable, sparkCatalog, ident, mvsq, args.tail)
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

  private def resolveMultiVectorSearchQuery(
      sparkTable: Table,
      sparkCatalog: TableCatalog,
      ident: Identifier,
      mvsq: MultiVectorSearchQuery,
      argsWithoutTable: Seq[Expression]): LogicalPlan = {
    sparkTable match {
      case st @ SparkTable(innerTable: InnerTable) =>
        val multiVectorSearch = mvsq.createMultiVectorSearch(innerTable, argsWithoutTable)
        val vectorSearchTable = VectorSearchTable.create(innerTable, multiVectorSearch)
        DataSourceV2Relation.create(
          st.copy(table = vectorSearchTable),
          Some(sparkCatalog),
          Some(ident),
          CaseInsensitiveStringMap.empty())
      case _ =>
        throw new RuntimeException(
          "multi_vector_search only supports Paimon SparkTable backed by InnerTable, " +
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
 * Usage: vector_search(table_name, column_name, query_vector, limit[, options])
 *   - table_name: the Paimon table to search
 *   - column_name: the vector column name
 *   - query_vector: array of floats representing the query vector
 *   - limit: the number of top results to return
 *   - options: optional options as a map or semicolon-separated key-value string
 *
 * Example: SELECT * FROM vector_search('T', 'v', array(50.0f, 51.0f, 52.0f), 5)
 */
case class VectorSearchQuery(override val args: Seq[Expression])
  extends PaimonTableValueFunction(VECTOR_SEARCH) {

  override def parseArgs(args: Seq[Expression]): Map[String, String] = {
    // This method is not used for VectorSearchQuery as we handle it specially
    Map.empty
  }

  def createVectorSearch(
      innerTable: InnerTable,
      argsWithoutTable: Seq[Expression]): VectorSearch = {
    if (argsWithoutTable.size != 3 && argsWithoutTable.size != 4) {
      throw new RuntimeException(
        s"$VECTOR_SEARCH needs three or four parameters after table_name: " +
          s"column_name, query_vector, limit[, options]. " +
          s"Got ${argsWithoutTable.size} parameters after table_name."
      )
    }
    val columnName = argsWithoutTable.head.eval().toString
    if (!innerTable.rowType().containsField(columnName)) {
      throw new RuntimeException(
        s"Column $columnName does not exist in table ${innerTable.name()}"
      )
    }
    val queryVector = extractQueryVector(argsWithoutTable(1))
    val limit = parsePositiveLimit(argsWithoutTable(2).eval())
    val options: Map[String, String] =
      if (argsWithoutTable.size == 4) {
        extractOptions(argsWithoutTable(3))
      } else {
        Map.empty[String, String]
      }
    new VectorSearch(queryVector, limit, columnName, options.asJava)
  }

  def extractQueryVector(expr: Expression): Array[Float] = {
    expr match {
      case Literal(arrayData, _) if arrayData != null =>
        val arr = arrayData.asInstanceOf[org.apache.spark.sql.catalyst.util.ArrayData]
        arr.toFloatArray()
      case CreateArray(elements, _) if elements != null =>
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

  def extractOptions(expr: Expression): Map[String, String] = {
    expr match {
      case CreateMap(children, _) if children != null =>
        children
          .grouped(2)
          .map {
            case Seq(keyExpr, valueExpr) => (extractString(keyExpr), extractString(valueExpr))
            case other =>
              throw new RuntimeException(s"Invalid options map entries: $other")
          }
          .toMap
      case _ =>
        expr.eval() match {
          case null => Map.empty
          case options: MapData => mapDataToStringMap(options)
          case options: java.util.Map[_, _] =>
            options.asScala.map {
              case (key, value) => (stringValue(key), stringValue(value))
            }.toMap
          case options: String => parseOptionsString(options)
          case options: UTF8String => parseOptionsString(options.toString)
          case other =>
            throw new RuntimeException(
              s"Invalid options type: ${other.getClass.getName}. " +
                "Expected a map or semicolon-separated key-value string.")
        }
    }
  }

  private def mapDataToStringMap(mapData: MapData): Map[String, String] = {
    val keys = mapData.keyArray().array
    val values = mapData.valueArray().array
    keys.indices.map(i => (stringValue(keys(i)), stringValue(values(i)))).toMap
  }

  private def parseOptionsString(options: String): Map[String, String] = {
    if (options == null || options.trim.isEmpty) {
      Map.empty
    } else {
      options
        .split(";")
        .map {
          kvString =>
            val kv = kvString.split("=", 2)
            if (kv.length != 2) {
              throw new IllegalArgumentException(
                s"Invalid option '$kvString'. Please use format 'key=value'.")
            }
            (kv(0).trim, kv(1).trim)
        }
        .toMap
    }
  }

  def extractString(expr: Expression): String = stringValue(expr.eval())

  private def stringValue(value: Any): String = {
    if (value == null) {
      throw new IllegalArgumentException("Option key and value cannot be null.")
    }
    value.toString
  }
}

/**
 * Plan for the [[MULTI_VECTOR_SEARCH]] table-valued function.
 *
 * Usage: multi_vector_search(table_name, routes, limit[, ranker])
 *   - table_name: the Paimon table to search
 *   - routes: route config array with vector_column, query_vector, limit, weight, and options
 *     fields
 *   - limit: the final number of ranked top results to return
 *   - ranker: optional ranker for combining results from multiple vector columns
 */
case class MultiVectorSearchQuery(override val args: Seq[Expression])
  extends PaimonTableValueFunction(MULTI_VECTOR_SEARCH) {

  override def parseArgs(args: Seq[Expression]): Map[String, String] = {
    Map.empty
  }

  def createMultiVectorSearch(
      innerTable: InnerTable,
      argsWithoutTable: Seq[Expression]): MultiVectorSearch = {
    if (argsWithoutTable.size != 2 && argsWithoutTable.size != 3) {
      throw new RuntimeException(
        s"$MULTI_VECTOR_SEARCH needs two or three parameters after table_name: " +
          s"routes, limit[, ranker]. " +
          s"Got ${argsWithoutTable.size} parameters after table_name.")
    }
    val finalLimit = parsePositiveLimit(argsWithoutTable(1).eval())
    val ranker =
      if (argsWithoutTable.size == 3) {
        VectorSearchQuery(Seq.empty).extractString(argsWithoutTable(2))
      } else {
        MultiVectorSearch.RRF_RANKER
      }

    val routes = extractRoutes(argsWithoutTable.head, finalLimit).map {
      route =>
        val columnName = route.fieldName()
        if (!innerTable.rowType().containsField(columnName)) {
          throw new RuntimeException(
            s"Column $columnName does not exist in table ${innerTable.name()}")
        }
        route
    }.toList

    new MultiVectorSearch(routes.asJava, finalLimit, ranker)
  }

  private def extractRoutes(expr: Expression, defaultLimit: Int): Seq[MultiVectorSearchRoute] = {
    expr match {
      case CreateArray(elements, _) if elements != null =>
        elements.map(extractRoute(_, defaultLimit))
      case _ =>
        throw new RuntimeException(s"Cannot extract multi-vector routes from expression: $expr")
    }
  }

  private def extractRoute(expr: Expression, defaultLimit: Int): MultiVectorSearchRoute = {
    expr match {
      case CreateNamedStruct(children) if children != null =>
        extractConfiguredRoute(children, defaultLimit)
      case _ =>
        throw new RuntimeException(s"Cannot extract multi-vector route from expression: $expr")
    }
  }

  private def extractConfiguredRoute(
      children: Seq[Expression],
      defaultLimit: Int): MultiVectorSearchRoute = {
    var columnName: Option[String] = None
    var queryVector: Option[Array[Float]] = None
    var limit: Option[Int] = None
    var weight: Option[Float] = None
    var options = Map.empty[String, String]

    children.grouped(2).foreach {
      case Seq(keyExpr, valueExpr) =>
        VectorSearchQuery(Seq.empty).extractString(keyExpr) match {
          case "vector_column" =>
            columnName = Some(VectorSearchQuery(Seq.empty).extractString(valueExpr))
          case "query_vector" =>
            queryVector = Some(VectorSearchQuery(Seq.empty).extractQueryVector(valueExpr))
          case "limit" =>
            limit = Some(parsePositiveLimit(valueExpr.eval()))
          case "weight" =>
            weight = Some(parsePositiveFloat(valueExpr.eval(), "weight"))
          case "options" =>
            options = VectorSearchQuery(Seq.empty).extractOptions(valueExpr)
          case key =>
            throw new IllegalArgumentException(
              s"Unsupported multi-vector route field '$key'. " +
                "Supported fields are vector_column, query_vector, limit, weight, and options.")
        }
      case other =>
        throw new RuntimeException(s"Invalid route config entries: $other")
    }

    val routeColumn =
      columnName.getOrElse(
        throw new IllegalArgumentException("Multi-vector route must define vector_column."))
    new MultiVectorSearchRoute(
      routeColumn,
      queryVector.getOrElse(
        throw new IllegalArgumentException(
          s"Multi-vector route for column $routeColumn must define query_vector.")),
      limit.getOrElse(defaultLimit),
      weight.getOrElse(1.0f),
      options.asJava
    )
  }

  private def parsePositiveFloat(value: Any, name: String): Float = {
    val parsed = value match {
      case f: Float => f
      case d: Double => d.toFloat
      case i: Int => i.toFloat
      case l: Long => l.toFloat
      case s: String => s.toFloat
      case u: UTF8String => u.toString.toFloat
      case other => throw new RuntimeException(s"Invalid $name type: ${other.getClass.getName}")
    }
    if (parsed <= 0) {
      throw new IllegalArgumentException(s"$name must be positive, but got: $parsed")
    }
    parsed
  }

}

/**
 * Plan for the [[FULL_TEXT_SEARCH]] table-valued function.
 *
 * Usage: full_text_search(table_name, column_name, query_text, limit)
 *   - table_name: the Paimon table to search
 *   - column_name: the text column name
 *   - query_text: the query text string
 *   - limit: the number of top results to return
 *
 * Example: SELECT * FROM full_text_search('T', 'content', 'hello world', 10)
 */
case class FullTextSearchQuery(override val args: Seq[Expression])
  extends PaimonTableValueFunction(FULL_TEXT_SEARCH) {

  override def parseArgs(args: Seq[Expression]): Map[String, String] = {
    // This method is not used for FullTextSearchQuery as we handle it specially
    Map.empty
  }

  def createFullTextSearch(
      innerTable: InnerTable,
      argsWithoutTable: Seq[Expression]): FullTextSearch = {
    if (argsWithoutTable.size != 3) {
      throw new RuntimeException(
        s"$FULL_TEXT_SEARCH needs three parameters after table_name: column_name, query_text, limit. " +
          s"Got ${argsWithoutTable.size} parameters after table_name."
      )
    }
    val columnName = argsWithoutTable.head.eval().toString
    if (!innerTable.rowType().containsField(columnName)) {
      throw new RuntimeException(
        s"Column $columnName does not exist in table ${innerTable.name()}"
      )
    }
    val queryText = argsWithoutTable(1).eval().toString
    val limit = parsePositiveLimit(argsWithoutTable(2).eval())
    new FullTextSearch(queryText, limit, columnName)
  }
}
