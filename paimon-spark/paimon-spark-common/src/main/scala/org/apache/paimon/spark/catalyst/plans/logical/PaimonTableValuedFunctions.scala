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
import org.apache.paimon.globalindex.HybridSearchRanker
import org.apache.paimon.predicate.{FullTextQuery, FullTextSearch, HybridSearch, HybridSearchRoute, Predicate, VectorSearch}
import org.apache.paimon.spark.{SparkTable, SparkTypeUtils, SparkV2FilterConverter}
import org.apache.paimon.spark.catalyst.plans.logical.PaimonTableValuedFunctions._
import org.apache.paimon.spark.schema.PaimonMetadataColumn
import org.apache.paimon.table.{DataTable, FullTextSearchTable, HybridSearchTable, InnerTable, VectorSearchTable}
import org.apache.paimon.table.source.snapshot.TimeTravelUtil.InconsistentTagBucketException

import org.apache.spark.sql.PaimonUtils.{createDataset, normalizeExprs, toAttributes, translateFilterV2}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.analysis.FunctionRegistryBase
import org.apache.spark.sql.catalyst.analysis.TableFunctionRegistry.TableFunctionBuilder
import org.apache.spark.sql.catalyst.expressions.{Alias, And, Attribute, AttributeSet, CreateArray, CreateMap, CreateNamedStruct, Expression, ExpressionInfo, ExprId, Literal, OuterReference}
import org.apache.spark.sql.catalyst.plans.{Inner, JoinType}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LeafNode, LogicalPlan, Project, SubqueryAlias, UnaryNode}
import org.apache.spark.sql.catalyst.util.MapData
import org.apache.spark.sql.connector.catalog.{Identifier, Table, TableCatalog}
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.unsafe.types.UTF8String

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.util.control.NonFatal

object PaimonTableValuedFunctions {

  private case class DynamicVectorSearchExtraction(
      relation: DynamicVectorSearchRelation,
      projectList: Seq[Expression],
      projectOutput: Seq[Attribute],
      searchFilters: Seq[Expression])

  val INCREMENTAL_QUERY = "paimon_incremental_query"
  val INCREMENTAL_BETWEEN_TIMESTAMP = "paimon_incremental_between_timestamp"
  val INCREMENTAL_TO_AUTO_TAG = "paimon_incremental_to_auto_tag"
  val VECTOR_SEARCH = "vector_search"
  val HYBRID_SEARCH = "hybrid_search"
  val FULL_TEXT_SEARCH = "full_text_search"

  val supportedFnNames: Seq[String] =
    Seq(
      INCREMENTAL_QUERY,
      INCREMENTAL_BETWEEN_TIMESTAMP,
      INCREMENTAL_TO_AUTO_TAG,
      VECTOR_SEARCH,
      HYBRID_SEARCH,
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
      case HYBRID_SEARCH =>
        FunctionRegistryBase.build[HybridSearchQuery](fnName, since = None)
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

    // Handle search table-valued functions specially.
    tvf match {
      case vsq: VectorSearchQuery =>
        resolveVectorSearchQuery(sparkTable, sparkCatalog, ident, vsq, args.tail)
      case hsq: HybridSearchQuery =>
        resolveHybridSearchQuery(sparkTable, sparkCatalog, ident, hsq, args.tail)
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
        if (vsq.hasOuterReference(argsWithoutTable)) {
          return vsq.createDynamicVectorSearch(innerTable, argsWithoutTable)
        }
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

  private def resolveHybridSearchQuery(
      sparkTable: Table,
      sparkCatalog: TableCatalog,
      ident: Identifier,
      hsq: HybridSearchQuery,
      argsWithoutTable: Seq[Expression]): LogicalPlan = {
    sparkTable match {
      case st @ SparkTable(innerTable: InnerTable) =>
        val hybridSearch = hsq.createHybridSearch(innerTable, argsWithoutTable)
        val hybridSearchTable = HybridSearchTable.create(innerTable, hybridSearch)
        DataSourceV2Relation.create(
          st.copy(table = hybridSearchTable),
          Some(sparkCatalog),
          Some(ident),
          CaseInsensitiveStringMap.empty())
      case _ =>
        throw new RuntimeException(
          "hybrid_search only supports Paimon SparkTable backed by InnerTable, " +
            s"but got table implementation: ${sparkTable.getClass.getName}")
    }
  }

  def resolveLateralVectorSearch(
      left: LogicalPlan,
      right: LogicalPlan,
      joinType: JoinType,
      condition: Option[Expression]): Option[LogicalPlan] = {
    extractDynamicVectorSearch(right) match {
      case None if containsDynamicVectorSearch(right) =>
        throw new UnsupportedOperationException(
          "LATERAL vector_search only supports SELECT <columns> FROM vector_search(...) " +
            "or SELECT <columns> FROM vector_search(...) WHERE <searched-table predicate>.")
      case None =>
        None
      case Some(_) if joinType != Inner =>
        throw new RuntimeException(
          s"LATERAL vector_search only supports INNER join, but got: $joinType.")
      case Some(
            DynamicVectorSearchExtraction(relation, projectList, projectOutput, searchFilters)) =>
        val vectorSearchOutput = vectorSearchOutputForProject(relation, projectList, searchFilters)
        if (
          searchFilters.nonEmpty && convertLateralVectorSearchFilters(
            relation.innerTable,
            vectorSearchOutput,
            projectList,
            projectOutput,
            searchFilters).isEmpty
        ) {
          throw new UnsupportedOperationException(
            "LATERAL vector_search only supports deterministic subquery predicates " +
              "convertible to Paimon predicates on searched-table columns.")
        }
        val stripOuterRef: Expression => Expression =
          _.transform { case OuterReference(a) => a.toAttribute }
        val lateralVectorSearch =
          LateralVectorSearch(
            left,
            relation.innerTable,
            relation.columnName,
            stripOuterRef(relation.queryVectorExpr),
            relation.limit,
            relation.options,
            vectorSearchOutput,
            projectList.map(stripOuterRef),
            projectOutput,
            searchFilters
          )
        Some(condition.map(Filter(_, lateralVectorSearch)).getOrElse(lateralVectorSearch))
    }
  }

  def convertLateralVectorSearchFilters(
      innerTable: InnerTable,
      vectorSearchOutput: Seq[Attribute],
      projectList: Seq[Expression],
      projectOutput: Seq[Attribute],
      filters: Seq[Expression]): Option[Seq[Predicate]] = {
    val converter = SparkV2FilterConverter(innerTable.rowType())
    val projectionByExprId = projectList
      .zip(projectOutput)
      .map { case (project, outputAttr) => outputAttr.exprId -> stripAlias(project) }
      .toMap
    try {
      val predicates = ArrayBuffer[Predicate]()
      normalizeExprs(filters.map(rewriteSearchFilter(_, projectionByExprId)), vectorSearchOutput)
        .flatMap(splitConjunctivePredicatesForFilter)
        .foreach {
          filter =>
            translateFilterV2(filter).flatMap(converter.convert(_)) match {
              case Some(predicate) => predicates += predicate
              case None => return None
            }
        }
      Some(predicates.toSeq)
    } catch {
      case NonFatal(_) => None
    }
  }

  private def rewriteSearchFilter(
      filter: Expression,
      projectionByExprId: Map[ExprId, Expression]): Expression = {
    filter.transform { case attr: Attribute => projectionByExprId.getOrElse(attr.exprId, attr) }
  }

  private def stripAlias(expression: Expression): Expression = {
    expression match {
      case Alias(child, _) => child
      case other => other
    }
  }

  private def splitConjunctivePredicatesForFilter(condition: Expression): Seq[Expression] = {
    condition match {
      case And(left, right) =>
        splitConjunctivePredicatesForFilter(left) ++ splitConjunctivePredicatesForFilter(right)
      case other => other :: Nil
    }
  }

  private def vectorSearchOutputForProject(
      relation: DynamicVectorSearchRelation,
      projectList: Seq[Expression],
      searchFilters: Seq[Expression]): Seq[Attribute] = {
    val projectReferences =
      AttributeSet.fromAttributeSets((projectList ++ searchFilters).map(_.references))
    relation.output.filter(projectReferences.contains)
  }

  private def extractDynamicVectorSearch(
      plan: LogicalPlan): Option[DynamicVectorSearchExtraction] = {
    plan match {
      case SubqueryAlias(_, child) =>
        extractDynamicVectorSearch(child).map {
          extraction => extraction.copy(projectOutput = plan.output)
        }
      case Project(projectList, Filter(condition, relation: DynamicVectorSearchRelation))
          if projectList.forall(_.resolved) && condition.resolved =>
        Some(DynamicVectorSearchExtraction(relation, projectList, plan.output, Seq(condition)))
      case Project(projectList, relation: DynamicVectorSearchRelation)
          if projectList.forall(_.resolved) =>
        Some(DynamicVectorSearchExtraction(relation, projectList, plan.output, Nil))
      case Filter(condition, relation: DynamicVectorSearchRelation) if condition.resolved =>
        Some(
          DynamicVectorSearchExtraction(relation, relation.output, relation.output, Seq(condition)))
      case relation: DynamicVectorSearchRelation =>
        Some(DynamicVectorSearchExtraction(relation, relation.output, relation.output, Nil))
      case _ => None
    }
  }

  def containsDynamicVectorSearch(plan: LogicalPlan): Boolean = {
    plan.isInstanceOf[DynamicVectorSearchRelation] || plan.children.exists(
      containsDynamicVectorSearch)
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

  def hasOuterReference(argsWithoutTable: Seq[Expression]): Boolean = {
    val queryVector = argsWithoutTable(1)
    (argsWithoutTable.size == 3 || argsWithoutTable.size == 4) &&
    (queryVector.references.nonEmpty || containsOuterReference(queryVector))
  }

  private def containsOuterReference(expr: Expression): Boolean = {
    expr.isInstanceOf[OuterReference] || expr.children.exists(containsOuterReference)
  }

  def createDynamicVectorSearch(
      innerTable: InnerTable,
      argsWithoutTable: Seq[Expression]): DynamicVectorSearchRelation = {
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
    val limit = parsePositiveLimit(argsWithoutTable(2).eval())
    val options: Map[String, String] =
      if (argsWithoutTable.size == 4) {
        extractOptions(argsWithoutTable(3))
      } else {
        Map.empty[String, String]
      }
    DynamicVectorSearchRelation(
      innerTable,
      columnName,
      argsWithoutTable(1),
      limit,
      options,
      toAttributes(SparkTypeUtils.fromPaimonRowType(innerTable.rowType())))
  }
}

/**
 * Plan for the [[HYBRID_SEARCH]] table-valued function.
 *
 * Usage: hybrid_search(table_name, vector_routes, full_text_routes, limit[, ranker])
 *   - table_name: the Paimon table to search
 *   - vector_routes: route config array with field, query_vector, limit, weight, and options fields
 *   - full_text_routes: route config array with query, limit, weight, and empty options fields
 *   - limit: the final number of ranked top results to return
 *   - ranker: optional ranker for combining results from multiple routes: rrf, weighted_score, or
 *     mrr
 */
case class HybridSearchQuery(override val args: Seq[Expression])
  extends PaimonTableValueFunction(HYBRID_SEARCH) {

  override def parseArgs(args: Seq[Expression]): Map[String, String] = {
    Map.empty
  }

  def createHybridSearch(
      innerTable: InnerTable,
      argsWithoutTable: Seq[Expression]): HybridSearch = {
    if (argsWithoutTable.size != 3 && argsWithoutTable.size != 4) {
      throw new RuntimeException(
        s"$HYBRID_SEARCH needs three or four parameters after table_name: " +
          s"vector_routes, full_text_routes, limit[, ranker]. " +
          s"Got ${argsWithoutTable.size} parameters after table_name.")
    }
    val finalLimit = parsePositiveLimit(argsWithoutTable(2).eval())
    val ranker =
      if (argsWithoutTable.size == 4) {
        VectorSearchQuery(Seq.empty).extractString(argsWithoutTable(3))
      } else {
        HybridSearchRanker.RRF_RANKER
      }

    val routes =
      (extractVectorRoutes(argsWithoutTable.head, finalLimit) ++
        extractFullTextRoutes(argsWithoutTable(1), finalLimit)).map {
        route =>
          route.columns().asScala.foreach {
            columnName =>
              if (!innerTable.rowType().containsField(columnName)) {
                throw new RuntimeException(
                  s"Column $columnName does not exist in table ${innerTable.name()}")
              }
          }
          route
      }.toList

    new HybridSearch(routes.asJava, finalLimit, ranker)
  }

  private def extractVectorRoutes(expr: Expression, defaultLimit: Int): Seq[HybridSearchRoute] = {
    expr match {
      case CreateArray(elements, _) if elements != null =>
        elements.map(extractVectorRoute(_, defaultLimit))
      case _ =>
        throw new RuntimeException(s"Cannot extract vector routes from expression: $expr")
    }
  }

  private def extractFullTextRoutes(expr: Expression, defaultLimit: Int): Seq[HybridSearchRoute] = {
    expr match {
      case CreateArray(elements, _) if elements != null =>
        elements.map(extractFullTextRoute(_, defaultLimit))
      case _ =>
        throw new RuntimeException(s"Cannot extract full-text routes from expression: $expr")
    }
  }

  private def extractVectorRoute(expr: Expression, defaultLimit: Int): HybridSearchRoute = {
    expr match {
      case CreateNamedStruct(children) if children != null =>
        extractConfiguredVectorRoute(children, defaultLimit)
      case _ =>
        throw new RuntimeException(s"Cannot extract vector route from expression: $expr")
    }
  }

  private def extractFullTextRoute(expr: Expression, defaultLimit: Int): HybridSearchRoute = {
    expr match {
      case CreateNamedStruct(children) if children != null =>
        extractConfiguredFullTextRoute(children, defaultLimit)
      case _ =>
        throw new RuntimeException(s"Cannot extract full-text route from expression: $expr")
    }
  }

  private def extractConfiguredVectorRoute(
      children: Seq[Expression],
      defaultLimit: Int): HybridSearchRoute = {
    var columnName: Option[String] = None
    var queryVector: Option[Array[Float]] = None
    var limit: Option[Int] = None
    var weight: Option[Float] = None
    var options = Map.empty[String, String]

    children.grouped(2).foreach {
      case Seq(keyExpr, valueExpr) =>
        VectorSearchQuery(Seq.empty).extractString(keyExpr) match {
          case "field" | "vector_column" =>
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
            throw new IllegalArgumentException(s"Unsupported vector route field '$key'. " +
              "Supported fields are field, vector_column, query_vector, limit, weight, and options.")
        }
      case other =>
        throw new RuntimeException(s"Invalid route config entries: $other")
    }

    val routeColumn =
      columnName.getOrElse(
        throw new IllegalArgumentException("Vector route must define field or vector_column."))
    HybridSearchRoute.vector(
      routeColumn,
      queryVector.getOrElse(
        throw new IllegalArgumentException(
          s"Vector route for column $routeColumn must define query_vector.")),
      limit.getOrElse(defaultLimit),
      weight.getOrElse(1.0f),
      options.asJava
    )
  }

  private def extractConfiguredFullTextRoute(
      children: Seq[Expression],
      defaultLimit: Int): HybridSearchRoute = {
    var query: Option[String] = None
    var limit: Option[Int] = None
    var weight: Option[Float] = None
    var options = Map.empty[String, String]

    children.grouped(2).foreach {
      case Seq(keyExpr, valueExpr) =>
        VectorSearchQuery(Seq.empty).extractString(keyExpr) match {
          case "query" =>
            query = Some(VectorSearchQuery(Seq.empty).extractString(valueExpr))
          case "limit" =>
            limit = Some(parsePositiveLimit(valueExpr.eval()))
          case "weight" =>
            weight = Some(parsePositiveFloat(valueExpr.eval(), "weight"))
          case "options" =>
            options = VectorSearchQuery(Seq.empty).extractOptions(valueExpr)
          case key =>
            throw new IllegalArgumentException(
              s"Unsupported full-text route field '$key'. " +
                "Supported fields are query, limit, weight, and options.")
        }
      case other =>
        throw new RuntimeException(s"Invalid route config entries: $other")
    }

    HybridSearchRoute.fullText(
      query.getOrElse(throw new IllegalArgumentException("Full-text route must define query.")),
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
    if (!java.lang.Float.isFinite(parsed) || parsed <= 0) {
      if (name == "weight") {
        throw new IllegalArgumentException(s"Weight must be finite and positive, got: $parsed")
      }
      throw new IllegalArgumentException(s"$name must be finite and positive, but got: $parsed")
    }
    parsed
  }

}

case class DynamicVectorSearchRelation(
    innerTable: InnerTable,
    columnName: String,
    queryVectorExpr: Expression,
    limit: Int,
    options: Map[String, String],
    relationOutput: Seq[Attribute])
  extends LeafNode {

  private lazy val outputWithScore: Seq[Attribute] =
    relationOutput ++
      Seq(PaimonMetadataColumn.SEARCH_SCORE.toAttribute)

  override def output: Seq[Attribute] = outputWithScore
}

case class LateralVectorSearch(
    left: LogicalPlan,
    innerTable: InnerTable,
    columnName: String,
    queryVectorExpr: Expression,
    limit: Int,
    options: Map[String, String],
    vectorSearchOutput: Seq[Attribute],
    projectList: Seq[Expression],
    projectOutput: Seq[Attribute],
    searchFilters: Seq[Expression] = Nil)
  extends UnaryNode {

  override def child: LogicalPlan = left

  override def output: Seq[Attribute] = left.output ++ projectOutput

  lazy val searchFilterOutputSet: AttributeSet = {
    val tableOutputSet = AttributeSet(
      vectorSearchOutput.filterNot(_.name == PaimonMetadataColumn.SEARCH_SCORE_COLUMN))
    AttributeSet(projectList.zip(projectOutput).collect {
      case (expr, attr) if isSearchFilterAttribute(expr, tableOutputSet) =>
        attr
    })
  }

  private def isSearchFilterAttribute(
      expression: Expression,
      tableOutputSet: AttributeSet): Boolean = {
    expression match {
      case Alias(attr: Attribute, _) => tableOutputSet.contains(attr)
      case attr: Attribute => tableOutputSet.contains(attr)
      case _ => false
    }
  }

  override lazy val producedAttributes: AttributeSet = {
    AttributeSet(vectorSearchOutput ++ output.filterNot(attr => inputSet.contains(attr)))
  }

  override lazy val references: AttributeSet = {
    AttributeSet.fromAttributeSets(expressions.map(_.references)) -- producedAttributes
  }

  override protected def withNewChildInternal(newChild: LogicalPlan): LogicalPlan = {
    copy(left = newChild)
  }
}

/**
 * Plan for the [[FULL_TEXT_SEARCH]] table-valued function.
 *
 * Usage: full_text_search(table_name, query_json, limit)
 *   - table_name: the Paimon table to search
 *   - query_json: the LanceDB-style full-text query JSON string
 *   - limit: the number of top results to return
 *
 * Example: SELECT * FROM full_text_search('T', '{"match":{"column":"content","terms":"hello"}}',
 * 10)
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
    if (argsWithoutTable.size != 2) {
      throw new RuntimeException(
        s"$FULL_TEXT_SEARCH needs two parameters after table_name: " +
          s"query_json, limit. " +
          s"Got ${argsWithoutTable.size} parameters after table_name."
      )
    }
    val query = FullTextQuery.fromJson(argsWithoutTable.head.eval().toString)
    query.columns().asScala.foreach {
      columnName =>
        if (!innerTable.rowType().containsField(columnName)) {
          throw new RuntimeException(
            s"Column $columnName does not exist in table ${innerTable.name()}"
          )
        }
    }
    val limit = parsePositiveLimit(argsWithoutTable(1).eval())
    new FullTextSearch(query, limit)
  }
}
