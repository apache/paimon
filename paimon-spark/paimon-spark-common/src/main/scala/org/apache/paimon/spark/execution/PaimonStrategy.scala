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

package org.apache.paimon.spark.execution

import org.apache.paimon.CoreOptions
import org.apache.paimon.globalindex.{GlobalIndexResult, ScoredGlobalIndexResult}
import org.apache.paimon.partition.PartitionPredicate
import org.apache.paimon.partition.PartitionPredicate.splitPartitionPredicatesAndDataPredicates
import org.apache.paimon.predicate.{Predicate, PredicateBuilder}
import org.apache.paimon.spark.{PaimonRecordReaderIterator, SparkCatalog, SparkGenericCatalog, SparkTable, SparkUtils}
import org.apache.paimon.spark.catalog.{SparkBaseCatalog, SupportView}
import org.apache.paimon.spark.catalyst.analysis.ResolvedPaimonView
import org.apache.paimon.spark.catalyst.plans.logical.{CopyIntoLocationCommand, CopyIntoLocationSource, CopyIntoTableCommand, CreateOrReplaceTagCommand, CreatePaimonView, DeleteTagCommand, DropPaimonView, LateralVectorSearch, PaimonCallCommand, PaimonDropPartitions, PaimonTableValuedFunctions, RenameTagCommand, ResolvedIdentifier, ShowPaimonViews, ShowTagsCommand, TruncatePaimonTableWithFilter}
import org.apache.paimon.spark.data.SparkInternalRow
import org.apache.paimon.spark.schema.PaimonMetadataColumn
import org.apache.paimon.table.{InnerTable, SpecialFields, Table}
import org.apache.paimon.table.source.{BatchVectorSearchBuilder, InnerTableScan, ReadBuilder, VectorScan}
import org.apache.paimon.types.RowType
import org.apache.paimon.utils.RoaringNavigableMap64

import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.{ResolvedNamespace, ResolvedTable}
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeSet, Expression, GenericInternalRow, JoinedRow, PredicateHelper, UnsafeProjection}
import org.apache.spark.sql.catalyst.plans.logical.{CreateTableAsSelect, DescribeRelation, LogicalPlan, ReplaceTable, ReplaceTableAsSelect, ShowCreateTable}
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.connector.catalog.{Identifier, PaimonLookupCatalog, TableCatalog}
import org.apache.spark.sql.execution.{PaimonDescribeTableExec, SparkPlan, SparkStrategy}
import org.apache.spark.sql.execution.datasources.v2.{DataSourceV2Implicits, DataSourceV2Relation}
import org.apache.spark.sql.execution.shim.{PaimonCreateTableAsSelectStrategy, PaimonReplaceTableAsSelectStrategy, PaimonReplaceTableStrategy}
import org.apache.spark.sql.paimon.shims.SparkShimLoader

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

case class PaimonStrategy(spark: SparkSession)
  extends SparkStrategy
  with PredicateHelper
  with PaimonLookupCatalog {

  import DataSourceV2Implicits._
  protected lazy val catalogManager = spark.sessionState.catalogManager

  override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {

    case ctas: CreateTableAsSelect =>
      PaimonCreateTableAsSelectStrategy(spark)(ctas)

    case rtas: ReplaceTableAsSelect =>
      PaimonReplaceTableAsSelectStrategy(spark)(rtas)

    case rt: ReplaceTable =>
      PaimonReplaceTableStrategy(spark)(rt)

    case c @ PaimonCallCommand(procedure, args) =>
      val input = buildInternalRow(args)
      PaimonCallExec(c.output, procedure, input) :: Nil

    case lvs: LateralVectorSearch =>
      LateralVectorSearchExec(
        lvs.innerTable,
        lvs.columnName,
        lvs.queryVectorExpr,
        lvs.limit,
        lvs.options,
        lvs.vectorSearchOutput,
        lvs.projectList,
        lvs.projectOutput,
        lvs.searchFilters,
        planLater(lvs.left)
      ) :: Nil

    case t @ ShowTagsCommand(PaimonCatalogAndIdentifier(catalog, ident)) =>
      ShowTagsExec(catalog, ident, t.output) :: Nil

    case CreateOrReplaceTagCommand(
          PaimonCatalogAndIdentifier(table, ident),
          tagName,
          tagOptions,
          create,
          replace,
          ifNotExists) =>
      CreateOrReplaceTagExec(table, ident, tagName, tagOptions, create, replace, ifNotExists) :: Nil

    case DeleteTagCommand(PaimonCatalogAndIdentifier(catalog, ident), tagStr, ifExists) =>
      DeleteTagExec(catalog, ident, tagStr, ifExists) :: Nil

    case RenameTagCommand(PaimonCatalogAndIdentifier(catalog, ident), sourceTag, targetTag) =>
      RenameTagExec(catalog, ident, sourceTag, targetTag) :: Nil

    case CreatePaimonView(
          ResolvedIdentifier(viewCatalog: SupportView, ident),
          queryText,
          query,
          columnAliases,
          columnComments,
          queryColumnNames,
          comment,
          properties,
          allowExisting,
          replace
        ) =>
      CreatePaimonViewExec(
        viewCatalog,
        ident,
        queryText,
        query.schema,
        columnAliases,
        columnComments,
        queryColumnNames,
        comment,
        properties,
        allowExisting,
        replace) :: Nil

    case DropPaimonView(ResolvedIdentifier(viewCatalog: SupportView, ident), ifExists) =>
      DropPaimonViewExec(viewCatalog, ident, ifExists) :: Nil

    // A new member was added to ResolvedNamespace since spark4.0,
    // unapply pattern matching is not used here to ensure compatibility across multiple spark versions.
    case ShowPaimonViews(r: ResolvedNamespace, pattern, output)
        if r.catalog.isInstanceOf[SupportView] =>
      ShowPaimonViewsExec(output, r.catalog.asInstanceOf[SupportView], r.namespace, pattern) :: Nil

    case ShowCreateTable(ResolvedPaimonView(viewCatalog, ident), _, output) =>
      ShowCreatePaimonViewExec(output, viewCatalog, ident) :: Nil

    case DescribeRelation(ResolvedPaimonView(viewCatalog, ident), _, isExtended, output) =>
      DescribePaimonViewExec(output, viewCatalog, ident, isExtended) :: Nil

    case DescribeRelation(r: ResolvedTable, partitionSpec, isExtended, output) =>
      (r.table, r.catalog) match {
        case (sparkTable: SparkTable, sparkCatalog: SparkBaseCatalog) =>
          PaimonDescribeTableExec(
            output,
            sparkCatalog,
            r.identifier,
            sparkTable,
            partitionSpec,
            isExtended) :: Nil
        case _ => Nil
      }

    case PaimonDropPartitions(
          r @ ResolvedTable(_, _, table: SparkTable, _),
          parts,
          ifExists,
          purge) =>
      PaimonDropPartitionsExec(
        table,
        parts.asResolvedPartitionSpecs,
        ifExists,
        purge,
        recacheTable(r)) :: Nil

    case TruncatePaimonTableWithFilter(
          table: Table,
          partitionPredicate: Option[PartitionPredicate]) =>
      TruncatePaimonTableWithFilterExec(table, partitionPredicate) :: Nil

    case c @ CopyIntoTableCommand(PaimonCatalogAndIdentifier(catalog, ident), _, _, _, _, _, _) =>
      CopyIntoTableExec(
        spark,
        catalog,
        ident,
        c.sourcePath,
        c.columns,
        c.fileFormat,
        c.pattern,
        c.force,
        c.onError,
        c.output) :: Nil

    case c @ CopyIntoLocationCommand(_, CopyIntoLocationSource.Query(query), _, _) =>
      CopyIntoLocationExec(
        spark,
        CopyIntoSource.QuerySource(query),
        c.targetPath,
        c.fileFormat,
        c.overwrite,
        c.output) :: Nil

    case c @ CopyIntoLocationCommand(
          _,
          CopyIntoLocationSource.TableName(PaimonCatalogAndIdentifier(catalog, ident)),
          _,
          _) =>
      CopyIntoLocationExec(
        spark,
        CopyIntoSource.TableSource(catalog, ident),
        c.targetPath,
        c.fileFormat,
        c.overwrite,
        c.output) :: Nil

    case _ => Nil
  }

  private def buildInternalRow(exprs: Seq[Expression]): InternalRow = {
    val values = new Array[Any](exprs.size)
    for (index <- exprs.indices) {
      values(index) = exprs(index).eval()
    }
    new GenericInternalRow(values)
  }

  private object PaimonCatalogAndIdentifier {
    def unapply(identifier: Seq[String]): Option[(TableCatalog, Identifier)] = {
      val catalogAndIdentifier =
        SparkUtils.catalogAndIdentifier(spark, identifier.asJava, catalogManager.currentCatalog)
      catalogAndIdentifier.catalog match {
        case paimonCatalog: SparkCatalog =>
          Some((paimonCatalog, catalogAndIdentifier.identifier()))
        case paimonCatalog: SparkGenericCatalog =>
          Some((paimonCatalog, catalogAndIdentifier.identifier()))
        case _ =>
          None
      }
    }
  }

  private def recacheTable(r: ResolvedTable)(): Unit = {
    val v2Relation = DataSourceV2Relation.create(r.table, Some(r.catalog), Some(r.identifier))
    SparkShimLoader.shim.classicApi.recacheByPlan(spark, v2Relation)
  }
}

case class LateralVectorSearchExec(
    innerTable: InnerTable,
    columnName: String,
    queryVectorExpr: Expression,
    limit: Int,
    options: Map[String, String],
    vectorSearchOutput: Seq[Attribute],
    projectList: Seq[Expression],
    projectOutput: Seq[Attribute],
    searchFilters: Seq[Expression],
    child: SparkPlan)
  extends SparkPlan
  with PredicateHelper {

  override def children: Seq[SparkPlan] = Seq(child)

  override def output: Seq[Attribute] = child.output ++ projectOutput

  @transient override lazy val producedAttributes: AttributeSet = {
    AttributeSet(vectorSearchOutput ++ output.filterNot(attr => inputSet.contains(attr)))
  }

  @transient
  override lazy val references: AttributeSet = {
    AttributeSet.fromAttributeSets(expressions.map(_.references)) -- producedAttributes
  }

  override protected def withNewChildrenInternal(newChildren: IndexedSeq[SparkPlan]): SparkPlan = {
    copy(child = newChildren.head)
  }

  override protected def doExecute(): RDD[InternalRow] = {
    child.execute().mapPartitions {
      outerRows =>
        val queryVectorProjection = UnsafeProjection.create(Seq(queryVectorExpr), child.output)
        val rightProjection =
          UnsafeProjection.create(projectList, child.output ++ vectorSearchOutput)
        val joinedRow = new JoinedRow
        val readerTracker = new LateralVectorSearchReaderTracker
        Option(TaskContext.get())
          .foreach(_.addTaskCompletionListener[Unit](_ => readerTracker.closeCurrent()))
        val searchContext = createSearchContext(rightProjection, readerTracker)
        val batchSize = searchContext.batchSize

        outerRows.map(_.copy()).grouped(batchSize).flatMap {
          outerRowBatch =>
            val searchBatch = ArrayBuffer[LateralVectorSearchQuery]()
            outerRowBatch.foreach {
              outerRow =>
                toFloatArray(queryVectorProjection(outerRow).get(0, queryVectorExpr.dataType))
                  .foreach(
                    queryVector => searchBatch += LateralVectorSearchQuery(outerRow, queryVector))
            }

            if (searchBatch.isEmpty) {
              Iterator.empty
            } else {
              search(searchBatch.toVector, searchContext).map {
                case (outerRow, rightRow) =>
                  joinedRow(outerRow, rightRow)
                  joinedRow.copy()
              }
            }
        }
    }
  }

  private def createSearchContext(
      rightProjection: UnsafeProjection,
      readerTracker: LateralVectorSearchReaderTracker): LateralVectorSearchContext = {
    val rowType = innerTable.rowType()
    val readFieldNames = vectorSearchOutput
      .filterNot(_.name == PaimonMetadataColumn.SEARCH_SCORE_COLUMN)
      .map(_.name)
    val readFieldNamesWithRowId =
      if (readFieldNames.contains(SpecialFields.ROW_ID.name())) {
        readFieldNames
      } else {
        readFieldNames :+ SpecialFields.ROW_ID.name()
      }
    val rowTypeWithRowId = SpecialFields.rowTypeWithRowId(rowType)
    val readRowType = rowType.project(readFieldNames.asJava)
    val readRowTypeWithRowId = SpecialFields.rowTypeWithRowId(readRowType)
    val readBuilder = innerTable
      .newReadBuilder()
      .withReadType(rowTypeWithRowId.project(readFieldNamesWithRowId.asJava))
    val scoreMetadataColumns =
      if (vectorSearchOutput.exists(_.name == PaimonMetadataColumn.SEARCH_SCORE_COLUMN)) {
        Seq(PaimonMetadataColumn.SEARCH_SCORE)
      } else {
        Seq.empty
      }
    val resultRowType =
      if (scoreMetadataColumns.isEmpty) {
        readRowTypeWithRowId
      } else {
        new RowType(
          (readRowTypeWithRowId.getFields.asScala ++ scoreMetadataColumns.map(
            _.toPaimonDataField)).asJava)
      }
    val sparkRow = SparkInternalRow.create(resultRowType)
    val vectorSearchBuilder = innerTable
      .newBatchVectorSearchBuilder()
      .withVectorColumn(columnName)
      .withLimit(limit)
      .withOptions(options.asJava)
    pushSearchFilters(readBuilder, vectorSearchBuilder)

    val vectorPlan = vectorSearchBuilder.newVectorScan().scan()
    val batchSize =
      Math.max(1, new CoreOptions(innerTable.options()).vectorSearchLateralJoinBatchSize())

    LateralVectorSearchContext(
      readBuilder,
      vectorSearchBuilder,
      vectorPlan,
      scoreMetadataColumns,
      sparkRow,
      rowIdOrdinal = resultRowType.getFieldIndex(SpecialFields.ROW_ID.name()),
      projectionInputOrdinals = vectorSearchOutput.map {
        attr =>
          if (attr.name == PaimonMetadataColumn.SEARCH_SCORE_COLUMN) {
            -1
          } else {
            resultRowType.getFieldIndex(attr.name)
          }
      },
      rightProjection,
      batchSize,
      readerTracker
    )
  }

  private def pushSearchFilters(
      readBuilder: ReadBuilder,
      vectorSearchBuilder: BatchVectorSearchBuilder): Unit = {
    val predicates = convertSearchFilters()
    if (predicates.nonEmpty) {
      val split = splitPartitionPredicatesAndDataPredicates(
        predicates.asJava,
        innerTable.rowType(),
        innerTable.partitionKeys())
      if (split.getLeft.isPresent) {
        val partitionFilter = split.getLeft.get()
        readBuilder.withPartitionFilter(partitionFilter)
        vectorSearchBuilder.withPartitionFilter(partitionFilter)
      }
      if (!split.getRight.isEmpty) {
        val dataFilter = PredicateBuilder.and(split.getRight)
        readBuilder.withFilter(dataFilter)
        vectorSearchBuilder.withFilter(dataFilter)
      }
    }
  }

  private def convertSearchFilters(): Seq[Predicate] = {
    if (searchFilters.isEmpty) {
      Seq.empty
    } else {
      PaimonTableValuedFunctions
        .convertLateralVectorSearchFilters(
          innerTable,
          vectorSearchOutput,
          projectList,
          projectOutput,
          searchFilters)
        .getOrElse {
          throw new UnsupportedOperationException(
            s"Cannot convert searched-table predicates for LATERAL vector_search: $searchFilters")
        }
    }
  }

  private def search(
      queries: Seq[LateralVectorSearchQuery],
      context: LateralVectorSearchContext): Iterator[(InternalRow, InternalRow)] = {
    val vectors = queries.map(_.queryVector).toArray
    val globalIndexResults = context.vectorSearchBuilder
      .withVectors(vectors)
      .newBatchVectorRead()
      .readBatch(context.vectorPlan)
      .asScala
      .toVector
    // Batch vector search must return one result per input query vector and preserve the input
    // order, because createRowIdToMatches pairs each result with its original outer row by index.
    require(
      globalIndexResults.size == queries.size,
      s"Batch vector search returned ${globalIndexResults.size} results for ${queries.size} " +
        "query vectors. The result count must match the query count."
    )
    val rowIdToMatches = createRowIdToMatches(queries, globalIndexResults)
    val batchGlobalIndexResult = createBatchGlobalIndexResult(globalIndexResults)
    val scan = context.readBuilder
      .newScan()
      .withGlobalIndexResult(batchGlobalIndexResult)
      .asInstanceOf[InnerTableScan]
    val read = context.readBuilder.newRead()

    scan.plan().splits().asScala.iterator.flatMap {
      split =>
        val reader =
          PaimonRecordReaderIterator(read.createReader(split), context.scoreMetadataColumns, split)
        val readerState = context.readerTracker.track(reader)
        new Iterator[Iterator[(InternalRow, InternalRow)]] {
          override def hasNext: Boolean = {
            val hasNext = reader.hasNext
            if (!hasNext) {
              readerState.closeOnce()
            }
            hasNext
          }

          override def next(): Iterator[(InternalRow, InternalRow)] = {
            val rightRow = context.sparkRow.replace(reader.next())
            val rowId = rightRow.getLong(context.rowIdOrdinal)
            rowIdToMatches.getOrElse(rowId, Seq.empty).iterator.map {
              searchMatch =>
                val projectedRow = projectRightRow(rightRow, searchMatch, context)
                (searchMatch.outerRow, projectedRow)
            }
          }
        }.flatMap(identity)
    }
  }

  private def projectRightRow(
      rightRow: InternalRow,
      searchMatch: LateralVectorSearchMatch,
      context: LateralVectorSearchContext): InternalRow = {
    val values = new Array[Any](vectorSearchOutput.size)
    vectorSearchOutput.zipWithIndex.foreach {
      case (attr, index) =>
        val ordinal = context.projectionInputOrdinals(index)
        values(index) = if (ordinal < 0) {
          searchMatch.score
        } else {
          rightRow.get(ordinal, attr.dataType)
        }
    }
    context.rightProjection(new JoinedRow(searchMatch.outerRow, new GenericInternalRow(values)))
  }

  private def createRowIdToMatches(
      queries: Seq[LateralVectorSearchQuery],
      globalIndexResults: Seq[GlobalIndexResult]): Map[Long, Seq[LateralVectorSearchMatch]] = {
    val rowIdToMatches =
      scala.collection.mutable.LinkedHashMap[Long, ArrayBuffer[LateralVectorSearchMatch]]()
    queries.zip(globalIndexResults).foreach {
      case (query, result) =>
        val scoreGetter = result match {
          case scored: ScoredGlobalIndexResult => Some(scored.scoreGetter())
          case _ => None
        }
        result.results().iterator().asScala.foreach {
          rowId =>
            rowIdToMatches.getOrElseUpdate(rowId, ArrayBuffer()) +=
              LateralVectorSearchMatch(
                query.outerRow,
                scoreGetter.map(_.score(rowId)).getOrElse(Float.NaN))
        }
    }
    rowIdToMatches.iterator.map { case (rowId, matches) => rowId -> matches.toSeq }.toMap
  }

  private def createBatchGlobalIndexResult(
      globalIndexResults: Seq[GlobalIndexResult]): GlobalIndexResult = {
    val rowIds = new RoaringNavigableMap64()
    globalIndexResults.foreach(result => rowIds.or(result.results()))
    GlobalIndexResult.create(rowIds)
  }

  private def toFloatArray(value: Any): Option[Array[Float]] = {
    value match {
      case null => None
      case arrayData: ArrayData => Some(arrayData.toFloatArray())
      case _ =>
        throw new RuntimeException(s"Cannot extract query vector from expression value: $value")
    }
  }

  private class LateralVectorSearchReaderTracker {
    @volatile private var currentReader: LateralVectorSearchReaderState = _

    def track(reader: PaimonRecordReaderIterator): LateralVectorSearchReaderState = {
      val state = new LateralVectorSearchReaderState(reader, this)
      this.synchronized {
        currentReader = state
      }
      state
    }

    def clear(state: LateralVectorSearchReaderState): Unit = {
      this.synchronized {
        if (currentReader eq state) {
          currentReader = null
        }
      }
    }

    def closeCurrent(): Unit = {
      val reader = currentReader
      if (reader != null) {
        reader.closeOnce()
      }
    }
  }

  private class LateralVectorSearchReaderState(
      reader: PaimonRecordReaderIterator,
      tracker: LateralVectorSearchReaderTracker) {
    private var closed = false

    def closeOnce(): Unit = {
      this.synchronized {
        if (!closed) {
          closed = true
          try {
            reader.close()
          } finally {
            tracker.clear(this)
          }
        }
      }
    }
  }

  private case class LateralVectorSearchContext(
      readBuilder: ReadBuilder,
      vectorSearchBuilder: BatchVectorSearchBuilder,
      vectorPlan: VectorScan.Plan,
      scoreMetadataColumns: Seq[PaimonMetadataColumn],
      sparkRow: SparkInternalRow,
      rowIdOrdinal: Int,
      projectionInputOrdinals: Seq[Int],
      rightProjection: UnsafeProjection,
      batchSize: Int,
      readerTracker: LateralVectorSearchReaderTracker)

  private case class LateralVectorSearchQuery(outerRow: InternalRow, queryVector: Array[Float])

  private case class LateralVectorSearchMatch(outerRow: InternalRow, score: Float)
}
