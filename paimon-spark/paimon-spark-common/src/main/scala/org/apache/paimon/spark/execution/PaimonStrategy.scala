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
import org.apache.paimon.data.BinaryRow
import org.apache.paimon.globalindex.{GlobalIndexResult, IndexedSplit, ScoredGlobalIndexResult}
import org.apache.paimon.partition.PartitionPredicate
import org.apache.paimon.partition.PartitionPredicate.splitPartitionPredicatesAndDataPredicates
import org.apache.paimon.predicate.{Predicate, PredicateBuilder}
import org.apache.paimon.spark.{PaimonRecordReaderIterator, PaimonScan, PostponeMergeInputScan, PostponeMergeOnRead, SparkCatalog, SparkConnectorOptions, SparkGenericCatalog, SparkTable, SparkUtils}
import org.apache.paimon.spark.catalog.{SparkBaseCatalog, SupportView}
import org.apache.paimon.spark.catalyst.analysis.ResolvedPaimonView
import org.apache.paimon.spark.catalyst.optimizer.RepartitionLateralVectorSearchInput
import org.apache.paimon.spark.catalyst.plans.logical.{CopyIntoLocationCommand, CopyIntoLocationSource, CopyIntoTableCommand, CreateOrReplaceTagCommand, CreatePaimonView, DeleteTagCommand, DropPaimonView, LateralVectorSearch, PaimonCallCommand, PaimonDropPartitions, PaimonTableValuedFunctions, RenameTagCommand, ResolvedIdentifier, ShowPaimonViews, ShowTagsCommand, TruncatePaimonTableWithFilter}
import org.apache.paimon.spark.data.SparkInternalRow
import org.apache.paimon.spark.format.PaimonFormatTable
import org.apache.paimon.spark.read.VectorSearchResultUtils
import org.apache.paimon.spark.schema.PaimonMetadataColumn
import org.apache.paimon.spark.util.OptionUtils
import org.apache.paimon.table.{FileStoreTable, InnerTable, SpecialFields, Table}
import org.apache.paimon.table.source.{BatchVectorSearchBuilder, DataSplit, InnerTableScan, PrimaryKeyScoredResult, PrimaryKeySearchPosition, PrimaryKeyVectorResult, ReadBuilder, VectorScan, VectorSearchSplit}
import org.apache.paimon.types.RowType
import org.apache.paimon.utils.RoaringNavigableMap64

import org.apache.spark.{HashPartitioner, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.{ResolvedNamespace, ResolvedTable}
import org.apache.spark.sql.catalyst.expressions.{And, Attribute, AttributeSet, Expression, GenericInternalRow, JoinedRow, PredicateHelper, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.catalyst.optimizer.BuildRight
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.catalyst.plans.logical.{AddPartitions, CreateTableAsSelect, DescribeRelation, DropPartitions, LogicalPlan, RepairTable, ReplaceTable, ReplaceTableAsSelect, ShowCreateTable}
import org.apache.spark.sql.catalyst.plans.physical.{Distribution, UnspecifiedDistribution}
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.connector.catalog.{Identifier, PaimonLookupCatalog, TableCatalog}
import org.apache.spark.sql.execution.{FilterExec, GlobalLimitExec, LeafExecNode, PaimonDescribeTableExec, ProjectExec, SparkPlan, SparkStrategy, UnaryExecNode}
import org.apache.spark.sql.execution.datasources.v2.{DataSourceV2Implicits, DataSourceV2Relation, DataSourceV2ScanRelation}
import org.apache.spark.sql.execution.exchange.ShuffleExchangeLike
import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, BroadcastNestedLoopJoinExec}
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

    case PhysicalOperation(projects, filters, relation: DataSourceV2ScanRelation) =>
      relation.scan match {
        case scan: PaimonScan if PostponeMergeOnRead.usesCustomSource(scan.table) =>
          scan.planPostponeMerge(spark.sparkContext.defaultParallelism) match {
            case Some(mergePlan) =>
              val inputScan = PostponeMergeInputScan(mergePlan)
              val inputOutput =
                org.apache.spark.sql.PaimonUtils.toAttributes(inputScan.readSchema())
              val inputRelation =
                SparkShimLoader.shim.createDataSourceV2ScanRelation(
                  relation,
                  inputScan,
                  inputOutput)
              val merge =
                PostponeMergeOnReadExec(
                  relation.output,
                  mergePlan,
                  PostponeMergeOnReadExec.computeShufflePartitions(
                    mergePlan.corePlan,
                    scan.coreOptions,
                    spark.sessionState.conf),
                  planLater(inputRelation)
                )
              val filtered =
                filters.reduceLeftOption(And).map(FilterExec(_, merge)).getOrElse(merge)
              ProjectExec(projects, filtered) :: Nil
            case None =>
              ProjectExec(projects, EmptyPostponeMergeExec(relation.output)) :: Nil
          }
        case _ => Nil
      }

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

    case AddPartitions(r @ ResolvedTable(_, _, table: PaimonFormatTable, _), parts, ifNotExists) =>
      PaimonAddFormatTablePartitionsExec(
        table,
        parts.asResolvedPartitionSpecs,
        ifNotExists,
        recacheTable(r)) :: Nil

    // Spark's DataSourceV2Strategy rejects RepairTable for every v2 table; Format Tables with
    // catalog-managed partitions support it through the sync engine, so intercept here (extension
    // strategies run first). Tables using filesystem partition discovery fall through and keep
    // the upstream rejection.
    case RepairTable(
          r @ ResolvedTable(_, _, table: PaimonFormatTable, _),
          enableAddPartitions,
          enableDropPartitions) if table.hasCatalogManagedPartitions =>
      PaimonRepairFormatTablePartitionsExec(
        table,
        enableAddPartitions,
        enableDropPartitions,
        recacheTable(r)) :: Nil

    case DropPartitions(
          r @ ResolvedTable(_, _, table: PaimonFormatTable, _),
          parts,
          ifExists,
          purge) =>
      PaimonDropFormatTablePartitionsExec(
        table,
        parts.asResolvedPartitionSpecs,
        ifExists,
        purge,
        recacheTable(r)) :: Nil

    case PaimonDropPartitions(
          r @ ResolvedTable(_, _, table: PaimonFormatTable, _),
          parts,
          ifExists,
          purge) =>
      PaimonDropFormatTablePartitionsExec(
        table,
        parts.asResolvedPartitionSpecs,
        ifExists,
        purge,
        recacheTable(r)) :: Nil

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

private case class EmptyPostponeMergeExec(output: Seq[Attribute]) extends LeafExecNode {

  override protected def doExecute(): RDD[InternalRow] = sparkContext.emptyRDD[InternalRow]
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

  // Statistics-based broadcast selection is only known after physical planning. Request a
  // distribution here so EnsureRequirements can restore the streamed LIMIT side's parallelism.
  override def requiredChildDistribution: Seq[Distribution] = {
    if (hasUnrepartitionedGlobalLimit(child)) {
      Seq(
        SparkShimLoader.shim.createClusteredDistribution(
          child.output,
          RepartitionLateralVectorSearchInput.parallelism))
    } else {
      Seq(UnspecifiedDistribution)
    }
  }

  private def hasUnrepartitionedGlobalLimit(plan: SparkPlan): Boolean = plan match {
    case _: ShuffleExchangeLike => false
    case _: GlobalLimitExec => true
    case join: BroadcastHashJoinExec =>
      hasUnrepartitionedGlobalLimit(if (join.buildSide == BuildRight) join.left else join.right)
    case join: BroadcastNestedLoopJoinExec =>
      hasUnrepartitionedGlobalLimit(if (join.buildSide == BuildRight) join.left else join.right)
    case unary: UnaryExecNode => hasUnrepartitionedGlobalLimit(unary.child)
    case _ => false
  }

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
    if (distributedSearchEnabled) {
      val plannedSearch = createPlannedSearch()
      val splits = plannedSearch.splits
      val groups =
        LateralVectorSearchExecution.splitGroups(splits, distributedMaxSplitGroups)
      val coreOptions = new CoreOptions(innerTable.options())
      if (
        plannedSearch.table.isDefined &&
        groups.size > 1 &&
        !coreOptions.deletionVectorsEnabled() &&
        LateralVectorSearchExecution.canDistribute(
          splits,
          options,
          innerTable.options().asScala.toMap)
      ) {
        return executeDistributed(plannedSearch, groups)
      }
    }
    executeLocal()
  }

  private def executeLocal(): RDD[InternalRow] = {
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

  private def distributedSearchEnabled: Boolean = {
    OptionUtils
      .getOptionString(SparkConnectorOptions.VECTOR_SEARCH_LATERAL_JOIN_DISTRIBUTED_ENABLED)
      .toBoolean
  }

  private def createPlannedSearch(): LateralVectorSearchPlan = {
    val vectorSearchBuilder = createVectorSearchBuilder(Seq.empty)
    val vectorPlan = vectorSearchBuilder.newVectorScan().scan()
    val splits = vectorPlan.splits().asScala.toSeq
    val batchSize =
      Math.max(1, new CoreOptions(innerTable.options()).vectorSearchLateralJoinBatchSize())
    val plannedTable =
      if (vectorPlan.plannedSnapshotId().isPresent) {
        innerTable match {
          case table: FileStoreTable =>
            Some(
              table.copyWithoutTimeTravel(Map(
                CoreOptions.SCAN_SNAPSHOT_ID.key() ->
                  vectorPlan.plannedSnapshotId().getAsLong.toString,
                CoreOptions.SCAN_TAG_NAME.key() -> null,
                CoreOptions.SCAN_TIMESTAMP.key() -> null,
                CoreOptions.SCAN_TIMESTAMP_MILLIS.key() -> null,
                CoreOptions.SCAN_WATERMARK.key() -> null,
                CoreOptions.SCAN_VERSION.key() -> null
              ).asJava))
          case _ => None
        }
      } else {
        None
      }
    LateralVectorSearchPlan(plannedTable, splits, batchSize)
  }

  private def executeDistributed(
      plannedSearch: LateralVectorSearchPlan,
      groups: Seq[LateralVectorSearchExecution.SplitGroup]): RDD[InternalRow] = {
    import LateralVectorSearchExecution._

    val searchTable = plannedSearch.table.get
    val childRDD = child.execute()
    val queryBatches = childRDD
      .mapPartitionsWithIndex {
        case (inputPartition, outerRows) =>
          val queryVectorProjection = UnsafeProjection.create(Seq(queryVectorExpr), child.output)
          val outerRowProjection = UnsafeProjection.create(child.output, child.output)
          var batchOrdinal = 0L
          outerRows
            .flatMap {
              outerRow =>
                toFloatArray(queryVectorProjection(outerRow).get(0, queryVectorExpr.dataType))
                  .map {
                    queryVector =>
                      QueryPayload(queryVector, outerRowProjection(outerRow).copy().getBytes)
                  }
            }
            .grouped(plannedSearch.batchSize)
            .map {
              batch =>
                val batchId = QueryBatchId(inputPartition, batchOrdinal)
                batchOrdinal += 1
                QueryBatch(batchId, batch.toArray)
            }
      }

    val groupsById = groups.map(group => group.id -> group).toMap
    val searchWork = queryBatches
      .flatMap {
        batch =>
          val vectors = batch.queries.map(_.vector)
          groups.iterator.map {
            group =>
              val outerRows =
                if (group.id == 0) batch.queries.map(_.outerRowBytes) else null
              group.id -> SearchQueryBatch(batch.id, vectors, outerRows)
          }
      }
      .partitionBy(new HashPartitioner(groups.size))

    val candidateResults = searchWork.mapPartitions {
      workItems =>
        if (!workItems.hasNext) {
          Iterator.empty
        } else {
          val first = workItems.next()
          val groupId = first._1
          val group = groupsById(groupId)
          val vectorSearchBuilder = createVectorSearchBuilder(Seq.empty, searchTable)
          val groupBatches = (Iterator.single(first) ++ workItems).map {
            case (currentGroupId, searchBatch) =>
              require(
                currentGroupId == groupId,
                s"Expected one vector split group per partition, but found groups $groupId and " +
                  s"$currentGroupId.")
              searchBatch
          }
          LateralVectorSearchExecution
            .groupSearchBatches(groupBatches, plannedSearch.batchSize)
            .flatMap {
              searchBatches =>
                val vectors = searchBatches.iterator.flatMap(_.vectors).toArray
                val groupSplits = group.splits.asJava
                val groupPlan = new VectorScan.Plan {
                  override def splits(): java.util.List[VectorSearchSplit] = groupSplits
                }
                val results = vectorSearchBuilder
                  .withVectors(vectors)
                  .newBatchVectorRead()
                  .readBatch(groupPlan)
                  .asScala
                require(
                  results.size == vectors.length,
                  s"Distributed batch vector search returned ${results.size} results for " +
                    s"${vectors.length} query vectors."
                )

                var resultOffset = 0
                searchBatches.iterator.map {
                  searchBatch =>
                    val candidates = results
                      .slice(resultOffset, resultOffset + searchBatch.vectors.length)
                      .map {
                        case scored: ScoredGlobalIndexResult => ScoredCandidates.from(scored)
                        case result =>
                          throw new IllegalStateException(
                            "Distributed vector search requires scored global-index results, " +
                              s"but got ${result.getClass.getName}")
                      }
                      .toArray
                    resultOffset += searchBatch.vectors.length
                    searchBatch.id -> PartialBatchResult(searchBatch.outerRowBytes, candidates)
                }
            }
        }
    }

    val mergeParallelism =
      Math.max(groups.size, Math.min(childRDD.getNumPartitions, lateralJoinParallelism))
    val mergedResults = candidateResults.combineByKey[PartialBatchResult](
      (value: PartialBatchResult) => value,
      (left: PartialBatchResult, right: PartialBatchResult) => left.merge(right, limit),
      (left: PartialBatchResult, right: PartialBatchResult) => left.merge(right, limit),
      new HashPartitioner(Math.max(1, mergeParallelism))
    )

    mergedResults.mapPartitions {
      merged =>
        val rightProjection =
          UnsafeProjection.create(projectList, child.output ++ vectorSearchOutput)
        val joinedRow = new JoinedRow
        val readerTracker = new LateralVectorSearchReaderTracker
        Option(TaskContext.get())
          .foreach(_.addTaskCompletionListener[Unit](_ => readerTracker.closeCurrent()))
        val materializationContext =
          createMaterializationContext(rightProjection, readerTracker, searchTable)

        merged.flatMap {
          case (_, partial) =>
            val queries = ArrayBuffer[LateralVectorSearchQuery]()
            val results = ArrayBuffer[GlobalIndexResult]()
            if (partial.outerRowBytes != null) {
              partial.outerRowBytes.zip(partial.candidates).foreach {
                case (outerRowBytes, candidates) if !candidates.isEmpty =>
                  val outerRow = new UnsafeRow(child.output.size)
                  outerRow.pointTo(outerRowBytes, outerRowBytes.length)
                  queries += LateralVectorSearchQuery(outerRow, Array.emptyFloatArray)
                  results += candidates.toResult
                case _ =>
              }
            }
            materializeSearchResults(queries.toVector, results.toVector, materializationContext)
              .map {
                case (outerRow, rightRow) =>
                  joinedRow(outerRow, rightRow)
                  joinedRow.copy()
              }
        }
    }
  }

  private def createSearchContext(
      rightProjection: UnsafeProjection,
      readerTracker: LateralVectorSearchReaderTracker): LateralVectorSearchContext = {
    val materializationContext =
      createMaterializationContext(rightProjection, readerTracker)
    val vectorSearchBuilder =
      createVectorSearchBuilder(Seq.empty)
    val vectorPlan = vectorSearchBuilder.newVectorScan().scan()
    val batchSize =
      Math.max(1, new CoreOptions(innerTable.options()).vectorSearchLateralJoinBatchSize())

    LateralVectorSearchContext(materializationContext, vectorSearchBuilder, vectorPlan, batchSize)
  }

  private def createMaterializationContext(
      rightProjection: UnsafeProjection,
      readerTracker: LateralVectorSearchReaderTracker,
      table: InnerTable = innerTable): LateralVectorSearchMaterializationContext = {
    val rowType = table.rowType()
    val readFieldNames = vectorSearchOutput
      .filterNot(
        attr =>
          attr.name == PaimonMetadataColumn.SEARCH_SCORE_COLUMN ||
            attr.name == SpecialFields.ROW_ID.name())
      .map(_.name)
    val readFieldNamesWithRowId = readFieldNames :+ SpecialFields.ROW_ID.name()
    val rowTypeWithRowId = SpecialFields.rowTypeWithRowId(rowType)
    val readRowType = rowType.project(readFieldNames.asJava)
    val readRowTypeWithRowId = SpecialFields.rowTypeWithRowId(readRowType)
    val readBuilder = table
      .newReadBuilder()
      .withReadType(rowTypeWithRowId.project(readFieldNamesWithRowId.asJava))
    val physicalReadBuilder = table
      .newReadBuilder()
      .withReadType(readRowType)
    pushSearchFilters(Seq(readBuilder, physicalReadBuilder), None, table)
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
    LateralVectorSearchMaterializationContext(
      readBuilder,
      physicalReadBuilder,
      scoreMetadataColumns,
      sparkRow,
      rowIdOrdinal = resultRowType.getFieldIndex(SpecialFields.ROW_ID.name()),
      metaColumnsOnly =
        VectorSearchResultUtils.isVectorSearchMetaOnly(vectorSearchOutput.map(_.name)),
      projectionInputOrdinals = vectorSearchOutput.map {
        attr =>
          if (attr.name == PaimonMetadataColumn.SEARCH_SCORE_COLUMN) {
            -1
          } else {
            resultRowType.getFieldIndex(attr.name)
          }
      },
      rightProjection,
      readerTracker
    )
  }

  private def createVectorSearchBuilder(
      readBuilders: Seq[ReadBuilder],
      table: InnerTable = innerTable): BatchVectorSearchBuilder = {
    val vectorSearchBuilder = table
      .newBatchVectorSearchBuilder()
      .withVectorColumn(columnName)
      .withLimit(limit)
      .withOptions(options.asJava)
    pushSearchFilters(readBuilders, Some(vectorSearchBuilder), table)
    vectorSearchBuilder
  }

  private def pushSearchFilters(
      readBuilders: Seq[ReadBuilder],
      vectorSearchBuilder: Option[BatchVectorSearchBuilder],
      table: InnerTable): Unit = {
    val predicates = convertSearchFilters(table)
    if (predicates.nonEmpty) {
      val split = splitPartitionPredicatesAndDataPredicates(
        predicates.asJava,
        table.rowType(),
        table.partitionKeys())
      if (split.getLeft.isPresent) {
        val partitionFilter = split.getLeft.get()
        readBuilders.foreach(_.withPartitionFilter(partitionFilter))
        vectorSearchBuilder.foreach(_.withPartitionFilter(partitionFilter))
      }
      if (!split.getRight.isEmpty) {
        val dataFilter = PredicateBuilder.and(split.getRight)
        readBuilders.foreach(_.withFilter(dataFilter))
        vectorSearchBuilder.foreach(_.withFilter(dataFilter))
      }
    }
  }

  private def convertSearchFilters(table: InnerTable): Seq[Predicate] = {
    if (searchFilters.isEmpty) {
      Seq.empty
    } else {
      PaimonTableValuedFunctions
        .convertLateralVectorSearchFilters(
          table,
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

  private def distributedMaxSplitGroups: Int = {
    val value =
      OptionUtils
        .getOptionString(
          SparkConnectorOptions.VECTOR_SEARCH_LATERAL_JOIN_DISTRIBUTED_MAX_SPLIT_GROUPS)
        .toInt
    require(
      value > 0,
      s"spark.paimon.${SparkConnectorOptions.VECTOR_SEARCH_LATERAL_JOIN_DISTRIBUTED_MAX_SPLIT_GROUPS
          .key()} must be positive, but got $value")
    value
  }

  private def lateralJoinParallelism: Int = {
    val value =
      OptionUtils
        .getOptionString(SparkConnectorOptions.VECTOR_SEARCH_LATERAL_JOIN_PARALLELISM)
        .toInt
    require(
      value > 0,
      s"spark.paimon.${SparkConnectorOptions.VECTOR_SEARCH_LATERAL_JOIN_PARALLELISM
          .key()} must be positive, but got $value")
    value
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
    materializeSearchResults(queries, globalIndexResults, context.materializationContext)
  }

  private def materializeSearchResults(
      queries: Seq[LateralVectorSearchQuery],
      globalIndexResults: Seq[GlobalIndexResult],
      context: LateralVectorSearchMaterializationContext): Iterator[(InternalRow, InternalRow)] = {
    val primaryKeyResults = primaryKeyVectorResults(globalIndexResults)
    if (context.metaColumnsOnly) {
      return primaryKeyResults match {
        case Some(results) => searchPrimaryKeyMetaColumns(queries, results, context)
        case None => searchMetaColumns(queries, globalIndexResults, context)
      }
    }
    primaryKeyResults.foreach(results => return searchPrimaryKeyRows(queries, results, context))
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

  private def searchPrimaryKeyMetaColumns(
      queries: Seq[LateralVectorSearchQuery],
      results: Seq[PrimaryKeyVectorResult],
      context: LateralVectorSearchMaterializationContext): Iterator[(InternalRow, InternalRow)] = {
    queries.zip(results).iterator.flatMap {
      case (query, result) =>
        result.positions().iterator().asScala.map {
          position =>
            val values = vectorSearchOutput.map {
              attr =>
                attr.name match {
                  case PaimonMetadataColumn.ROW_ID_COLUMN => position.rowPosition()
                  case PaimonMetadataColumn.SEARCH_SCORE_COLUMN => position.score()
                  case name =>
                    throw new IllegalArgumentException(
                      s"Unsupported primary-key vector search metadata column: $name")
                }
            }.toArray
            val projectedRow = context.rightProjection(
              new JoinedRow(
                query.outerRow,
                new GenericInternalRow(values.asInstanceOf[Array[Any]])))
            (query.outerRow, projectedRow)
        }
    }
  }

  private def primaryKeyVectorResults(
      globalIndexResults: Seq[GlobalIndexResult]): Option[Seq[PrimaryKeyVectorResult]] = {
    val results = globalIndexResults.collect { case result: PrimaryKeyVectorResult => result }
    require(
      results.isEmpty || results.size == globalIndexResults.size,
      "Batch vector search cannot mix primary-key physical results with global row-ID results."
    )
    if (results.isEmpty) None else Some(results)
  }

  private def searchPrimaryKeyRows(
      queries: Seq[LateralVectorSearchQuery],
      results: Seq[PrimaryKeyVectorResult],
      context: LateralVectorSearchMaterializationContext): Iterator[(InternalRow, InternalRow)] = {
    val snapshotId = results.head.snapshotId()
    require(
      results.forall(_.snapshotId() == snapshotId),
      "Primary-key batch vector results must belong to the same snapshot."
    )

    val positionToMatches = scala.collection.mutable
      .LinkedHashMap[LateralVectorSearchPhysicalPosition, ArrayBuffer[LateralVectorSearchMatch]]()
    val uniquePositions = scala.collection.mutable
      .LinkedHashMap[LateralVectorSearchPhysicalPosition, PrimaryKeySearchPosition]()
    queries.zip(results).foreach {
      case (query, result) =>
        result.positions().asScala.foreach {
          position =>
            val key = LateralVectorSearchPhysicalPosition.from(position)
            positionToMatches.getOrElseUpdate(key, ArrayBuffer()) +=
              LateralVectorSearchMatch(query.outerRow, position.score())
            uniquePositions.getOrElseUpdate(key, position)
        }
    }

    val sourceSplits =
      scala.collection.mutable.LinkedHashMap[LateralVectorSearchPhysicalFile, DataSplit]()
    results.foreach {
      result =>
        result.splits().asScala.foreach {
          split =>
            val dataSplit = split.dataSplit()
            val key = LateralVectorSearchPhysicalFile.from(dataSplit)
            sourceSplits.getOrElseUpdate(key, dataSplit)
        }
    }
    val batchResult = new PrimaryKeyScoredResult(
      snapshotId,
      sourceSplits.values.toList.asJava,
      uniquePositions.values.toList.asJava)
    val scan = context.physicalReadBuilder
      .newScan()
      .withGlobalIndexResult(batchResult)
      .asInstanceOf[InnerTableScan]
    val read = context.physicalReadBuilder.newRead()

    scan.plan().splits().asScala.iterator.flatMap {
      split =>
        val indexedSplit = split.asInstanceOf[IndexedSplit]
        val dataSplit = indexedSplit.dataSplit()
        val file = LateralVectorSearchPhysicalFile.from(dataSplit)
        val reader =
          PaimonRecordReaderIterator(
            read.createReader(split),
            Seq(PaimonMetadataColumn.ROW_ID) ++ context.scoreMetadataColumns,
            split)
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
            val position = LateralVectorSearchPhysicalPosition(
              file.partition,
              file.bucket,
              file.dataFileName,
              rightRow.getLong(context.rowIdOrdinal))
            positionToMatches.getOrElse(position, Seq.empty).iterator.map {
              searchMatch =>
                val projectedRow = projectRightRow(rightRow, searchMatch, context)
                (searchMatch.outerRow, projectedRow)
            }
          }
        }.flatMap(identity)
    }
  }

  private def searchMetaColumns(
      queries: Seq[LateralVectorSearchQuery],
      globalIndexResults: Seq[GlobalIndexResult],
      context: LateralVectorSearchMaterializationContext): Iterator[(InternalRow, InternalRow)] = {
    queries.zip(globalIndexResults).iterator.flatMap {
      case (query, result) =>
        val scoreGetter = result match {
          case scored: ScoredGlobalIndexResult => Some(scored.scoreGetter())
          case _ => None
        }
        result.results().iterator().asScala.map {
          rowId =>
            val values = vectorSearchOutput
              .map(attr => VectorSearchResultUtils.valueOf(attr.name, rowId, scoreGetter))
              .toArray
            val projectedRow = context.rightProjection(
              new JoinedRow(
                query.outerRow,
                new GenericInternalRow(values.asInstanceOf[Array[Any]])))
            (query.outerRow, projectedRow)
        }
    }
  }

  private def projectRightRow(
      rightRow: InternalRow,
      searchMatch: LateralVectorSearchMatch,
      context: LateralVectorSearchMaterializationContext): InternalRow = {
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
      materializationContext: LateralVectorSearchMaterializationContext,
      vectorSearchBuilder: BatchVectorSearchBuilder,
      vectorPlan: VectorScan.Plan,
      batchSize: Int)

  private case class LateralVectorSearchMaterializationContext(
      readBuilder: ReadBuilder,
      physicalReadBuilder: ReadBuilder,
      scoreMetadataColumns: Seq[PaimonMetadataColumn],
      sparkRow: SparkInternalRow,
      rowIdOrdinal: Int,
      metaColumnsOnly: Boolean,
      projectionInputOrdinals: Seq[Int],
      rightProjection: UnsafeProjection,
      readerTracker: LateralVectorSearchReaderTracker)

  private case class LateralVectorSearchPlan(
      table: Option[InnerTable],
      splits: Seq[VectorSearchSplit],
      batchSize: Int)

  private case class LateralVectorSearchQuery(outerRow: InternalRow, queryVector: Array[Float])

  private case class LateralVectorSearchMatch(outerRow: InternalRow, score: Float)

  private case class LateralVectorSearchPhysicalFile(
      partition: BinaryRow,
      bucket: Int,
      dataFileName: String)

  private object LateralVectorSearchPhysicalFile {
    def from(split: DataSplit): LateralVectorSearchPhysicalFile = {
      require(
        split.dataFiles().size() == 1,
        "Primary-key indexed split must contain exactly one data file."
      )
      LateralVectorSearchPhysicalFile(
        split.partition().copy(),
        split.bucket(),
        split.dataFiles().get(0).fileName())
    }
  }

  private case class LateralVectorSearchPhysicalPosition(
      partition: BinaryRow,
      bucket: Int,
      dataFileName: String,
      rowPosition: Long)

  private object LateralVectorSearchPhysicalPosition {
    def from(position: PrimaryKeySearchPosition): LateralVectorSearchPhysicalPosition = {
      LateralVectorSearchPhysicalPosition(
        position.partition().copy(),
        position.bucket(),
        position.dataFileName(),
        position.rowPosition())
    }
  }
}
