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

import org.apache.paimon.options.Options
import org.apache.paimon.spark.SparkTable
import org.apache.paimon.spark.catalyst.Compatibility
import org.apache.paimon.spark.catalyst.analysis.PaimonRelation.isPaimonTable
import org.apache.paimon.spark.catalyst.plans.logical.{PaimonDropPartitions, PaimonHiveDynamicPartitionQuery}
import org.apache.paimon.spark.commands.{PaimonAnalyzeTableColumnCommand, PaimonDynamicPartitionOverwriteCommand, PaimonShowColumnsCommand, SchemaEvolutionHelper}
import org.apache.paimon.table.FileStoreTable

import org.apache.spark.sql.{PaimonUtils, SparkSession}
import org.apache.spark.sql.catalyst.analysis.ResolvedTable
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.{AnalysisHelper, _}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.catalyst.util.CharVarcharUtils
import org.apache.spark.sql.connector.catalog.TableCapability
import org.apache.spark.sql.execution.datasources.v2.{DataSourceV2Implicits, DataSourceV2Relation}

import scala.collection.JavaConverters._

class PaimonAnalysis(session: SparkSession) extends Rule[LogicalPlan] {
  import DataSourceV2Implicits._
  import PaimonAnalysis._

  override def apply(plan: LogicalPlan): LogicalPlan = {
    val withPaimonWrites = AnalysisHelper.allowInvokingTransformsInAnalyzer {
      plan.transformDown {
        // Keep fallback conversion before the generic V2 rewrite; otherwise an already-resolved
        // query can remain as OverwritePartitionsDynamic until Spark's capability check rejects it.
        case o @ PaimonDynamicPartitionOverwrite(r, d) if o.resolved =>
          PaimonDynamicPartitionOverwriteCommand(r, d, o.query, o.writeOptions, o.isByName)

        case a @ PaimonV2WriteCommand(table)
            if a.query.getTagValue(PAIMON_WRITE_RESOLVED).isEmpty =>
          val options = Options.fromMap(writeOptions(a).asJava)
          val mergeSchemaEnabled = SchemaEvolutionHelper.mergeSchemaEnabled(options)
          val newQuery = resolvePaimonWrite(a, table, options, mergeSchemaEnabled)
          if (newQuery ne a.query) {
            // Tag to short-circuit the next Analyzer pass; otherwise inline-kept extras would loop.
            newQuery.setTagValue(PAIMON_WRITE_RESOLVED, ())
            Compatibility.withNewQuery(a, newQuery)
          } else {
            a
          }
      }
    }

    withPaimonWrites.resolveOperatorsDown {
      case merge: MergeIntoTable
          if !merge.resolved && isPaimonTable(merge.targetTable) && merge.childrenResolved =>
        PaimonMergeIntoResolver(merge, session)

      case s @ ShowColumns(PaimonRelation(table), _, _) if s.resolved =>
        PaimonShowColumnsCommand(table)

      case d @ PaimonDropPartitions(ResolvedTable(_, _, table: SparkTable, _), parts, _, _)
          if d.resolved =>
        PaimonDropPartitions.validate(table, parts.asResolvedPartitionSpecs)
        d

      case r: ReplaceColumns if r.resolved && isPaimonTable(r.table) =>
        // Spark rewrites REPLACE COLUMNS into a batch that drops every existing column and re-adds
        // the new set. Re-adding columns assigns brand-new field ids while existing data files keep
        // the old ids, so same-named columns are read back as null, silently corrupting data. Reject
        // it here, before the change batch reaches the catalog where it is indistinguishable from an
        // ordinary drop+add.
        throw new UnsupportedOperationException(
          "ALTER TABLE ... REPLACE COLUMNS is not supported for Paimon tables. " +
            "Please use RENAME COLUMN, ALTER COLUMN TYPE, DROP COLUMN, and ADD COLUMN instead.")
    }
  }

  private def writeOptions(v2WriteCommand: V2WriteCommand): Map[String, String] = {
    v2WriteCommand match {
      case a: AppendData => a.writeOptions
      case o: OverwriteByExpression => o.writeOptions
      case op: OverwritePartitionsDynamic => op.writeOptions
      case _ => Map.empty[String, String]
    }
  }

  private def resolvePaimonWrite(
      v2WriteCommand: V2WriteCommand,
      table: DataSourceV2Relation,
      options: Options,
      mergeSchemaEnabled: Boolean): LogicalPlan = {
    val query = stripHiveDynamicPartitionMarker(v2WriteCommand.query)
    hiveDynamicPartitionColumns(v2WriteCommand.query) match {
      case Some(dynamicPartitionColumns) if !v2WriteCommand.isByName =>
        resolveDynamicPartitionWrite(
          query,
          table,
          hiveStyleDynamicPartitionOutput(table, dynamicPartitionColumns),
          options,
          mergeSchemaEnabled)
      case _ =>
        v2WriteCommand match {
          case o: OverwritePartitionsDynamic if !o.isByName =>
            resolveDynamicPartitionWrite(
              query,
              table,
              hiveStyleDynamicPartitionOutput(query, table),
              options,
              mergeSchemaEnabled)
          case _ =>
            val expected =
              expectedAttrsForWrite(query, table, options, v2WriteCommand.isByName)
            resolveWriteOutput(
              query,
              table.name,
              expected,
              v2WriteCommand.isByName,
              mergeSchemaEnabled)
        }
    }
  }

  private def hiveDynamicPartitionColumns(query: LogicalPlan): Option[Seq[String]] = {
    query.collectFirst {
      case PaimonHiveDynamicPartitionQuery(dynamicPartitionColumns, _) =>
        dynamicPartitionColumns
    }
  }

  private def stripHiveDynamicPartitionMarker(query: LogicalPlan): LogicalPlan = {
    query.transformDown { case PaimonHiveDynamicPartitionQuery(_, child) => child }
  }

  private def resolveDynamicPartitionWrite(
      query: LogicalPlan,
      table: DataSourceV2Relation,
      hiveStyleOutput: Option[Seq[Attribute]],
      options: Options,
      mergeSchemaEnabled: Boolean): LogicalPlan = {
    hiveStyleOutput match {
      case Some(hiveStyleOutput)
          if !sameOutputNames(query.output, table.output) &&
            !sameOutputNames(hiveStyleOutput, table.output) =>
        val hiveStyleQuery =
          resolveWriteOutput(query, table.name, hiveStyleOutput, byName = false, mergeSchemaEnabled)
        resolveWriteOutput(
          hiveStyleQuery,
          table.name,
          expectedAttrsForWrite(hiveStyleQuery, table, options, byName = true),
          byName = true,
          mergeSchemaEnabled)
      case _ =>
        resolveWriteOutput(
          query,
          table.name,
          expectedAttrsForWrite(query, table, options, byName = false),
          byName = false,
          mergeSchemaEnabled)
    }
  }

  private def expectedAttrsForWrite(
      query: LogicalPlan,
      table: DataSourceV2Relation,
      options: Options,
      byName: Boolean): Seq[Attribute] = {
    SchemaEvolutionHelper.expectedAttrsForCatalogWrite(
      table,
      query.schema,
      options,
      byName,
      session)
  }

  private def resolveWriteOutput(
      query: LogicalPlan,
      tableName: String,
      expectedOutput: Seq[Attribute],
      byName: Boolean,
      mergeSchemaEnabled: Boolean): LogicalPlan = {
    if (paimonWriteResolved(query, expectedOutput)) {
      query
    } else {
      PaimonOutputResolver.resolveOutputColumns(
        tableName,
        expectedOutput,
        query,
        byName,
        mergeSchemaEnabled)
    }
  }

  private def hiveStyleDynamicPartitionOutput(
      query: LogicalPlan,
      table: DataSourceV2Relation): Option[Seq[Attribute]] = {
    val dynamicPartitionColumns =
      table.table.asInstanceOf[SparkTable].getTable.partitionKeys().asScala.toSeq
    hiveStyleDynamicPartitionOutput(table, dynamicPartitionColumns).filter {
      hiveStyleOutput => sameOutputNames(query.output, hiveStyleOutput)
    }
  }

  private def hiveStyleDynamicPartitionOutput(
      table: DataSourceV2Relation,
      dynamicPartitionColumns: Seq[String]): Option[Seq[Attribute]] = {
    val partitionKeys = table.table.asInstanceOf[SparkTable].getTable.partitionKeys().asScala.toSeq
    if (partitionKeys.isEmpty || dynamicPartitionColumns.isEmpty) {
      None
    } else {
      val dynamicPartitionAttrs = partitionKeys
        .filter {
          partition => dynamicPartitionColumns.exists(dynamic => conf.resolver(dynamic, partition))
        }
        .flatMap {
          dynamicPartition => table.output.find(attr => conf.resolver(attr.name, dynamicPartition))
        }
      val dataAttrs = table.output.filterNot {
        attr => dynamicPartitionColumns.exists(partition => conf.resolver(attr.name, partition))
      }
      val hiveStyleOutput = dataAttrs ++ dynamicPartitionAttrs
      if (dynamicPartitionAttrs.size == dynamicPartitionColumns.size) {
        Some(hiveStyleOutput)
      } else {
        None
      }
    }
  }

  private def sameOutputNames(left: Seq[Attribute], right: Seq[Attribute]): Boolean = {
    left.length == right.length &&
    left.zip(right).forall { case (l, r) => conf.resolver(l.name, r.name) }
  }

  // Mirrors Spark's V2WriteCommand `outputResolved` strictness: query and table outputs must match
  // by name, position, type (ignoring nullable compatibility), and nullability. Any nested
  // structural differences also have to be reconciled before we declare the write resolved.
  private def paimonWriteResolved(query: LogicalPlan, expectedOutput: Seq[Attribute]): Boolean = {
    query.output.size == expectedOutput.size &&
    query.output.zip(expectedOutput).forall {
      case (inAttr, outAttr) =>
        val inType = CharVarcharUtils.getRawType(inAttr.metadata).getOrElse(inAttr.dataType)
        val outType = CharVarcharUtils.getRawType(outAttr.metadata).getOrElse(outAttr.dataType)
        inAttr.name == outAttr.name &&
        PaimonUtils.equalsIgnoreCompatibleNullability(inType, outType) &&
        (outAttr.nullable || !inAttr.nullable)
    }
  }

}

object PaimonAnalysis {
  val PAIMON_WRITE_RESOLVED: TreeNodeTag[Unit] = TreeNodeTag[Unit]("paimon.write.resolved")
}

case class PaimonPostHocResolutionRules(session: SparkSession) extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = {
    val withoutHiveDynamicPartitionMarkers = AnalysisHelper.allowInvokingTransformsInAnalyzer {
      plan.transformDown { case PaimonHiveDynamicPartitionQuery(_, child) => child }
    }

    withoutHiveDynamicPartitionMarkers match {
      case a @ AnalyzeTable(
            ResolvedTable(catalog, identifier, table: SparkTable, _),
            partitionSpec,
            noScan) if a.resolved =>
        if (partitionSpec.nonEmpty) {
          throw new UnsupportedOperationException("Analyze table partition is not supported")
        } else if (noScan) {
          throw new IllegalArgumentException("NOSCAN is ineffective with paimon")
        } else {
          PaimonAnalyzeTableColumnCommand(
            catalog,
            identifier,
            table,
            Option.apply(Seq()),
            allColumns = false)
        }

      case a @ AnalyzeColumn(
            ResolvedTable(catalog, identifier, table: SparkTable, _),
            columnNames,
            allColumns) if a.resolved =>
        PaimonAnalyzeTableColumnCommand(catalog, identifier, table, columnNames, allColumns)

      case _ => withoutHiveDynamicPartitionMarkers
    }
  }
}

object PaimonV2WriteCommand {
  def unapply(o: V2WriteCommand): Option[DataSourceV2Relation] = {
    if (o.query.resolved) {
      o.table match {
        case r: DataSourceV2Relation if r.table.isInstanceOf[SparkTable] => Some(r)
        case _ => None
      }
    } else {
      None
    }
  }
}

object PaimonDynamicPartitionOverwrite {
  def unapply(o: OverwritePartitionsDynamic): Option[(DataSourceV2Relation, FileStoreTable)] = {
    if (o.query.resolved) {
      // when overwrite dynamic is not supported, fallback to use v1 write
      o.table match {
        case r: DataSourceV2Relation
            if r.table.isInstanceOf[SparkTable] && !r.table
              .capabilities()
              .contains(TableCapability.OVERWRITE_DYNAMIC) =>
          Some((r, r.table.asInstanceOf[SparkTable].getTable.asInstanceOf[FileStoreTable]))
        case _ => None
      }
    } else {
      None
    }
  }
}
