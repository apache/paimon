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
import org.apache.paimon.spark.catalyst.plans.logical.PaimonDropPartitions
import org.apache.paimon.spark.commands.{PaimonAnalyzeTableColumnCommand, PaimonDynamicPartitionOverwriteCommand, PaimonShowColumnsCommand, SchemaEvolutionHelper}
import org.apache.paimon.table.FileStoreTable

import org.apache.spark.sql.{PaimonUtils, SparkSession}
import org.apache.spark.sql.catalyst.analysis.{NamedRelation, ResolvedTable}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.catalyst.util.CharVarcharUtils
import org.apache.spark.sql.connector.catalog.TableCapability
import org.apache.spark.sql.execution.datasources.v2.{DataSourceV2Implicits, DataSourceV2Relation}

import scala.collection.JavaConverters._

class PaimonAnalysis(session: SparkSession) extends Rule[LogicalPlan] {
  import DataSourceV2Implicits._
  import PaimonAnalysis._
  override def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsDown {

    case a @ PaimonV2WriteCommand(table)
        if !paimonWriteResolved(a.query, table) &&
          a.query.getTagValue(PAIMON_WRITE_RESOLVED).isEmpty =>
      val options = Options.fromMap(writeOptions(a).asJava)
      val mergeSchemaEnabled = SchemaEvolutionHelper.mergeSchemaEnabled(options)
      val expected = SchemaEvolutionHelper.expectedAttrsForCatalogWrite(
        table,
        a.query.schema,
        options,
        a.isByName,
        session)
      val newQuery = PaimonOutputResolver.resolveOutputColumns(
        table.name,
        expected,
        a.query,
        a.isByName,
        mergeSchemaEnabled)
      if (newQuery ne a.query) {
        // Tag to short-circuit the next Analyzer pass; otherwise inline-kept extras would loop.
        newQuery.setTagValue(PAIMON_WRITE_RESOLVED, ())
        Compatibility.withNewQuery(a, newQuery)
      } else {
        a
      }

    case o @ PaimonDynamicPartitionOverwrite(r, d) if o.resolved =>
      PaimonDynamicPartitionOverwriteCommand(r, d, o.query, o.writeOptions, o.isByName)

    case merge: MergeIntoTable
        if !merge.resolved && isPaimonTable(merge.targetTable) && merge.childrenResolved =>
      PaimonMergeIntoResolver(merge, session)

    case s @ ShowColumns(PaimonRelation(table), _, _) if s.resolved =>
      PaimonShowColumnsCommand(table)

    case d @ PaimonDropPartitions(ResolvedTable(_, _, table: SparkTable, _), parts, _, _)
        if d.resolved =>
      PaimonDropPartitions.validate(table, parts.asResolvedPartitionSpecs)
      d
  }

  private def writeOptions(v2WriteCommand: V2WriteCommand): Map[String, String] = {
    v2WriteCommand match {
      case a: AppendData => a.writeOptions
      case o: OverwriteByExpression => o.writeOptions
      case op: OverwritePartitionsDynamic => op.writeOptions
      case _ => Map.empty[String, String]
    }
  }

  // Mirrors Spark's V2WriteCommand `outputResolved` strictness: query and table outputs must match
  // by name, position, type (ignoring nullable compatibility), and nullability. Any nested
  // structural differences also have to be reconciled before we declare the write resolved.
  private def paimonWriteResolved(query: LogicalPlan, table: NamedRelation): Boolean = {
    query.output.size == table.output.size &&
    query.output.zip(table.output).forall {
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
    plan match {
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

      case _ => plan
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
