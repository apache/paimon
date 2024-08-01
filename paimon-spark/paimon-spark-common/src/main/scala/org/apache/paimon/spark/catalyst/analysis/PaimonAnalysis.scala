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

import org.apache.paimon.spark.SparkTable
import org.apache.paimon.spark.catalyst.Compatibility
import org.apache.paimon.spark.catalyst.analysis.PaimonRelation.isPaimonTable
import org.apache.paimon.spark.commands.{PaimonAnalyzeTableColumnCommand, PaimonDynamicPartitionOverwriteCommand, PaimonTruncateTableCommand}
import org.apache.paimon.table.FileStoreTable

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.ResolvedTable
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, Expression, NamedExpression}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.types.{ArrayType, DataType, MapType, StructType}

import scala.collection.JavaConverters._

class PaimonAnalysis(session: SparkSession) extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsDown {

    case a @ PaimonV2WriteCommand(table, paimonTable)
        if a.isByName && needsSchemaAdjustmentByName(a.query, table.output, paimonTable) =>
      val newQuery = resolveQueryColumnsByName(a.query, table.output)
      if (newQuery != a.query) {
        Compatibility.withNewQuery(a, newQuery)
      } else {
        a
      }

    case a @ PaimonV2WriteCommand(table, paimonTable)
        if !a.isByName && needsSchemaAdjustmentByPosition(a.query, table.output, paimonTable) =>
      val newQuery = resolveQueryColumnsByPosition(a.query, table.output)
      if (newQuery != a.query) {
        Compatibility.withNewQuery(a, newQuery)
      } else {
        a
      }

    case o @ PaimonDynamicPartitionOverwrite(r, d) if o.resolved =>
      PaimonDynamicPartitionOverwriteCommand(r, d, o.query, o.writeOptions, o.isByName)

    case merge: MergeIntoTable if isPaimonTable(merge.targetTable) && merge.childrenResolved =>
      PaimonMergeIntoResolver(merge, session)
  }

  private def needsSchemaAdjustmentByName(
      query: LogicalPlan,
      targetAttrs: Seq[Attribute],
      paimonTable: FileStoreTable): Boolean = {
    val userSpecifiedNames = if (session.sessionState.conf.caseSensitiveAnalysis) {
      query.output.map(a => (a.name, a)).toMap
    } else {
      CaseInsensitiveMap(query.output.map(a => (a.name, a)).toMap)
    }
    val specifiedTargetAttrs = targetAttrs.filter(col => userSpecifiedNames.contains(col.name))
    !schemaCompatible(
      specifiedTargetAttrs.toStructType,
      query.output.toStructType,
      paimonTable.partitionKeys().asScala)
  }

  private def resolveQueryColumnsByName(
      query: LogicalPlan,
      targetAttrs: Seq[Attribute]): LogicalPlan = {
    val output = query.output
    val project = targetAttrs.map {
      attr =>
        val outputAttr = output
          .find(t => session.sessionState.conf.resolver(t.name, attr.name))
          .getOrElse {
            throw new RuntimeException("xxx")
          }
        addCastToColumn(outputAttr, attr)
    }
    Project(project, query)
  }

  private def needsSchemaAdjustmentByPosition(
      query: LogicalPlan,
      targetAttrs: Seq[Attribute],
      paimonTable: FileStoreTable): Boolean = {
    val output = query.output
    targetAttrs.map(_.name) != output.map(_.name) ||
    !schemaCompatible(
      targetAttrs.toStructType,
      output.toStructType,
      paimonTable.partitionKeys().asScala)
  }

  private def resolveQueryColumnsByPosition(
      query: LogicalPlan,
      tableAttributes: Seq[Attribute]): LogicalPlan = {
    val project = query.output.zipWithIndex.map {
      case (attr, i) =>
        val targetAttr = tableAttributes(i)
        addCastToColumn(attr, targetAttr)
    }
    Project(project, query)
  }

  private def schemaCompatible(
      dataSchema: StructType,
      tableSchema: StructType,
      partitionCols: Seq[String],
      parent: Array[String] = Array.empty): Boolean = {

    if (tableSchema.size != dataSchema.size) {
      throw new RuntimeException("the number of data columns don't match with the table schema's.")
    }

    def dataTypeCompatible(column: String, dt1: DataType, dt2: DataType): Boolean = {
      (dt1, dt2) match {
        case (s1: StructType, s2: StructType) =>
          schemaCompatible(s1, s2, partitionCols, Array(column))
        case (a1: ArrayType, a2: ArrayType) =>
          dataTypeCompatible(column, a1.elementType, a2.elementType)
        case (m1: MapType, m2: MapType) =>
          dataTypeCompatible(column, m1.keyType, m2.keyType) && dataTypeCompatible(
            column,
            m1.valueType,
            m2.valueType)
        case (d1, d2) => d1 == d2
      }
    }

    dataSchema.zip(tableSchema).forall {
      case (f1, f2) => dataTypeCompatible(f1.name, f1.dataType, f2.dataType)
    }
  }

  private def addCastToColumn(attr: Attribute, targetAttr: Attribute): NamedExpression = {
    val expr = (attr.dataType, targetAttr.dataType) match {
      case (s, t) if s == t =>
        attr
      case _ =>
        cast(attr, targetAttr.dataType)
    }
    Alias(expr, targetAttr.name)(explicitMetadata = Option(targetAttr.metadata))
  }

  private def cast(expr: Expression, dataType: DataType): Expression = {
    val cast = Compatibility.cast(expr, dataType, Option(conf.sessionLocalTimeZone))
    cast.setTagValue(Compatibility.castByTableInsertionTag, ())
    cast
  }
}

case class PaimonPostHocResolutionRules(session: SparkSession) extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = {
    plan match {
      case t @ TruncateTable(PaimonRelation(table)) if t.resolved =>
        PaimonTruncateTableCommand(table, Map.empty)

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
  def unapply(o: V2WriteCommand): Option[(DataSourceV2Relation, FileStoreTable)] = {
    if (o.query.resolved) {
      o.table match {
        case r: DataSourceV2Relation if r.table.isInstanceOf[SparkTable] =>
          Some((r, r.table.asInstanceOf[SparkTable].getTable.asInstanceOf[FileStoreTable]))
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
      o.table match {
        case r: DataSourceV2Relation if r.table.isInstanceOf[SparkTable] =>
          Some((r, r.table.asInstanceOf[SparkTable].getTable.asInstanceOf[FileStoreTable]))
        case _ => None
      }
    } else {
      None
    }
  }
}
