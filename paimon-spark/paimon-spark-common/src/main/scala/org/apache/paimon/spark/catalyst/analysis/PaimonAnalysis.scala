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
import org.apache.paimon.spark.commands.{PaimonAnalyzeTableColumnCommand, PaimonDynamicPartitionOverwriteCommand, PaimonShowColumnsCommand, PaimonTruncateTableCommand}
import org.apache.paimon.table.FileStoreTable

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.{NamedRelation, ResolvedTable}
import org.apache.spark.sql.catalyst.expressions.{Alias, ArrayTransform, Attribute, CreateStruct, Expression, GetArrayItem, GetStructField, LambdaFunction, Literal, NamedExpression, NamedLambdaVariable}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.util.CharVarcharUtils
import org.apache.spark.sql.catalyst.util.TypeUtils.toSQLId
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.types.{ArrayType, DataType, IntegerType, MapType, Metadata, StructField, StructType}

import scala.collection.mutable

class PaimonAnalysis(session: SparkSession) extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsDown {
    case a @ PaimonV2WriteCommand(table) if !paimonWriteResolved(a.query, table) =>
      val newQuery = resolveQueryColumns(a.query, table, a.isByName)
      if (newQuery != a.query) {
        Compatibility.withNewQuery(a, newQuery)
      } else {
        a
      }

    case o @ PaimonDynamicPartitionOverwrite(r, d) if o.resolved =>
      PaimonDynamicPartitionOverwriteCommand(r, d, o.query, o.writeOptions, o.isByName)

    case merge: MergeIntoTable if isPaimonTable(merge.targetTable) && merge.childrenResolved =>
      PaimonMergeIntoResolver(merge, session)

    case s @ ShowColumns(PaimonRelation(table), _, _) if s.resolved =>
      PaimonShowColumnsCommand(table)
  }

  private def paimonWriteResolved(query: LogicalPlan, table: NamedRelation): Boolean = {
    query.output.size == table.output.size &&
    query.output.zip(table.output).forall {
      case (inAttr, outAttr) =>
        val inType = CharVarcharUtils.getRawType(inAttr.metadata).getOrElse(inAttr.dataType)
        val outType = CharVarcharUtils.getRawType(outAttr.metadata).getOrElse(outAttr.dataType)
        inAttr.name == outAttr.name && schemaCompatible(inType, outType)
    }
  }

  private def resolveQueryColumns(
      query: LogicalPlan,
      table: NamedRelation,
      byName: Boolean): LogicalPlan = {
    // More details see: `TableOutputResolver#resolveOutputColumns`
    if (byName) {
      resolveQueryColumnsByName(query, table)
    } else {
      resolveQueryColumnsByPosition(query, table)
    }
  }

  private def resolveQueryColumnsByName(query: LogicalPlan, table: NamedRelation): LogicalPlan = {
    val inputCols = query.output
    val expectedCols = table.output
    if (inputCols.size > expectedCols.size) {
      throw new RuntimeException(
        s"Cannot write incompatible data for the table `${table.name}`, " +
          "the number of data columns don't match with the table schema's.")
    }

    val matchedCols = mutable.HashSet.empty[String]
    val reorderedCols = expectedCols.map {
      expectedCol =>
        val matched = inputCols.filter(col => conf.resolver(col.name, expectedCol.name))
        if (matched.isEmpty) {
          // TODO: Support Spark default value framework if Paimon supports to change default values.
          if (!expectedCol.nullable) {
            throw new RuntimeException(
              s"Cannot write incompatible data for the table `${table.name}`, " +
                s"due to non-nullable column `${expectedCol.name}` has no specified value.")
          }
          Alias(Literal(null, expectedCol.dataType), expectedCol.name)()
        } else if (matched.length > 1) {
          throw new RuntimeException(
            s"Cannot write incompatible data for the table `${table.name}`, due to column name conflicts: ${matched
                .mkString(", ")}.")
        } else {
          matchedCols += matched.head.name
          val matchedCol = matched.head
          addCastToColumn(matchedCol, expectedCol, isByName = true)
        }
    }

    assert(reorderedCols.length == expectedCols.length)
    if (matchedCols.size < inputCols.length) {
      val extraCols = inputCols
        .filterNot(col => matchedCols.contains(col.name))
        .map(col => s"${toSQLId(col.name)}")
        .mkString(", ")
      // There are seme unknown column names
      throw new RuntimeException(
        s"Cannot write incompatible data for the table `${table.name}`, due to unknown column names: ${extraCols
            .mkString(", ")}.")
    }
    Project(reorderedCols, query)
  }

  private def resolveQueryColumnsByPosition(
      query: LogicalPlan,
      table: NamedRelation): LogicalPlan = {
    val expectedCols = table.output
    val queryCols = query.output
    if (queryCols.size != expectedCols.size) {
      throw new RuntimeException(
        s"Cannot write incompatible data for the table `${table.name}`, " +
          "the number of data columns don't match with the table schema's.")
    }

    val project = queryCols.zipWithIndex.map {
      case (attr, i) =>
        val targetAttr = expectedCols(i)
        addCastToColumn(attr, targetAttr, isByName = false)
    }
    Project(project, query)
  }

  private def schemaCompatible(dataSchema: DataType, tableSchema: DataType): Boolean = {
    (dataSchema, tableSchema) match {
      case (s1: StructType, s2: StructType) =>
        s1.zip(s2).forall { case (d1, d2) => schemaCompatible(d1.dataType, d2.dataType) }
      case (a1: ArrayType, a2: ArrayType) =>
        a1.containsNull == a2.containsNull && schemaCompatible(a1.elementType, a2.elementType)
      case (m1: MapType, m2: MapType) =>
        m1.valueContainsNull == m2.valueContainsNull &&
        schemaCompatible(m1.keyType, m2.keyType) &&
        schemaCompatible(m1.valueType, m2.valueType)
      case (d1, d2) => d1 == d2
    }
  }

  private def addCastToColumn(
      attr: Attribute,
      targetAttr: Attribute,
      isByName: Boolean): NamedExpression = {
    val expr = (attr.dataType, targetAttr.dataType) match {
      case (s, t) if s == t =>
        attr
      case (s: StructType, t: StructType) if s != t =>
        if (isByName) {
          addCastToStructByName(attr, s, t)
        } else {
          addCastToStructByPosition(attr, s, t)
        }
      case (ArrayType(s: StructType, sNull: Boolean), ArrayType(t: StructType, _: Boolean))
          if s != t =>
        val castToStructFunc = if (isByName) {
          addCastToStructByName _
        } else {
          addCastToStructByPosition _
        }
        castToArrayStruct(attr, s, t, sNull, castToStructFunc)
      case _ =>
        cast(attr, targetAttr.dataType)
    }
    Alias(withStrLenCheck(expr, targetAttr.metadata), targetAttr.name)(explicitMetadata =
      Option(targetAttr.metadata))
  }

  private def addCastToStructByName(
      parent: NamedExpression,
      source: StructType,
      target: StructType): NamedExpression = {
    val fields = target.map {
      case targetField @ StructField(name, nested: StructType, _, _) =>
        val sourceIndex = source.fieldIndex(name)
        val sourceField = source(sourceIndex)
        sourceField.dataType match {
          case s: StructType =>
            val subField = castStructField(parent, sourceIndex, sourceField.name, targetField)
            addCastToStructByName(subField, s, nested)
          case o =>
            throw new RuntimeException(s"Can not support to cast $o to StructType.")
        }
      case targetField =>
        val sourceIndex = source.fieldIndex(targetField.name)
        val sourceField = source(sourceIndex)
        castStructField(parent, sourceIndex, sourceField.name, targetField)
    }
    Alias(CreateStruct(fields), parent.name)(
      parent.exprId,
      parent.qualifier,
      Option(parent.metadata))
  }

  private def addCastToStructByPosition(
      parent: NamedExpression,
      source: StructType,
      target: StructType): NamedExpression = {
    if (source.length != target.length) {
      throw new RuntimeException("The number of fields in source and target is not same.")
    }

    val fields = target.zipWithIndex.map {
      case (targetField @ StructField(_, nested: StructType, _, _), i) =>
        val sourceField = source(i)
        sourceField.dataType match {
          case s: StructType =>
            val subField = castStructField(parent, i, sourceField.name, targetField)
            addCastToStructByPosition(subField, s, nested)
          case o =>
            throw new RuntimeException(s"Can not support to cast $o to StructType.")
        }
      case (targetField, i) =>
        val sourceField = source(i)
        castStructField(parent, i, sourceField.name, targetField)
    }
    Alias(CreateStruct(fields), parent.name)(
      parent.exprId,
      parent.qualifier,
      Option(parent.metadata))
  }

  private def castStructField(
      parent: NamedExpression,
      i: Int,
      sourceFieldName: String,
      targetField: StructField): NamedExpression = {
    Alias(
      withStrLenCheck(
        cast(GetStructField(parent, i, Option(sourceFieldName)), targetField.dataType),
        targetField.metadata),
      targetField.name)(explicitMetadata = Option(targetField.metadata))
  }

  private def castToArrayStruct(
      parent: NamedExpression,
      source: StructType,
      target: StructType,
      sourceNullable: Boolean,
      castToStructFunc: (NamedExpression, StructType, StructType) => NamedExpression
  ): Expression = {
    val structConverter: (Expression, Expression) => Expression = (_, i) =>
      castToStructFunc(Alias(GetArrayItem(parent, i), i.toString)(), source, target)
    val transformLambdaFunc = {
      val elementVar = NamedLambdaVariable("elementVar", source, sourceNullable)
      val indexVar = NamedLambdaVariable("indexVar", IntegerType, false)
      LambdaFunction(structConverter(elementVar, indexVar), Seq(elementVar, indexVar))
    }
    ArrayTransform(parent, transformLambdaFunc)
  }

  private def cast(expr: Expression, dataType: DataType): Expression = {
    val cast = Compatibility.cast(expr, dataType, Option(conf.sessionLocalTimeZone))
    cast.setTagValue(Compatibility.castByTableInsertionTag, ())
    cast
  }

  private def withStrLenCheck(expr: Expression, metadata: Metadata): Expression = {
    if (!conf.charVarcharAsString) {
      CharVarcharUtils
        .getRawType(metadata)
        .map(rawType => CharVarcharUtils.stringLengthCheck(expr, rawType))
        .getOrElse(expr)
    } else {
      expr
    }
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
