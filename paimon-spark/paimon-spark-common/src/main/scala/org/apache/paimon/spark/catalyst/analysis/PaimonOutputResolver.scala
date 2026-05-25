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

import org.apache.paimon.spark.catalyst.Compatibility

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.objects.AssertNotNull
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project}
import org.apache.spark.sql.catalyst.util.CharVarcharUtils
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

import scala.collection.mutable

/**
 * Trimmed fork of Spark's `TableOutputResolver` with `mergeSchemaEnabled` for schema evolution.
 *
 *   - Missing target fields: NULL-fill at any depth.
 *   - Extra source fields: throw when `mergeSchemaEnabled = false`; otherwise kept in the
 *     projection at any depth so `SchemaHelper.mergeSchema` evolves the table at write time. Safe
 *     because `PaimonSparkTableBase` advertises `ACCEPT_ANY_SCHEMA`, short-circuiting Spark's
 *     `outputResolved` check.
 *   - Nullable downcast: wrap with `AssertNotNull`.
 *   - Type mismatch: wrap with `Compatibility.cast` tagged for downstream explicit-cast check.
 */
object PaimonOutputResolver {

  def resolveOutputColumns(
      tableName: String,
      expected: Seq[Attribute],
      query: LogicalPlan,
      byName: Boolean,
      conf: SQLConf,
      mergeSchemaEnabled: Boolean): LogicalPlan = {
    val resolved: Seq[NamedExpression] = if (byName) {
      reorderColumnsByName(tableName, query.output, expected, conf, mergeSchemaEnabled, Nil)
    } else {
      resolveColumnsByPosition(tableName, query.output, expected, conf, Nil)
    }
    if (resolved == query.output) {
      query
    } else {
      Project(resolved, query)
    }
  }

  private def reorderColumnsByName(
      tableName: String,
      inputCols: Seq[NamedExpression],
      expectedCols: Seq[Attribute],
      conf: SQLConf,
      mergeSchemaEnabled: Boolean,
      colPath: Seq[String]): Seq[NamedExpression] = {
    val isTopLevel = colPath.isEmpty
    val matchedNames = mutable.HashSet.empty[String]
    val reordered = expectedCols.map {
      expectedCol =>
        val matches = inputCols.filter(col => conf.resolver(col.name, expectedCol.name))
        val newColPath = colPath :+ expectedCol.name
        if (matches.isEmpty) {
          nullFill(expectedCol)
        } else if (matches.length > 1) {
          throw new RuntimeException(
            s"Cannot write to `$tableName`, " +
              s"due to ambiguous column name `${newColPath.mkString(".")}`.")
        } else {
          matchedNames += matches.head.name
          val actualExpectedCol = expectedCol.withDataType {
            CharVarcharUtils.getRawType(expectedCol.metadata).getOrElse(expectedCol.dataType)
          }
          resolveField(
            tableName,
            matches.head,
            actualExpectedCol,
            byName = true,
            conf,
            mergeSchemaEnabled,
            newColPath)
        }
    }

    if (matchedNames.size < inputCols.length) {
      val extras = inputCols.filterNot(col => matchedNames.contains(col.name))
      if (!mergeSchemaEnabled) {
        val extrasStr = extras.map(c => s"`${c.name}`").mkString(", ")
        val msg = if (isTopLevel) {
          s"Cannot write to `$tableName`, extra columns: $extrasStr"
        } else {
          s"Cannot write to `$tableName`, " +
            s"extra struct fields at `${colPath.mkString(".")}`: $extrasStr"
        }
        throw new RuntimeException(msg)
      }
      reordered ++ extras.map(preserveAsAlias)
    } else {
      reordered
    }
  }

  private def resolveColumnsByPosition(
      tableName: String,
      inputCols: Seq[NamedExpression],
      expectedCols: Seq[Attribute],
      conf: SQLConf,
      colPath: Seq[String]): Seq[NamedExpression] = {
    val actualExpectedCols = expectedCols.map {
      attr =>
        attr.withDataType {
          CharVarcharUtils.getRawType(attr.metadata).getOrElse(attr.dataType)
        }
    }
    if (inputCols.size != actualExpectedCols.size) {
      val where = if (colPath.isEmpty) {
        s"`$tableName`"
      } else {
        s"`$tableName` at `${colPath.mkString(".")}`"
      }
      throw new RuntimeException(s"Cannot write to $where, " +
        s"the number of data columns (${inputCols.size}) doesn't match the table schema's (${actualExpectedCols.size}).")
    }
    inputCols.zip(actualExpectedCols).map {
      case (inputCol, expectedCol) =>
        resolveField(
          tableName,
          inputCol,
          expectedCol,
          byName = false,
          conf,
          mergeSchemaEnabled = false,
          colPath :+ expectedCol.name)
    }
  }

  private def resolveField(
      tableName: String,
      input: NamedExpression,
      expected: Attribute,
      byName: Boolean,
      conf: SQLConf,
      mergeSchemaEnabled: Boolean,
      colPath: Seq[String]): NamedExpression = {
    (input.dataType, expected.dataType) match {
      case (sourceType: StructType, targetType: StructType) =>
        resolveStructType(
          tableName,
          input,
          sourceType,
          expected,
          targetType,
          byName,
          conf,
          mergeSchemaEnabled,
          colPath)
      case (sourceType: ArrayType, targetType: ArrayType) =>
        resolveArrayType(
          tableName,
          input,
          sourceType,
          expected,
          targetType,
          byName,
          conf,
          mergeSchemaEnabled,
          colPath)
      case (sourceType: MapType, targetType: MapType) =>
        resolveMapType(
          tableName,
          input,
          sourceType,
          expected,
          targetType,
          byName,
          conf,
          mergeSchemaEnabled,
          colPath)
      case _ =>
        checkField(input, expected, conf, colPath)
    }
  }

  private def resolveStructType(
      tableName: String,
      input: Expression,
      sourceType: StructType,
      expected: Attribute,
      targetType: StructType,
      byName: Boolean,
      conf: SQLConf,
      mergeSchemaEnabled: Boolean,
      colPath: Seq[String]): NamedExpression = {
    val nullCheckedInput = checkNullability(input, expected, colPath)
    val fields = sourceType.zipWithIndex.map {
      case (f, i) =>
        Alias(GetStructField(nullCheckedInput, i, Option(f.name)), f.name)()
    }
    val resolved = if (byName) {
      reorderColumnsByName(
        tableName,
        fields,
        toAttributes(targetType),
        conf,
        mergeSchemaEnabled,
        colPath)
    } else {
      resolveColumnsByPosition(tableName, fields, toAttributes(targetType), conf, colPath)
    }
    val struct = CreateStruct(resolved)
    val res = if (nullCheckedInput.nullable) {
      If(IsNull(nullCheckedInput), Literal(null, struct.dataType), struct)
    } else {
      struct
    }
    Alias(res, expected.name)(explicitMetadata = Option(expected.metadata))
  }

  private def resolveArrayType(
      tableName: String,
      input: Expression,
      sourceType: ArrayType,
      expected: Attribute,
      targetType: ArrayType,
      byName: Boolean,
      conf: SQLConf,
      mergeSchemaEnabled: Boolean,
      colPath: Seq[String]): NamedExpression = {
    val nullCheckedInput = checkNullability(input, expected, colPath)
    val param = NamedLambdaVariable("element", sourceType.elementType, sourceType.containsNull)
    val fakeAttr =
      AttributeReference("element", targetType.elementType, targetType.containsNull)()
    val resolved = if (byName) {
      reorderColumnsByName(tableName, Seq(param), Seq(fakeAttr), conf, mergeSchemaEnabled, colPath)
    } else {
      resolveColumnsByPosition(tableName, Seq(param), Seq(fakeAttr), conf, colPath)
    }
    assert(resolved.length == 1)
    val elementExpr = stripOuterAlias(resolved.head)
    val transformed = if (elementExpr.fastEquals(param)) {
      nullCheckedInput
    } else {
      ArrayTransform(nullCheckedInput, LambdaFunction(elementExpr, Seq(param)))
    }
    Alias(transformed, expected.name)(explicitMetadata = Option(expected.metadata))
  }

  private def resolveMapType(
      tableName: String,
      input: Expression,
      sourceType: MapType,
      expected: Attribute,
      targetType: MapType,
      byName: Boolean,
      conf: SQLConf,
      mergeSchemaEnabled: Boolean,
      colPath: Seq[String]): NamedExpression = {
    val nullCheckedInput = checkNullability(input, expected, colPath)
    val keyParam = NamedLambdaVariable("key", sourceType.keyType, nullable = false)
    val fakeKeyAttr = AttributeReference("key", targetType.keyType, nullable = false)()
    val resolvedKey = if (byName) {
      reorderColumnsByName(
        tableName,
        Seq(keyParam),
        Seq(fakeKeyAttr),
        conf,
        mergeSchemaEnabled,
        colPath)
    } else {
      resolveColumnsByPosition(tableName, Seq(keyParam), Seq(fakeKeyAttr), conf, colPath)
    }

    val valueParam =
      NamedLambdaVariable("value", sourceType.valueType, sourceType.valueContainsNull)
    val fakeValueAttr =
      AttributeReference("value", targetType.valueType, targetType.valueContainsNull)()
    val resolvedValue = if (byName) {
      reorderColumnsByName(
        tableName,
        Seq(valueParam),
        Seq(fakeValueAttr),
        conf,
        mergeSchemaEnabled,
        colPath)
    } else {
      resolveColumnsByPosition(tableName, Seq(valueParam), Seq(fakeValueAttr), conf, colPath)
    }

    assert(resolvedKey.length == 1 && resolvedValue.length == 1)
    val keyExpr = stripOuterAlias(resolvedKey.head)
    val valueExpr = stripOuterAlias(resolvedValue.head)
    val transformed = if (keyExpr.fastEquals(keyParam) && valueExpr.fastEquals(valueParam)) {
      nullCheckedInput
    } else {
      val newKeys = if (keyExpr.fastEquals(keyParam)) {
        MapKeys(nullCheckedInput)
      } else {
        ArrayTransform(MapKeys(nullCheckedInput), LambdaFunction(keyExpr, Seq(keyParam)))
      }
      val newValues = if (valueExpr.fastEquals(valueParam)) {
        MapValues(nullCheckedInput)
      } else {
        ArrayTransform(MapValues(nullCheckedInput), LambdaFunction(valueExpr, Seq(valueParam)))
      }
      MapFromArrays(newKeys, newValues)
    }
    Alias(transformed, expected.name)(explicitMetadata = Option(expected.metadata))
  }

  private def checkField(
      input: NamedExpression,
      expected: Attribute,
      conf: SQLConf,
      colPath: Seq[String]): NamedExpression = {
    val attrTypeHasCharVarchar = CharVarcharUtils.hasCharVarchar(expected.dataType)
    val attrTypeWithoutCharVarchar = if (attrTypeHasCharVarchar) {
      CharVarcharUtils.replaceCharVarcharWithString(expected.dataType)
    } else {
      expected.dataType
    }
    val casted = if (input.dataType == attrTypeWithoutCharVarchar) {
      input
    } else {
      addCast(input, attrTypeWithoutCharVarchar, conf)
    }
    val withStrLenCheck = if (conf.charVarcharAsString || !attrTypeHasCharVarchar) {
      casted
    } else {
      CharVarcharUtils.stringLengthCheck(casted, expected.dataType)
    }
    val nullChecked = checkNullability(withStrLenCheck, expected, colPath)
    Alias(nullChecked, expected.name)(explicitMetadata = Option(expected.metadata))
  }

  private def checkNullability(
      input: Expression,
      expected: Attribute,
      colPath: Seq[String]): Expression = {
    if (input.nullable && !expected.nullable) {
      AssertNotNull(input, colPath)
    } else {
      input
    }
  }

  private def addCast(expr: Expression, dataType: DataType, conf: SQLConf): Expression = {
    val cast = Compatibility.cast(expr, dataType, Option(conf.sessionLocalTimeZone))
    cast.setTagValue(Compatibility.castByTableInsertionTag, ())
    cast
  }

  private def nullFill(expected: Attribute): NamedExpression = {
    Alias(Literal(null, expected.dataType), expected.name)(
      explicitMetadata = Option(expected.metadata))
  }

  private def preserveAsAlias(expr: NamedExpression): NamedExpression = expr match {
    case a: Alias => a
    case other =>
      Alias(other, other.name)(explicitMetadata = Option(other.metadata))
  }

  private def stripOuterAlias(expr: Expression): Expression = expr match {
    case a: Alias => a.child
    case other => other
  }

  private def toAttributes(structType: StructType): Seq[Attribute] = {
    structType.map(f => AttributeReference(f.name, f.dataType, f.nullable, f.metadata)())
  }
}
