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

package org.apache.paimon.codegen

import org.apache.paimon.codegen.GeneratedExpression.{NEVER_NULL, NO_CODE}
import org.apache.paimon.codegen.GenerateUtils.{generateRecordStatement, rowSetField, DEFAULT_OUT_RECORD_TERM, DEFAULT_OUT_RECORD_WRITER_TERM}
import org.apache.paimon.data.{BinaryRow, InternalRow}
import org.apache.paimon.types.{RowType, TimestampType}
import org.apache.paimon.utils.TypeUtils.isInteroperable

class ExprCodeGenerator(ctx: CodeGeneratorContext) {

  /**
   * Generates an expression from a sequence of other expressions. The evaluation result may be
   * stored in the variable outRecordTerm.
   *
   * @param fieldExprs
   *   field expressions to be converted
   * @param returnType
   *   conversion target type. Type must have the same arity than fieldExprs.
   * @param outRow
   *   the result term
   * @param outRowWriter
   *   the result writer term for BinaryRowData.
   * @param reusedOutRow
   *   If objects or variables can be reused, they will be added to reusable code sections
   *   internally.
   * @param outRowAlreadyExists
   *   Don't need addReusableRecord if out row already exists.
   * @return
   *   instance of GeneratedExpression
   */
  def generateResultExpression(
      fieldExprs: Seq[GeneratedExpression],
      returnType: RowType,
      returnTypeClazz: Class[_ <: InternalRow],
      outRow: String = DEFAULT_OUT_RECORD_TERM,
      outRowWriter: Option[String] = Some(DEFAULT_OUT_RECORD_WRITER_TERM),
      reusedOutRow: Boolean = true,
      outRowAlreadyExists: Boolean = false): GeneratedExpression = {
    val fieldExprIdxToOutputRowPosMap = fieldExprs.indices.map(i => i -> i).toMap
    generateResultExpression(
      fieldExprs,
      fieldExprIdxToOutputRowPosMap,
      returnType,
      returnTypeClazz,
      outRow,
      outRowWriter,
      reusedOutRow,
      outRowAlreadyExists)
  }

  /**
   * Generates an expression from a sequence of other expressions. The evaluation result may be
   * stored in the variable outRecordTerm.
   *
   * @param fieldExprs
   *   field expressions to be converted
   * @param fieldExprIdxToOutputRowPosMap
   *   Mapping index of fieldExpr in `fieldExprs` to position of output row.
   * @param returnType
   *   conversion target type. Type must have the same arity than fieldExprs.
   * @param outRow
   *   the result term
   * @param outRowWriter
   *   the result writer term for BinaryRowData.
   * @param reusedOutRow
   *   If objects or variables can be reused, they will be added to reusable code sections
   *   internally.
   * @param outRowAlreadyExists
   *   Don't need addReusableRecord if out row already exists.
   * @return
   *   instance of GeneratedExpression
   */
  def generateResultExpression(
      fieldExprs: Seq[GeneratedExpression],
      fieldExprIdxToOutputRowPosMap: Map[Int, Int],
      returnType: RowType,
      returnTypeClazz: Class[_ <: InternalRow],
      outRow: String,
      outRowWriter: Option[String],
      reusedOutRow: Boolean,
      outRowAlreadyExists: Boolean): GeneratedExpression = {
    // initial type check
    if (returnType.getFieldCount != fieldExprs.length) {
      throw new CodeGenException(
        s"Arity [${returnType.getFieldCount}] of result type [$returnType] does not match " +
          s"number [${fieldExprs.length}] of expressions [$fieldExprs].")
    }
    if (fieldExprIdxToOutputRowPosMap.size != fieldExprs.length) {
      throw new CodeGenException(
        s"Size [${returnType.getFieldCount}] of fieldExprIdxToOutputRowPosMap does not match " +
          s"number [${fieldExprs.length}] of expressions [$fieldExprs].")
    }
    // type check
    fieldExprs.zipWithIndex.foreach {
      // timestamp type(Include TimeIndicator) and generic type can compatible with each other.
      case (fieldExpr, i) if fieldExpr.resultType.isInstanceOf[TimestampType] =>
        if (returnType.getTypeAt(i).getClass != fieldExpr.resultType.getClass) {
          throw new CodeGenException(
            s"Incompatible types of expression and result type, Expression[$fieldExpr] type is " +
              s"[${fieldExpr.resultType}], result type is [${returnType.getTypeAt(i)}]")
        }
      case (fieldExpr, i) if !isInteroperable(fieldExpr.resultType, returnType.getTypeAt(i)) =>
        throw new CodeGenException(
          s"Incompatible types of expression and result type. Expression[$fieldExpr] type is " +
            s"[${fieldExpr.resultType}], result type is [${returnType.getTypeAt(i)}]")
      case _ => // ok
    }

    val setFieldsCode = fieldExprs.zipWithIndex
      .map {
        case (fieldExpr, index) =>
          val pos = fieldExprIdxToOutputRowPosMap.getOrElse(
            index,
            throw new CodeGenException(s"Illegal field expr index: $index"))
          rowSetField(ctx, returnTypeClazz, outRow, pos.toString, fieldExpr, outRowWriter)
      }
      .mkString("\n")

    val outRowInitCode = if (!outRowAlreadyExists) {
      val initCode = generateRecordStatement(returnType, returnTypeClazz, outRow, outRowWriter, ctx)
      if (reusedOutRow) {
        NO_CODE
      } else {
        initCode
      }
    } else {
      NO_CODE
    }

    val code = if (returnTypeClazz == classOf[BinaryRow] && outRowWriter.isDefined) {
      val writer = outRowWriter.get
      val resetWriter = s"$writer.reset();"
      val completeWriter: String = s"$writer.complete();"
      s"""
         |$outRowInitCode
         |$resetWriter
         |$setFieldsCode
         |$completeWriter
        """.stripMargin
    } else {
      s"""
         |$outRowInitCode
         |$setFieldsCode
        """.stripMargin
    }
    GeneratedExpression(outRow, NEVER_NULL, code, returnType)
  }
}
