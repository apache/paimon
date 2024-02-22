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
import org.apache.paimon.codegen.GenerateUtils._
import org.apache.paimon.data.{BinaryRow, InternalRow}
import org.apache.paimon.types.RowType

/**
 * CodeGenerator for projection, Take out some fields of [[InternalRow]] to generate a new
 * [[InternalRow]].
 */
object ProjectionCodeGenerator {

  def generateProjectionExpression(
      ctx: CodeGeneratorContext,
      inType: RowType,
      outType: RowType,
      inputMapping: Array[Int],
      outClass: Class[_ <: InternalRow] = classOf[BinaryRow],
      inputTerm: String = DEFAULT_INPUT1_TERM,
      outRecordTerm: String = DEFAULT_OUT_RECORD_TERM,
      outRecordWriterTerm: String = DEFAULT_OUT_RECORD_WRITER_TERM,
      reusedOutRecord: Boolean = true): GeneratedExpression = {
    val exprGenerator = new ExprCodeGenerator(ctx)
    val accessExprs =
      inputMapping.map(idx => GenerateUtils.generateFieldAccess(ctx, inType, inputTerm, idx))
    val expression = exprGenerator.generateResultExpression(
      accessExprs,
      outType,
      outClass,
      outRow = outRecordTerm,
      outRowWriter = Option(outRecordWriterTerm),
      reusedOutRow = reusedOutRecord)

    val outRowInitCode = {
      val initCode =
        generateRecordStatement(outType, outClass, outRecordTerm, Some(outRecordWriterTerm), ctx)
      if (reusedOutRecord) {
        NO_CODE
      } else {
        initCode
      }
    }

    val code =
      s"""
         |$outRowInitCode
         |${expression.code}
        """.stripMargin
    GeneratedExpression(outRecordTerm, NEVER_NULL, code, outType)
  }

  /**
   * CodeGenerator for projection.
   *
   * @param reusedOutRecord
   *   If objects or variables can be reused, they will be added a reusable output record to the
   *   member area of the generated class. If not they will be as temp variables.
   * @return
   */
  def generateProjection(
      ctx: CodeGeneratorContext,
      name: String,
      inType: RowType,
      outType: RowType,
      inputMapping: Array[Int],
      outClass: Class[_ <: InternalRow] = classOf[BinaryRow],
      inputTerm: String = DEFAULT_INPUT1_TERM,
      outRecordTerm: String = DEFAULT_OUT_RECORD_TERM,
      outRecordWriterTerm: String = DEFAULT_OUT_RECORD_WRITER_TERM,
      reusedOutRecord: Boolean = true): GeneratedClass[Projection] = {
    val className = newName(name)
    val baseClass = classOf[Projection]

    val expression = generateProjectionExpression(
      ctx,
      inType,
      outType,
      inputMapping,
      outClass,
      inputTerm,
      outRecordTerm,
      outRecordWriterTerm,
      reusedOutRecord)

    val code =
      s"""
         |public class $className implements ${baseClass.getCanonicalName}<$ROW_DATA, ${outClass.getCanonicalName}> {
         |
         |  ${ctx.reuseMemberCode()}
         |
         |  public $className(Object[] references) throws Exception {
         |    ${ctx.reuseInitCode()}
         |  }
         |
         |  @Override
         |  public ${outClass.getCanonicalName} apply($ROW_DATA $inputTerm) {
         |    ${ctx.reuseLocalVariableCode()}
         |    ${expression.code}
         |    return ${expression.resultTerm};
         |  }
         |}
        """.stripMargin

    new GeneratedClass(className, code, ctx.references.toArray)
  }

  /** For java invoke. */
  def generateProjection(
      ctx: CodeGeneratorContext,
      name: String,
      inputType: RowType,
      outputType: RowType,
      inputMapping: Array[Int]): GeneratedClass[Projection] =
    generateProjection(
      ctx,
      name,
      inputType,
      outputType,
      inputMapping,
      inputTerm = DEFAULT_INPUT1_TERM)
}
