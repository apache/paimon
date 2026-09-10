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

package org.apache.spark.sql.execution.datasources.v2

import org.apache.paimon.spark.util.OptionUtils

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeSet, BasePredicate, BindReferences, Expression, Projection, UnsafeProjection}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, CodeGenerator, ExprCode, FalseLiteral, GeneratePredicate, JavaCode}
import org.apache.spark.sql.catalyst.expressions.codegen.Block.BlockHelper
import org.apache.spark.sql.catalyst.plans.logical.MergeRows._
import org.apache.spark.sql.catalyst.util.truncatedString
import org.apache.spark.sql.execution.{CodegenSupport, SparkPlan, UnaryExecNode}
import org.apache.spark.sql.types.BooleanType
import org.roaringbitmap.longlong.Roaring64Bitmap

import scala.collection.mutable

case class MergeRowsExec(
    isSourceRowPresent: Expression,
    isTargetRowPresent: Expression,
    matchedInstructions: Seq[Instruction],
    notMatchedInstructions: Seq[Instruction],
    notMatchedBySourceInstructions: Seq[Instruction],
    checkCardinality: Boolean,
    output: Seq[Attribute],
    child: SparkPlan)
  extends UnaryExecNode
  with CodegenSupport {

  @transient override lazy val producedAttributes: AttributeSet = {
    AttributeSet(output.filterNot(attr => inputSet.contains(attr)))
  }

  @transient
  override lazy val references: AttributeSet = {
    val usedExprs = if (checkCardinality) {
      val rowIdAttr = child.output.find(attr => conf.resolver(attr.name, ROW_ID))
      assert(rowIdAttr.isDefined, "Cannot find row ID attr")
      rowIdAttr.get +: expressions
    } else {
      expressions
    }
    AttributeSet.fromAttributeSets(usedExprs.map(_.references)) -- producedAttributes
  }

  override def simpleString(maxFields: Int): String = {
    s"MergeRowsExec${truncatedString(output, "[", ", ", "]", maxFields)}"
  }

  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan = {
    copy(child = newChild)
  }

  override protected def doExecute(): RDD[InternalRow] = {
    child.execute().mapPartitions(processPartition)
  }

  override def inputRDDs(): Seq[RDD[InternalRow]] = {
    child.asInstanceOf[CodegenSupport].inputRDDs()
  }

  override def needCopyResult: Boolean = {
    val hasSplitInstruction = (matchedInstructions ++ notMatchedInstructions ++
      notMatchedBySourceInstructions).exists(_.isInstanceOf[Split])
    hasSplitInstruction || child.asInstanceOf[CodegenSupport].needCopyResult
  }

  override def supportCodegen: Boolean = {
    OptionUtils.mergeCodegenEnabled() &&
    conf.wholeStageEnabled &&
    CodeGenerator.isValidParamLength(
      CodeGenerator.calculateParamLength(child.output.filter(usedInputs.contains)))
  }

  override protected def doProduce(ctx: CodegenContext): String = {
    child.asInstanceOf[CodegenSupport].produce(ctx, this)
  }

  override def doConsume(ctx: CodegenContext, input: Seq[ExprCode], row: ExprCode): String = {
    val funcName = ctx.freshName("mergeProcessRow")
    val (args, params, paramExprs) = constructConsumeParameters(ctx, child.output, input)
    val body = generateInstructionExecutionCode(ctx, paramExprs)
    val addedFuncName = ctx.addNewFunction(
      funcName,
      s"""
         |private void $funcName(${params.mkString(", ")}) throws java.io.IOException {
         |  $body
         |}
       """.stripMargin
    )

    s"$addedFuncName(${args.mkString(", ")});"
  }

  private def generateCardinalityValidationCode(
      ctx: CodegenContext,
      rowIdOrdinal: Int,
      input: Seq[ExprCode]): String = {
    val bitmapClass = classOf[Roaring64Bitmap]
    val rowIdBitmap = ctx.addMutableState(
      bitmapClass.getName,
      "matchedRowIds",
      variable => s"$variable = new ${bitmapClass.getName}();")
    val currentRowId = input(rowIdOrdinal)

    code"""
          |${currentRowId.code}
          |if ($rowIdBitmap.contains(${currentRowId.value})) {
          |  throw new RuntimeException("Should not happens");
          |}
          |$rowIdBitmap.add(${currentRowId.value});
     """.stripMargin.toString
  }

  private def generateInstructionExecutionCode(
      ctx: CodegenContext,
      inputExprs: Seq[ExprCode]): String = {
    val sourcePresentExpr = generatePredicateCode(ctx, isSourceRowPresent, child.output, inputExprs)
    val targetPresentExpr = generatePredicateCode(ctx, isTargetRowPresent, child.output, inputExprs)
    val matchedInstructionsCode = generateInstructionsCode(ctx, matchedInstructions, inputExprs)
    val notMatchedInstructionsCode =
      generateInstructionsCode(ctx, notMatchedInstructions, inputExprs)
    val notMatchedBySourceInstructionsCode =
      generateInstructionsCode(ctx, notMatchedBySourceInstructions, inputExprs)
    val cardinalityValidationCode = if (checkCardinality) {
      val rowIdOrdinal = child.output.indexWhere(attr => conf.resolver(attr.name, ROW_ID))
      assert(rowIdOrdinal != -1, "Cannot find row ID attr")
      generateCardinalityValidationCode(ctx, rowIdOrdinal, inputExprs)
    } else {
      ""
    }

    s"""
       |${sourcePresentExpr.code}
       |${targetPresentExpr.code}
       |if (${targetPresentExpr.value} && ${sourcePresentExpr.value}) {
       |  $cardinalityValidationCode
       |  $matchedInstructionsCode
       |} else if (${sourcePresentExpr.value}) {
       |  $notMatchedInstructionsCode
       |} else if (${targetPresentExpr.value}) {
       |  $notMatchedBySourceInstructionsCode
       |}
     """.stripMargin
  }

  private def generateInstructionsCode(
      ctx: CodegenContext,
      instructions: Seq[Instruction],
      inputExprs: Seq[ExprCode]): String = {
    if (instructions.isEmpty) {
      ""
    } else {
      val instructionCodes =
        instructions.map(instruction => generateSingleInstructionCode(ctx, instruction, inputExprs))
      s"""
         |${instructionCodes.mkString("\n")}
         |return;
       """.stripMargin
    }
  }

  private def generateSingleInstructionCode(
      ctx: CodegenContext,
      instruction: Instruction,
      inputExprs: Seq[ExprCode]): String = {
    instruction match {
      case Keep(condition, outputExprs) =>
        val projectionExpr = generateProjectionCode(ctx, outputExprs, inputExprs)
        val predicateExpr = generatePredicateCode(ctx, condition, child.output, inputExprs)
        s"""
           |${predicateExpr.code}
           |if (${predicateExpr.value}) {
           |  ${consume(ctx, projectionExpr)}
           |  return;
           |}
         """.stripMargin

      case Discard(condition) =>
        val predicateExpr = generatePredicateCode(ctx, condition, child.output, inputExprs)
        s"""
           |${predicateExpr.code}
           |if (${predicateExpr.value}) {
           |  return;
           |}
         """.stripMargin

      case Split(condition, outputExprs, otherOutputExprs) =>
        val projectionExpr = generateProjectionCode(ctx, outputExprs, inputExprs)
        val otherProjectionExpr = generateProjectionCode(ctx, otherOutputExprs, inputExprs)
        val predicateExpr = generatePredicateCode(ctx, condition, child.output, inputExprs)
        s"""
           |${predicateExpr.code}
           |if (${predicateExpr.value}) {
           |  ${consume(ctx, projectionExpr)}
           |  ${consume(ctx, otherProjectionExpr)}
           |  return;
           |}
         """.stripMargin

      case other =>
        throw new RuntimeException("Unsupported instruction type: " + other.getClass.getSimpleName)
    }
  }

  private def withCodegenContext[T](ctx: CodegenContext, inputCurrentVars: Seq[ExprCode])(
      block: => T): T = {
    val originalCurrentVars = ctx.currentVars
    val originalInputRow = ctx.INPUT_ROW
    try {
      ctx.currentVars = inputCurrentVars
      block
    } finally {
      ctx.currentVars = originalCurrentVars
      ctx.INPUT_ROW = originalInputRow
    }
  }

  private def generatePredicateCode(
      ctx: CodegenContext,
      predicate: Expression,
      inputAttrs: Seq[Attribute],
      inputCurrentVars: Seq[ExprCode]): ExprCode = {
    withCodegenContext(ctx, inputCurrentVars) {
      val boundPredicate = BindReferences.bindReference(predicate, inputAttrs)
      val evaluatedPredicate = boundPredicate.genCode(ctx)
      val predicateVar = ctx.freshName("predicateResult")
      val code = code"""
                       |${evaluatedPredicate.code}
                       |boolean $predicateVar = !${evaluatedPredicate.isNull} &&
                       |  ${evaluatedPredicate.value};
                     """.stripMargin
      ExprCode(code, FalseLiteral, JavaCode.variable(predicateVar, BooleanType))
    }
  }

  private def generateProjectionCode(
      ctx: CodegenContext,
      outputExprs: Seq[Expression],
      inputCurrentVars: Seq[ExprCode]): Seq[ExprCode] = {
    withCodegenContext(ctx, inputCurrentVars) {
      val boundExprs = outputExprs.map(BindReferences.bindReference(_, child.output))
      boundExprs.map(_.genCode(ctx))
    }
  }

  private def constructConsumeParameters(
      ctx: CodegenContext,
      attributes: Seq[Attribute],
      variables: Seq[ExprCode]): (Seq[String], Seq[String], Seq[ExprCode]) = {
    val arguments = mutable.ArrayBuffer[String]()
    val parameters = mutable.ArrayBuffer[String]()
    val paramVars = mutable.ArrayBuffer(variables: _*)

    variables.zipWithIndex.foreach {
      case (evaluatedVariable, index) =>
        if (usedInputs.contains(attributes(index))) {
          val paramName = ctx.freshName(s"expr_$index")
          val paramType = CodeGenerator.javaType(attributes(index).dataType)
          arguments += evaluatedVariable.value.toString
          parameters += s"$paramType $paramName"
          val paramIsNull = if (!attributes(index).nullable) {
            FalseLiteral
          } else {
            val isNull = ctx.freshName(s"exprIsNull_$index")
            arguments += evaluatedVariable.isNull.toString
            parameters += s"boolean $isNull"
            JavaCode.isNullVariable(isNull)
          }
          paramVars(index) =
            ExprCode(paramIsNull, JavaCode.variable(paramName, attributes(index).dataType))
        }
    }

    (arguments.toSeq, parameters.toSeq, paramVars.toSeq)
  }

  private def processPartition(rowIterator: Iterator[InternalRow]): Iterator[InternalRow] = {
    val isSourceRowPresentPred = createPredicate(isSourceRowPresent)
    val isTargetRowPresentPred = createPredicate(isTargetRowPresent)

    val matchedInstructionExecs = planInstructions(matchedInstructions)
    val notMatchedInstructionExecs = planInstructions(notMatchedInstructions)
    val notMatchedBySourceInstructionExecs = planInstructions(notMatchedBySourceInstructions)

    val cardinalityValidator = if (checkCardinality) {
      val rowIdOrdinal = child.output.indexWhere(attr => conf.resolver(attr.name, ROW_ID))
      assert(rowIdOrdinal != -1, "Cannot find row ID attr")
      BitmapCardinalityValidator(rowIdOrdinal)
    } else {
      NoopCardinalityValidator
    }

    val mergeIterator = new MergeRowIterator(
      rowIterator,
      cardinalityValidator,
      isTargetRowPresentPred,
      isSourceRowPresentPred,
      matchedInstructionExecs,
      notMatchedInstructionExecs,
      notMatchedBySourceInstructionExecs
    )

    // null indicates a record must be discarded
    mergeIterator.filter(_ != null)
  }

  private def createProjection(exprs: Seq[Expression]): UnsafeProjection = {
    UnsafeProjection.create(exprs, child.output)
  }

  private def createPredicate(expr: Expression): BasePredicate = {
    GeneratePredicate.generate(expr, child.output)
  }

  private def planInstructions(instructions: Seq[Instruction]): Seq[InstructionExec] = {
    instructions.map {
      case Keep(cond, output) =>
        KeepExec(createPredicate(cond), createProjection(output))

      case Discard(cond) =>
        DiscardExec(createPredicate(cond))

      case Split(cond, output, otherOutput) =>
        SplitExec(createPredicate(cond), createProjection(output), createProjection(otherOutput))

      case other =>
        throw new RuntimeException("Unsupported instruction type: " + other.getClass.getSimpleName)
    }
  }

  sealed trait InstructionExec {
    def condition: BasePredicate
  }

  case class KeepExec(condition: BasePredicate, projection: Projection) extends InstructionExec {
    def apply(row: InternalRow): InternalRow = projection.apply(row)
  }

  case class DiscardExec(condition: BasePredicate) extends InstructionExec

  case class SplitExec(
      condition: BasePredicate,
      projection: Projection,
      otherProjection: Projection)
    extends InstructionExec {
    def projectRow(row: InternalRow): InternalRow = projection.apply(row)

    def projectExtraRow(row: InternalRow): InternalRow = otherProjection.apply(row)
  }

  sealed trait CardinalityValidator {
    def validate(row: InternalRow): Unit
  }

  object NoopCardinalityValidator extends CardinalityValidator {
    def validate(row: InternalRow): Unit = {}
  }

  /**
   * A simple cardinality validator that keeps track of seen row IDs in a roaring bitmap. This
   * validator assumes the target table is never broadcasted or replicated, which guarantees matches
   * for one target row are always co-located in the same partition.
   *
   * IDs are generated by [[org.apache.spark.sql.catalyst.expressions.MonotonicallyIncreasingID]].
   */
  case class BitmapCardinalityValidator(rowIdOrdinal: Int) extends CardinalityValidator {
    // use Roaring64Bitmap as row IDs generated by MonotonicallyIncreasingID are 64-bit integers
    private val matchedRowIds = new Roaring64Bitmap()

    override def validate(row: InternalRow): Unit = {
      val currentRowId = row.getLong(rowIdOrdinal)
      if (matchedRowIds.contains(currentRowId)) {
        throw new RuntimeException("Should not happens")
      }
      matchedRowIds.add(currentRowId)
    }
  }

  /**
   * An iterator that acts on joined target and source rows and computes deletes, updates and
   * inserts according to provided MERGE instructions.
   *
   * If a particular joined row should be discarded, this iterator returns null.
   */
  class MergeRowIterator(
      private val rowIterator: Iterator[InternalRow],
      private val cardinalityValidator: CardinalityValidator,
      private val isTargetRowPresentPred: BasePredicate,
      private val isSourceRowPresentPred: BasePredicate,
      private val matchedInstructions: Seq[InstructionExec],
      private val notMatchedInstructions: Seq[InstructionExec],
      private val notMatchedBySourceInstructions: Seq[InstructionExec])
    extends Iterator[InternalRow] {

    var cachedExtraRow: InternalRow = _

    override def hasNext: Boolean = cachedExtraRow != null || rowIterator.hasNext

    override def next(): InternalRow = {
      if (cachedExtraRow != null) {
        val extraRow = cachedExtraRow
        cachedExtraRow = null
        return extraRow
      }

      val row = rowIterator.next()

      val isSourceRowPresent = isSourceRowPresentPred.eval(row)
      val isTargetRowPresent = isTargetRowPresentPred.eval(row)

      if (isTargetRowPresent && isSourceRowPresent) {
        cardinalityValidator.validate(row)
        applyInstructions(row, matchedInstructions)
      } else if (isSourceRowPresent) {
        applyInstructions(row, notMatchedInstructions)
      } else if (isTargetRowPresent) {
        applyInstructions(row, notMatchedBySourceInstructions)
      } else {
        null
      }
    }

    private def applyInstructions(
        row: InternalRow,
        instructions: Seq[InstructionExec]): InternalRow = {

      for (instruction <- instructions) {
        if (instruction.condition.eval(row)) {
          instruction match {
            case keep: KeepExec =>
              return keep.apply(row)

            case _: DiscardExec =>
              return null

            case split: SplitExec =>
              cachedExtraRow = split.projectExtraRow(row)
              return split.projectRow(row)
          }
        }
      }

      null
    }
  }
}
