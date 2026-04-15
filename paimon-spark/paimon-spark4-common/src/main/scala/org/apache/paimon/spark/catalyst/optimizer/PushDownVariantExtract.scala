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

package org.apache.paimon.spark.catalyst.optimizer

import org.apache.paimon.data.variant.VariantMetadataUtils
import org.apache.paimon.spark.{PaimonScan, SparkTypeUtils}

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.variant.VariantGet
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation
import org.apache.spark.sql.types.VariantType

import scala.collection.JavaConverters._
import scala.collection.mutable

/**
 * Pushes down [[VariantGet]] extraction into Paimon's shredded Variant reading, enabling Parquet
 * column pruning for shredded Variant columns.
 *
 * When a Variant column is stored in shredded format (typed_value sub-columns per field), and the
 * query only accesses specific fields via variant_get / try_variant_get, this rule replaces the
 * full Variant read with a struct-typed read that only scans the needed typed_value sub-columns.
 *
 * Transforms:
 * {{{
 *   Project [variant_get(v, "$.age", IntegerType)]
 *   └─ DataSourceV2ScanRelation [v: VariantType]
 * }}}
 * Into:
 * {{{
 *   Project [GetStructField(v, 0)]
 *   └─ DataSourceV2ScanRelation [v: StructType<0: IntegerType>]
 *      (PaimonScan with variantProjections = Map("v" -> VariantRowType{0:INT for $.age}))
 * }}}
 *
 * Only applies when:
 *   - The underlying scan is a [[PaimonScan]]
 *   - All usages of the variant column in the projection are through [[VariantGet]] (no direct
 *     attribute reference)
 *   - The path argument of [[VariantGet]] is foldable (constant-evaluable at planning time)
 *
 * Note: the rule does not check whether the column is physically shredded. If it is not, the
 * format layer falls back to extracting fields from the binary blob, so correctness is preserved
 * — just without the IO benefit.
 */
object PushDownVariantExtract extends Rule[LogicalPlan] {

  /** Deduplication key for a single VariantGet operation. */
  private case class GetKey(
      path: String,
      targetType: org.apache.spark.sql.types.DataType,
      failOnError: Boolean,
      timeZoneId: String)

  override def apply(plan: LogicalPlan): LogicalPlan = plan.transformUp {
    case project @ Project(projectList, rel: DataSourceV2ScanRelation)
        if rel.scan.isInstanceOf[PaimonScan] =>
      tryPushDown(project, projectList, rel).getOrElse(project)
  }

  private def tryPushDown(
      project: Project,
      projectList: Seq[NamedExpression],
      rel: DataSourceV2ScanRelation): Option[Project] = {

    val scan = rel.scan.asInstanceOf[PaimonScan]

    // Collect VariantType output attributes (by name)
    val variantAttrNames: Set[String] = rel.output
      .filter(_.dataType == VariantType)
      .map(_.name)
      .toSet
    if (variantAttrNames.isEmpty) return None

    // Walk the project list to find VariantGet usages and direct references
    val variantGetsByCol = mutable.Map[String, mutable.ListBuffer[VariantGet]]()
    val colUsedDirectly = mutable.Set[String]()

    def collectUsages(expr: Expression): Unit = expr match {
      case vg: VariantGet if vg.path.foldable =>
        vg.child match {
          case ar: AttributeReference if variantAttrNames.contains(ar.name) =>
            variantGetsByCol.getOrElseUpdate(ar.name, mutable.ListBuffer.empty) += vg
          case _ =>
            // Nested or non-column child: treat child refs as direct usages
            vg.child.foreach(collectUsages)
        }
      case ar: AttributeReference if variantAttrNames.contains(ar.name) =>
        colUsedDirectly += ar.name
      case other =>
        other.children.foreach(collectUsages)
    }

    projectList.foreach(collectUsages)

    // Only push down columns that are exclusively used via VariantGet with literal paths
    val pushableCols = variantGetsByCol.keys.filterNot(colUsedDirectly.contains).toSet
    if (pushableCols.isEmpty) return None

    // For each pushable column, build a VariantRowType (deduplicated by GetKey)
    val newVariantRowTypes = mutable.Map[String, org.apache.paimon.types.RowType]()
    val getKeyToIndex = mutable.Map[(String, GetKey), Int]()

    pushableCols.foreach {
      colName =>
        val builder = VariantMetadataUtils.VariantRowTypeBuilder.builder()
        var nextIdx = 0

        variantGetsByCol(colName).foreach {
          vg =>
            val path = vg.path.eval().toString
            val tz = vg.timeZoneId.getOrElse("UTC")
            val key = GetKey(path, vg.targetType, vg.failOnError, tz)
            val globalKey = (colName, key)
            if (!getKeyToIndex.contains(globalKey)) {
              getKeyToIndex(globalKey) = nextIdx
              nextIdx += 1
              builder.field(SparkTypeUtils.toPaimonType(vg.targetType), path, vg.failOnError, tz)
            }
        }
        newVariantRowTypes(colName) = builder.build()
    }

    // Create new PaimonScan with variant projections
    val newScan = scan.copy(variantProjections = newVariantRowTypes.toMap)

    // Update output attributes: variant columns become StructType (field names "0","1",...)
    val newOutput = rel.output.map {
      attr =>
        newVariantRowTypes.get(attr.name) match {
          case Some(vrt) => attr.withDataType(SparkTypeUtils.fromPaimonRowType(vrt))
          case None => attr
        }
    }
    val newRel = rel.copy(scan = newScan, output = newOutput)
    val newAttrByName = newOutput.map(a => a.name -> a).toMap

    // Rewrite project list: replace VariantGet with GetStructField(newAttr, ordinal)
    val newProjectList: Seq[NamedExpression] = projectList.map {
      expr =>
        expr
          .transform {
            case vg: VariantGet if vg.path.foldable =>
              vg.child match {
                case ar: AttributeReference if newVariantRowTypes.contains(ar.name) =>
                  val path = vg.path.eval().toString
                  val tz = vg.timeZoneId.getOrElse("UTC")
                  val key = GetKey(path, vg.targetType, vg.failOnError, tz)
                  val idx = getKeyToIndex((ar.name, key))
                  GetStructField(newAttrByName(ar.name), idx)
                case _ => vg
              }
          }
          .asInstanceOf[NamedExpression]
    }

    Some(Project(newProjectList, newRel))
  }
}
