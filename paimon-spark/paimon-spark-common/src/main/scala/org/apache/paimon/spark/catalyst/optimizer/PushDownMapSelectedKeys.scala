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

import org.apache.paimon.CoreOptions
import org.apache.paimon.CoreOptions.MapStorageLayout
import org.apache.paimon.data.shredding.MapSelectedKeysMetadataUtils
import org.apache.paimon.data.shredding.MapSharedShreddingUtils
import org.apache.paimon.spark.PaimonScan

import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeReference, Expression, ExprId, GetMapValue, GetStructField, Literal, NamedExpression}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation
import org.apache.spark.sql.types.{DataType, MapType, StringType, StructField, StructType}

import scala.collection.mutable

/**
 * Pushes literal-key MAP access on shared-shredding MAP columns into Paimon scan read type.
 *
 * <p>Example: {@code SELECT id, attrs['key1'], attrs['key2'] FROM T} becomes a scan returning
 * {@code attrs} as {@code struct<0:valueType,1:valueType>} and a project reading struct fields.
 */
object PushDownMapSelectedKeys extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = plan.transform {
    case project @ Project(projectList, relation: DataSourceV2ScanRelation) =>
      relation.scan match {
        case scan: PaimonScan =>
          rewriteProject(project, projectList, relation, scan).getOrElse(project)
        case _ => project
      }
  }

  private def rewriteProject(
      project: Project,
      projectList: Seq[NamedExpression],
      relation: DataSourceV2ScanRelation,
      scan: PaimonScan): Option[LogicalPlan] = {
    val candidates = collectCandidates(projectList, scan)
    if (candidates.isEmpty) {
      return None
    }

    val outputByExprId = relation.output.map(a => a.exprId -> a).toMap
    val selectedMaps = mutable.LinkedHashMap.empty[MapPath, SelectedMap]
    candidates.foreach {
      candidate =>
        outputByExprId.get(candidate.rootExprId).foreach {
          root =>
            val path = MapPath(root.exprId, candidate.path)
            val selectedMap =
              selectedMaps.getOrElseUpdate(
                path,
                SelectedMap(root, candidate.path, candidate.mapType, mutable.ArrayBuffer.empty))
            if (!selectedMap.keys.contains(candidate.key)) {
              selectedMap.keys.append(candidate.key)
            }
        }
    }
    if (selectedMaps.isEmpty) {
      return None
    }

    val selectedByPath = selectedMaps.toMap
    val selectedByRoot = selectedMaps.values.toSeq.groupBy(_.root.exprId)
    val attrToRewritten = selectedByRoot.map {
      case (exprId, selections) =>
        val root = selections.head.root
        exprId -> root
          .withDataType(rewriteSparkType(root.dataType, root.name, selections))
          .asInstanceOf[AttributeReference]
    }

    val rewriteState = RewriteState(selectedByPath, attrToRewritten)
    val rewrittenProjectList = projectList.map(rewriteExpression(_, rewriteState))
    if (rewriteState.failed) {
      return None
    }

    val pushedMapSelectedKeys = scan.pushedMapSelectedKeys ++ selectedMaps.values.map {
      selected => selected.path.head -> selected.keys.toSeq
    }.toMap
    val rewrittenScan = scan.copy(pushedMapSelectedKeys = pushedMapSelectedKeys)
    val rewrittenOutput =
      relation.output.map(attr => attrToRewritten.getOrElse(attr.exprId, attr))
    val rewrittenRelation = relation.copy(scan = rewrittenScan, output = rewrittenOutput)
    Some(project.copy(projectList = rewrittenProjectList, child = rewrittenRelation))
  }

  private def collectCandidates(
      projectList: Seq[NamedExpression],
      scan: PaimonScan): Seq[MapKeyCandidate] = {
    val candidates = mutable.ArrayBuffer.empty[MapKeyCandidate]
    projectList.foreach {
      expr =>
        expr.foreach {
          case mapKeyValueAccess(access) if canPushDown(scan, access) =>
            candidates.append(
              MapKeyCandidate(access.root.exprId, access.path, access.mapType, access.key))
          case _ =>
        }
    }
    candidates.toSeq
  }

  private def rewriteExpression(
      expression: NamedExpression,
      state: RewriteState): NamedExpression = {
    expression match {
      case alias: Alias =>
        alias.child match {
          case mapKeyValueAccess(access) if state.selectedByPath.contains(access.mapPath) =>
            rewriteMapKeyAccess(access, state) match {
              case Some(rewritten) =>
                alias.withNewChildren(Seq(rewritten)).asInstanceOf[NamedExpression]
              case None => expression
            }
          case _ =>
            if (hasSelectedPathReference(alias, state)) {
              state.failed = true
            }
            expression
        }
      case attr: Attribute if state.hasRootSelection(attr.exprId) =>
        state.failed = true
        expression
      case _ =>
        if (hasSelectedPathReference(expression, state)) {
          state.failed = true
        }
        expression
    }
  }

  private def rewriteMapKeyAccess(access: MapKeyAccess, state: RewriteState): Option[Expression] = {
    val selected = state.selectedByPath(access.mapPath)
    val ordinal = selected.keys.indexOf(access.key)
    if (ordinal < 0) {
      state.failed = true
      None
    } else {
      buildPathExpression(state.attrToRewritten(access.root.exprId), access.path).map {
        selectedMapRow => GetStructField(selectedMapRow, ordinal, Some(access.key))
      }
    }
  }

  private def hasSelectedPathReference(expression: Expression, state: RewriteState): Boolean = {
    var found = false
    expression.foreach {
      case mapFieldReference(access) if state.selectedByPath.contains(access.mapPath) =>
        found = true
      case _ =>
    }
    found
  }

  private def canPushDown(scan: PaimonScan, access: MapKeyAccess): Boolean = {
    if (access.path.length != 1 || !canEncodeKey(access.key)) {
      return false
    }

    access.mapType match {
      case MapType(StringType, _, _) =>
        fieldType(scan.table.rowType(), access.path.head) match {
          case Some(mapType: org.apache.paimon.types.MapType) if isStringKeyMap(mapType) =>
            CoreOptions.fromMap(scan.table.options()).mapStorageLayout(access.path.head) ==
              MapStorageLayout.SHARED_SHREDDING
          case _ => false
        }
      case _ => false
    }
  }

  private def canEncodeKey(key: String): Boolean = {
    !key.contains(MapSelectedKeysMetadataUtils.KEY_DELIMITER) &&
    !key.startsWith(MapSelectedKeysMetadataUtils.METADATA_KEY)
  }

  private def isStringKeyMap(mapType: org.apache.paimon.types.MapType): Boolean = {
    MapSharedShreddingUtils.isShreddingKeyMap(mapType)
  }

  private object mapKeyValueAccess {
    def unapply(expression: Expression): Option[MapKeyAccess] = {
      expression match {
        case GetMapValue(mapFieldReference(access), StringLiteral(key)) =>
          Some(access.copy(key = key))
        case e if e.prettyName == "element_at" && e.children.length == 2 =>
          (e.children.head, e.children(1)) match {
            case (mapFieldReference(access), StringLiteral(key)) => Some(access.copy(key = key))
            case _ => None
          }
        case _ => None
      }
    }
  }

  private object mapFieldReference {
    def unapply(expression: Expression): Option[MapKeyAccess] = {
      toFieldPath(expression).collect {
        case (root, path, mapType: MapType) => MapKeyAccess(root, path, mapType, "")
      }
    }
  }

  private object StringLiteral {
    def unapply(expression: Expression): Option[String] = {
      expression match {
        case Literal(value, StringType) if value != null => Some(value.toString)
        case _ => None
      }
    }
  }

  private def toFieldPath(expression: Expression): Option[(Attribute, Seq[String], DataType)] = {
    expression match {
      case attr: Attribute => Some((attr, Seq(attr.name), attr.dataType))
      case GetStructField(child, ordinal, name) =>
        toFieldPath(child).flatMap {
          case (root, path, childType: StructType) if ordinal < childType.fields.length =>
            val field = childType.fields(ordinal)
            Some((root, path :+ name.getOrElse(field.name), field.dataType))
          case _ => None
        }
      case _ => None
    }
  }

  private def fieldType(
      rowType: org.apache.paimon.types.RowType,
      fieldName: String): Option[org.apache.paimon.types.DataType] = {
    if (!rowType.containsField(fieldName)) {
      None
    } else {
      Some(rowType.getField(fieldName).`type`())
    }
  }

  private def rewriteSparkType(
      dataType: DataType,
      fieldName: String,
      selections: Seq[SelectedMap]): DataType = {
    selections.find(_.path == Seq(fieldName)) match {
      case Some(selected) => selected.structType
      case None => dataType
    }
  }

  private def buildPathExpression(
      root: AttributeReference,
      path: Seq[String]): Option[Expression] = {
    if (path == Seq(root.name)) Some(root) else None
  }

  private case class MapKeyCandidate(
      rootExprId: ExprId,
      path: Seq[String],
      mapType: MapType,
      key: String)

  private case class MapPath(rootExprId: ExprId, path: Seq[String])

  private case class MapKeyAccess(
      root: Attribute,
      path: Seq[String],
      mapType: MapType,
      key: String) {
    def mapPath: MapPath = MapPath(root.exprId, path)
  }

  private case class SelectedMap(
      root: Attribute,
      path: Seq[String],
      mapType: MapType,
      keys: mutable.ArrayBuffer[String]) {
    def structType: StructType = {
      val MapType(_, valueType, _) = mapType
      StructType(keys.zipWithIndex.map {
        case (_, ordinal) =>
          StructField(ordinal.toString, valueType, nullable = true)
      }.toSeq)
    }
  }

  private case class RewriteState(
      selectedByPath: Map[MapPath, SelectedMap],
      attrToRewritten: Map[ExprId, AttributeReference]) {
    var failed: Boolean = false

    def hasRootSelection(exprId: ExprId): Boolean = {
      selectedByPath.keys.exists(_.rootExprId == exprId)
    }
  }
}
