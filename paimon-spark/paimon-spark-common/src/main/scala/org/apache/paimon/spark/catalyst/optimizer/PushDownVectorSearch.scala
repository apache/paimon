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
import org.apache.paimon.predicate.VectorSearch
import org.apache.paimon.spark.SparkTable
import org.apache.paimon.spark.catalog.functions.PaimonFunctions
import org.apache.paimon.table.{FileStoreTable, InnerTable, VectorSearchTable}

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.types.{ArrayType, FloatType}

/**
 * An optimization rule that detects `ORDER BY cosine_similarity(embedding_col, vector) DESC LIMIT
 * N` pattern and pushes down vector search to Paimon's global vector index.
 *
 * This rule only applies when:
 *   - The table has `data-evolution.enabled=true` and `row-tracking.enabled=true`
 *   - A global vector index exists on the embedding column
 *   - The ORDER BY uses cosine_similarity with DESC ordering
 *   - A LIMIT clause is present
 */
object PushDownVectorSearch extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = {
    plan.transformDown {
      // Pattern: GlobalLimit -> LocalLimit -> Project -> Sort -> DataSourceV2Relation
      case globalLimit @ GlobalLimit(limitExpr, LocalLimit(_, project: Project))
          if isProjectWithSortAndPaimonRelation(project) =>
        handleProjectWithSort(globalLimit, limitExpr, project)

      // Pattern: GlobalLimit -> LocalLimit -> Sort -> Project -> DataSourceV2Relation
      case globalLimit @ GlobalLimit(limitExpr, LocalLimit(_, sort: Sort))
          if isSortWithProjectAndPaimonRelation(sort) =>
        handleSortWithProject(globalLimit, limitExpr, sort)

      // Pattern: GlobalLimit -> LocalLimit -> Sort -> DataSourceV2Relation (no project)
      case globalLimit @ GlobalLimit(limitExpr, LocalLimit(_, sort: Sort))
          if isSortWithPaimonRelation(sort) =>
        handleSortWithoutProject(globalLimit, limitExpr, sort)
    }
  }

  private def isProjectWithSortAndPaimonRelation(project: Project): Boolean = {
    project.child match {
      case sort: Sort if sort.global =>
        isPaimonRelation(sort.child) &&
        canPushVectorSearch(sort.child) &&
        isVectorSearchPattern(sort.order)
      case _ => false
    }
  }

  private def isSortWithProjectAndPaimonRelation(sort: Sort): Boolean = {
    if (!sort.global) return false
    sort.child match {
      case Project(_, relation) =>
        isPaimonRelation(relation) &&
        canPushVectorSearch(relation) &&
        isVectorSearchPattern(sort.order)
      case _ => false
    }
  }

  private def isSortWithPaimonRelation(sort: Sort): Boolean = {
    if (!sort.global) return false
    sort.child match {
      case _: Project => false // handled by isSortWithProjectAndPaimonRelation
      case relation if isPaimonRelation(relation) =>
        canPushVectorSearch(relation) && isVectorSearchPattern(sort.order)
      case _ => false
    }
  }

  private def handleProjectWithSort(
      globalLimit: GlobalLimit,
      limitExpr: Expression,
      project: Project): LogicalPlan = {
    val sort = project.child.asInstanceOf[Sort]
    val relation = sort.child.asInstanceOf[DataSourceV2Relation]
    val limit = extractLimit(limitExpr)

    extractVectorSearchInfo(sort.order, project.projectList, relation) match {
      case Some((fieldName, queryVector)) if limit.isDefined =>
        val vectorSearch = new VectorSearch(queryVector, limit.get, fieldName)
        val newRelation = wrapWithVectorSearchTable(relation, vectorSearch)
        val newSort = sort.copy(child = newRelation)
        val newProject = project.copy(child = newSort)
        globalLimit.copy(child = LocalLimit(limitExpr, newProject))
      case _ => globalLimit
    }
  }

  private def handleSortWithProject(
      globalLimit: GlobalLimit,
      limitExpr: Expression,
      sort: Sort): LogicalPlan = {
    val project = sort.child.asInstanceOf[Project]
    val relation = project.child.asInstanceOf[DataSourceV2Relation]
    val limit = extractLimit(limitExpr)

    extractVectorSearchInfo(sort.order, project.projectList, relation) match {
      case Some((fieldName, queryVector)) if limit.isDefined =>
        val vectorSearch = new VectorSearch(queryVector, limit.get, fieldName)
        val newRelation = wrapWithVectorSearchTable(relation, vectorSearch)
        val newProject = project.copy(child = newRelation)
        val newSort = sort.copy(child = newProject)
        globalLimit.copy(child = LocalLimit(limitExpr, newSort))
      case _ => globalLimit
    }
  }

  private def handleSortWithoutProject(
      globalLimit: GlobalLimit,
      limitExpr: Expression,
      sort: Sort): LogicalPlan = {
    val relation = sort.child.asInstanceOf[DataSourceV2Relation]
    val limit = extractLimit(limitExpr)

    extractVectorSearchInfoDirect(sort.order, relation) match {
      case Some((fieldName, queryVector)) if limit.isDefined =>
        val vectorSearch = new VectorSearch(queryVector, limit.get, fieldName)
        val newRelation = wrapWithVectorSearchTable(relation, vectorSearch)
        val newSort = sort.copy(child = newRelation)
        globalLimit.copy(child = LocalLimit(limitExpr, newSort))
      case _ => globalLimit
    }
  }

  private def isPaimonRelation(plan: LogicalPlan): Boolean = {
    plan match {
      case r: DataSourceV2Relation => r.table.isInstanceOf[SparkTable]
      case _ => false
    }
  }

  private def wrapWithVectorSearchTable(
      relation: DataSourceV2Relation,
      vectorSearch: VectorSearch): DataSourceV2Relation = {
    relation.table match {
      case sparkTable @ SparkTable(table: InnerTable) =>
        val vectorSearchTable = VectorSearchTable.create(table, vectorSearch)
        relation.copy(table = sparkTable.copy(table = vectorSearchTable))
      case _ => relation
    }
  }

  private def canPushVectorSearch(plan: LogicalPlan): Boolean = {
    plan match {
      case relation: DataSourceV2Relation =>
        relation.table match {
          case SparkTable(table: InnerTable) =>
            // Check if table is already wrapped with VectorSearchTable
            table match {
              case _: VectorSearchTable => false // Already wrapped
              case fileStoreTable: FileStoreTable =>
                val options = CoreOptions.fromMap(fileStoreTable.options())
                options.dataEvolutionEnabled() && options.rowTrackingEnabled() && options
                  .globalIndexEnabled()
              case _ => false
            }
          case _ => false
        }
      case _ => false
    }
  }

  private def isVectorSearchPattern(sortOrders: Seq[SortOrder]): Boolean = {
    sortOrders.headOption.exists {
      sortOrder =>
        // Must be descending order for cosine similarity (higher is better)
        sortOrder.direction == Descending && isCosineSimilarityExpression(sortOrder.child)
    }
  }

  private def isCosineSimilarityExpression(expr: Expression): Boolean = {
    expr match {
      case ApplyFunctionExpression(func, _)
          if func.name() == PaimonFunctions.COSINE_SIMILARITY &&
            func.canonicalName().startsWith("paimon") =>
        true
      case Alias(child, _) => isCosineSimilarityExpression(child)
      case _ => false
    }
  }

  private def extractLimit(limitExpr: Expression): Option[Int] = {
    limitExpr match {
      case Literal(value: Int, _) => Some(value)
      case Literal(value: Long, _) => Some(value.toInt)
      case _ => None
    }
  }

  private def extractVectorSearchInfo(
      sortOrders: Seq[SortOrder],
      projectList: Seq[NamedExpression],
      relation: DataSourceV2Relation): Option[(String, Array[Float])] = {
    sortOrders.headOption.flatMap {
      sortOrder => extractFromExpression(sortOrder.child, projectList, relation)
    }
  }

  private def extractVectorSearchInfoDirect(
      sortOrders: Seq[SortOrder],
      relation: DataSourceV2Relation): Option[(String, Array[Float])] = {
    sortOrders.headOption.flatMap {
      sortOrder => extractFromExpression(sortOrder.child, Seq.empty, relation)
    }
  }

  private def extractFromExpression(
      expr: Expression,
      projectList: Seq[NamedExpression],
      relation: DataSourceV2Relation): Option[(String, Array[Float])] = {
    expr match {
      case ApplyFunctionExpression(func, args)
          if func.name() == PaimonFunctions.COSINE_SIMILARITY &&
            func.canonicalName().startsWith("paimon") =>
        extractFromArgs(args, projectList, relation)
      case Alias(child, _) => extractFromExpression(child, projectList, relation)
      case _ => None
    }
  }

  private def extractFromArgs(
      args: Seq[Expression],
      projectList: Seq[NamedExpression],
      relation: DataSourceV2Relation): Option[(String, Array[Float])] = {
    if (args.size != 2) {
      return None
    }

    // First arg is the column reference, second is the query vector
    val (columnExpr, vectorExpr) = (args(0), args(1))

    // Extract field name from the column reference
    val fieldName = extractFieldName(columnExpr, projectList, relation)

    // Extract the query vector from the literal
    val queryVector = extractQueryVector(vectorExpr)

    for {
      field <- fieldName
      vector <- queryVector
    } yield (field, vector)
  }

  private def extractFieldName(
      expr: Expression,
      projectList: Seq[NamedExpression],
      relation: DataSourceV2Relation): Option[String] = {
    expr match {
      case attr: AttributeReference =>
        // Try to find in project list first
        projectList
          .collectFirst {
            case a @ Alias(child: AttributeReference, _) if a.exprId == attr.exprId =>
              child.name
            case a: AttributeReference if a.exprId == attr.exprId =>
              a.name
          }
          .orElse(Some(attr.name))
      case _ => None
    }
  }

  private def extractQueryVector(expr: Expression): Option[Array[Float]] = {
    expr match {
      case Literal(arrayData, ArrayType(FloatType, _)) if arrayData != null =>
        val arr = arrayData.asInstanceOf[org.apache.spark.sql.catalyst.util.ArrayData]
        Some(arr.toFloatArray())
      case CreateArray(elements, _) =>
        val floats = elements.flatMap(extractFloatValue)
        if (floats.size == elements.size) Some(floats.toArray) else None
      case _ => None
    }
  }

  private def extractFloatValue(expr: Expression): Option[Float] = {
    expr match {
      case Literal(v: Float, _) => Some(v)
      case Literal(v: Double, _) => Some(v.toFloat)
      case Literal(v: java.lang.Float, _) if v != null => Some(v.floatValue())
      case Literal(v: java.lang.Double, _) if v != null => Some(v.floatValue())
      case c: Cast if c.dataType == FloatType => extractFloatValue(c.child)
      case _ => None
    }
  }
}
