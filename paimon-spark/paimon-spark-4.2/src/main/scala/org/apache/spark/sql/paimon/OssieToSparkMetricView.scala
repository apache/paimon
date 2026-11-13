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

package org.apache.spark.sql.paimon

import org.apache.paimon.spark.ossie.OssieModel

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAttribute, UnresolvedFunction, UnresolvedStar}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

import scala.collection.mutable

/** Compile field references before Spark sees the model, preserving source/field shadowing. */
private[paimon] object OssieToSparkMetricView {
  private val aggregates = Set("sum", "count", "avg", "min", "max")
  private val functions = aggregates ++ Set("abs", "round", "floor", "ceil", "ceiling", "coalesce")

  def convert(
      spark: SparkSession,
      model: OssieModel,
      source: Seq[String],
      sourceColumns: Seq[String]): String = {
    val sourceNames = sourceColumns.map(n => OssieModel.normalize(n) -> n).toMap
    require(sourceNames.size == sourceColumns.size, "Ambiguous case-insensitive source columns")
    val publicNames =
      (model.fields.map(_.name) ++ model.metrics.map(_.name)).map(OssieModel.normalize)
    val prefix = Iterator
      .from(0)
      .map(i => s"__ossie_${i}_")
      .find(p => !publicNames.exists(_.startsWith(p)))
      .get
    val fieldNames = model.fields.zipWithIndex.map {
      case (f, i) => OssieModel.normalize(f.name) -> s"${prefix}f$i"
    }.toMap
    val rawColumns = mutable.LinkedHashMap[String, String]()

    def sourceColumn(name: String): String = sourceNames.getOrElse(
      OssieModel.normalize(name),
      throw new IllegalArgumentException(s"Unknown Ossie source column: $name"))

    def field(name: String): Expression = UnresolvedAttribute(
      Seq(
        fieldNames.getOrElse(
          OssieModel.normalize(name),
          throw new IllegalArgumentException(s"Unknown Ossie dataset field: $name"))))

    val projections = model.fields.zipWithIndex.map {
      case (f, i) =>
        val expr = parse(spark, f.sql, metric = false).transformUp {
          case a: UnresolvedAttribute =>
            require(a.nameParts.size == 1, "Ossie field expressions must reference source columns")
            UnresolvedAttribute(Seq(sourceColumn(a.nameParts.head)))
        }
        s"${render(expr)} AS ${quote(s"${prefix}f$i")}"
    }

    val measures = model.metrics.map {
      m =>
        val expr = parse(spark, m.sql, metric = true).transformUp {
          case a: UnresolvedAttribute =>
            a.nameParts match {
              case Seq(name) if !m.modelScoped => field(name)
              case Seq(dataset, name)
                  if m.modelScoped &&
                    OssieModel.normalize(dataset) == OssieModel.normalize(model.dataset) =>
                field(name)
              case Seq(qualifier, name)
                  if !m.modelScoped &&
                    qualifier.equalsIgnoreCase("source") && !fieldNames.contains("source") =>
                val actual = sourceColumn(name)
                val alias = rawColumns.getOrElseUpdate(actual, s"${prefix}s${rawColumns.size}")
                UnresolvedAttribute(Seq(alias))
              case _ =>
                throw new IllegalArgumentException(s"Invalid Ossie metric reference: ${a.sql}")
            }
        }
        m.name -> render(expr)
    }
    val rawProjections = rawColumns.toSeq.map {
      case (name, alias) => s"${quote(name)} AS ${quote(alias)}"
    }
    val sql = "SELECT " + (projections ++ rawProjections).mkString(", ") +
      " FROM " + source.map(quote).mkString(".")
    val mapper = new ObjectMapper()
    val root = mapper.createObjectNode()
    root.put("version", "0.1")
    root.put("source", sql)
    val dimensions = root.putArray("dimensions")
    model.fields.zipWithIndex.foreach {
      case (f, i) => dimensions.addObject().put("name", f.name).put("expr", quote(s"${prefix}f$i"))
    }
    val metricNodes = root.putArray("measures")
    measures.foreach {
      case (name, expr) => metricNodes.addObject().put("name", name).put("expr", expr)
    }
    // JSON is a YAML subset, and avoids another serialization path for SQL strings.
    mapper.writeValueAsString(root)
  }

  private def parse(spark: SparkSession, sql: String, metric: Boolean): Expression = {
    val conf = new SQLConf()
    spark.sessionState.conf.getAllConfs.foreach { case (k, v) => conf.setConfString(k, v) }
    conf.setConf(SQLConf.ANSI_ENABLED, true)
    conf.setConf(SQLConf.DOUBLE_QUOTED_IDENTIFIERS, true)
    val expr = SQLConf.withExistingConf(conf)(CatalystSqlParser.parseExpression(sql))
    var aggregate = false
    expr.foreach {
      case f: UnresolvedFunction =>
        require(
          f.ignoreNulls.isEmpty && f.orderingWithinGroup.isEmpty,
          "Ossie null treatment and ordered-set aggregates are not supported")
        require(
          f.nameParts.size == 1 && functions.contains(OssieModel.normalize(f.nameParts.head)),
          s"Unsupported OSSIE_SQL_2026 function: ${f.nameParts.mkString(".")}"
        )
        val name = OssieModel.normalize(f.nameParts.head)
        if (aggregates.contains(name)) {
          require(metric, "Ossie field expressions cannot contain aggregates")
          aggregate = true
        }
        require(
          !f.arguments.exists(_.isInstanceOf[UnresolvedStar]) || name == "count",
          "Only COUNT(*) supports a star in Ossie expressions")
      case _: Literal | _: UnresolvedAttribute | _: UnresolvedStar =>
      case _: Add | _: Subtract | _: Multiply | _: Divide | _: Remainder | _: UnaryMinus |
          _: UnaryPositive | _: CaseWhen | _: EqualTo | _: LessThan | _: LessThanOrEqual |
          _: GreaterThan | _: GreaterThanOrEqual | _: And | _: Or | _: Not | _: IsNull |
          _: IsNotNull | _: In =>
      case c: Cast =>
        require(
          c.dataType.isInstanceOf[NumericType] || c.dataType == StringType ||
            c.dataType == BooleanType || c.dataType == DateType,
          s"Unsupported Ossie cast type: ${c.dataType}"
        )
      case other =>
        throw new IllegalArgumentException(s"Unsupported OSSIE_SQL_2026 expression: ${other.sql}")
    }
    require(!metric || aggregate, "Ossie metrics must contain a supported aggregate")
    expr
  }

  private def quote(name: String): String = "`" + name.replace("`", "``") + "`"

  // UnresolvedFunction inherits Expression.sql, which drops DISTINCT and FILTER. Render those
  // explicitly before asking the surrounding scalar expressions to render their own syntax.
  private def render(expr: Expression): String = expr.transformUp {
    case f: UnresolvedFunction =>
      val distinct = if (f.isDistinct) "DISTINCT " else ""
      val filter = f.filter.map(p => s" FILTER (WHERE ${p.sql})").getOrElse("")
      RenderedSQL(
        s"${f.nameParts.map(quote).mkString(".")}($distinct${f.arguments.map(_.sql).mkString(", ")})$filter")
  }.sql

  private case class RenderedSQL(text: String) extends LeafExpression with Unevaluable {
    override def dataType: org.apache.spark.sql.types.DataType = NullType
    override def nullable: Boolean = true
    override def sql: String = text
  }
}
