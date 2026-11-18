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

package org.apache.spark.sql.paimon.shims

import org.apache.paimon.geo.BoundingBox
import org.apache.paimon.predicate.{FieldRef, GeometryIntersectsTransform, Predicate, PredicateBuilder}
import org.apache.paimon.shade.org.locationtech.jts.geom.Geometry
import org.apache.paimon.shade.org.locationtech.jts.geom.prep.{PreparedGeometry, PreparedGeometryFactory}
import org.apache.paimon.shade.org.locationtech.jts.io.WKBReader
import org.apache.paimon.types.{GeometryType => PaimonGeometryType, RowType}

import org.apache.spark.sql.SparkSessionExtensions
import org.apache.spark.sql.catalyst.{FunctionIdentifier, InternalRow}
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry
import org.apache.spark.sql.catalyst.expressions.{ApplyFunctionExpression, BoundReference, Expression, ExpressionInfo, Literal}
import org.apache.spark.sql.catalyst.expressions.st.{ST_AsBinary, ST_Srid}
import org.apache.spark.sql.connector.catalog.functions.ScalarFunction
import org.apache.spark.sql.connector.expressions.{Expression => V2Expression, Literal => V2Literal, NamedReference, UserDefinedScalarFunc}
import org.apache.spark.sql.connector.expressions.filter.{Predicate => SparkPredicate}
import org.apache.spark.sql.types.{BooleanType, DataType, GeometryType, NullType}

import scala.util.Try

/** SQL geometry intersection and its conservative scan predicate for a constant query geometry. */
private[shims] object SpatialIntersects {
  private val Name = "st_intersects"
  private val CanonicalName = "org.apache.paimon.st_intersects"

  def register(extensions: SparkSessionExtensions): Unit = {
    val id = FunctionIdentifier(Name)
    if (!FunctionRegistry.builtin.functionExists(id)) {
      extensions.injectFunction((id, new ExpressionInfo(classOf[Intersects].getName, Name), build))
    }
  }

  private def build(args: Seq[Expression]): Expression = {
    require(args.size == 2, "ST_Intersects requires two GEOMETRY arguments")
    val types = args.map(_.dataType)
    require(
      types.forall(t => t == NullType || t.isInstanceOf[GeometryType]),
      "ST_Intersects requires GEOMETRY arguments")
    if (types.contains(NullType)) {
      Literal.create(null, BooleanType)
    } else {
      ApplyFunctionExpression(Intersects(types, args.indexWhere(_.foldable)), args)
    }
  }

  // Only recognize the entire positive predicate. A negated envelope match would lose rows.
  def candidate(expression: V2Expression, rowType: RowType, ranked: Boolean): Option[Predicate] =
    expression match {
      case p: SparkPredicate if p.name() == "BOOLEAN_EXPRESSION" =>
        p.children() match {
          case Array(f: UserDefinedScalarFunc) if f.canonicalName() == CanonicalName =>
            f.children() match {
              case Array(col: NamedReference, lit: V2Literal[_]) =>
                bounds(col, lit, rowType, ranked)
              case Array(lit: V2Literal[_], col: NamedReference) =>
                bounds(col, lit, rowType, ranked)
              case _ => None
            }
          case _ => None
        }
      case _ => None
    }

  private def bounds(
      col: NamedReference,
      lit: V2Literal[_],
      rowType: RowType,
      ranked: Boolean): Option[Predicate] =
    (col.fieldNames(), lit.dataType()) match {
      case (Array(name), t: GeometryType) if lit.value() != null && !t.isMixedSrid =>
        val index = rowType.getFieldNames.indexOf(name)
        if (index < 0) return None
        rowType.getTypeAt(index) match {
          case geom: PaimonGeometryType if GeometryType(geom.getCrs).srid == t.srid =>
            // The V1 index accepts 2D WKB. Other valid geometries keep exact SQL evaluation.
            val wkb =
              new ST_AsBinary(Literal(lit.value(), t)).replacement.eval().asInstanceOf[Array[Byte]]
            val builder = new PredicateBuilder(rowType)
            if (ranked) {
              val exact = new GeometryIntersectsTransform(new FieldRef(index, name, geom), wkb)
              Some(builder.equal(exact, true))
            } else {
              Try(BoundingBox.fromWkb(wkb)).toOption.filterNot(_.isEmpty).map {
                b => builder.bboxIntersects(index, b.minX(), b.minY(), b.maxX(), b.maxY())
              }
            }
          case _ => None
        }
      case _ => None
    }

  private case class Intersects(types: Seq[DataType], constant: Int)
    extends ScalarFunction[java.lang.Boolean]
    with Serializable {
    override def name(): String = Name
    override def canonicalName(): String = CanonicalName
    override def inputTypes(): Array[DataType] = types.toArray
    override def resultType(): DataType = BooleanType

    private val sameFixedSrid = types(0) == types(1) &&
      !types(0).asInstanceOf[GeometryType].isMixedSrid

    @transient private lazy val refs = types.indices.map(i => BoundReference(i, types(i), true))
    @transient private lazy val binary = refs.map(ref => new ST_AsBinary(ref).replacement)
    @transient private lazy val srids = refs.map(ref => ST_Srid(ref).replacement)
    @transient private lazy val reader = new WKBReader()
    @transient private var prepared: PreparedGeometry = _

    private def geometry(i: Int, input: InternalRow): Geometry =
      reader.read(binary(i).eval(input).asInstanceOf[Array[Byte]])

    override def produceResult(input: InternalRow): java.lang.Boolean = {
      if (input.isNullAt(0) || input.isNullAt(1)) return null
      require(
        sameFixedSrid || srids(0).eval(input) == srids(1).eval(input),
        "ST_Intersects requires geometries with the same SRID")
      if (constant >= 0) {
        if (prepared == null) prepared = PreparedGeometryFactory.prepare(geometry(constant, input))
        prepared.intersects(geometry(1 - constant, input))
      } else {
        geometry(0, input).intersects(geometry(1, input))
      }
    }
  }
}
