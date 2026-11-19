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

package org.apache.paimon.spark.sql

import org.apache.paimon.spark.PaimonSparkTestBase
import org.apache.paimon.types.{DataTypes, EdgeAlgorithm}

import org.apache.spark.SparkConf
import org.apache.spark.sql.{AnalysisException, Row}
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.expressions.ApplyFunctionExpression

/** Tests Spark 4.1 SQL interoperability with Paimon geospatial columns. */
class GeospatialTypeSQLTest extends PaimonSparkTestBase {

  override protected def sparkConf: SparkConf = {
    super.sparkConf.set("spark.sql.geospatial.enabled", "true")
  }

  test("Spark SQL requires geospatial support to be enabled") {
    withSparkSQLConf("spark.sql.geospatial.enabled" -> "false") {
      val error = intercept[AnalysisException] {
        sql("CREATE TABLE geospatial_disabled (geom GEOMETRY(4326)) USING paimon")
      }
      assert(error.getMessage.contains("GEOSPATIAL_DISABLED"))
    }
  }

  test("Spark SQL reads and writes native geospatial values") {
    withTable("t") {
      sql("""
            |CREATE TABLE t (
            |  id INT,
            |  geom GEOMETRY(4326),
            |  geog GEOGRAPHY(4326)
            |) TBLPROPERTIES ('file.format' = 'parquet')
            |""".stripMargin)

      sql("""
            |INSERT INTO t VALUES
            |  (1,
            |   ST_SetSrid(
            |     ST_GeomFromWKB(unhex('0101000000000000000000F03F0000000000000040')),
            |     4326),
            |   ST_GeogFromWKB(unhex('010100000000000000000008400000000000001040'))),
            |  (2, NULL,
            |   ST_GeogFromWKB(unhex('0101000000000000000000F03F0000000000000040')))
            |""".stripMargin)

      checkAnswer(
        sql("""
              |SELECT id,
              |       hex(ST_AsBinary(geom)), ST_Srid(geom),
              |       hex(ST_AsBinary(geog)), ST_Srid(geog)
              |FROM t ORDER BY id
              |""".stripMargin),
        Seq(
          Row(
            1,
            "0101000000000000000000F03F0000000000000040",
            4326,
            "010100000000000000000008400000000000001040",
            4326),
          Row(2, null, null, "0101000000000000000000F03F0000000000000040", 4326)
        )
      )

      val fields = loadTable("t").schema().fields()
      assert(fields.get(1).`type`() == DataTypes.GEOMETRY("OGC:CRS84"))
      assert(
        fields.get(2).`type`() ==
          DataTypes.GEOGRAPHY("OGC:CRS84", EdgeAlgorithm.SPHERICAL))
    }
  }

  test("ST_Intersects refines the geometry before full-text top-k") {
    val diagonal =
      "0102000000020000000000000000000000000000000000000000000000000000400000000000000040"
    val point = "01010000000000000000000000000000000000F03F"
    def geom(hex: String): String = s"ST_SetSrid(ST_GeomFromWKB(unhex('$hex')), 4326)"
    val pointZ = "01E90300000000000000000000000000000000F03F0000000000000040"
    Seq(true, false).foreach {
      indexed =>
        withTable("t") {
          sql(
            """CREATE TABLE t (id INT, geom GEOMETRY(4326), content STRING) TBLPROPERTIES (
              | 'bucket' = '-1', 'row-tracking.enabled' = 'true', 'data-evolution.enabled' = 'true',
              | 'scalar-index.search-mode' = 'full')""".stripMargin)
          val stored = if (indexed) point else pointZ
          sql(
            s"INSERT INTO t VALUES (1, ${geom(diagonal)}, 'paimon lake'), (2, ${geom(stored)}, 'paimon')")
          val indexes = Seq(("content", "test-fulltext")) ++ (if (indexed) Seq(("geom", "rtree"))
                                                              else Seq.empty)
          indexes.foreach {
            case (col, index) =>
              sql(
                s"CALL sys.create_global_index(table => 'test.t', index_column => '$col', index_type => '$index')")
                .collect()
          }
          val search =
            """full_text_search('t', 'content', '{"match":{"query":"paimon lake"}}', 1)"""
          checkAnswer(sql(s"SELECT id FROM $search"), Seq(Row(1)))
          checkAnswer(
            sql(s"SELECT id FROM $search WHERE ST_Intersects(geom, ${geom(pointZ)})"),
            Seq(Row(2)))
          val mixedPoint = s"ST_SetSrid(ST_GeomFromWKB(unhex('$point')), 4300 + 26)"
          Seq(
            s"ST_Intersects(geom, $mixedPoint)",
            s"ST_Intersects(geom, ${geom(point)}) IS TRUE",
            s"TRUE <=> ST_Intersects(geom, ${geom(point)})"
          ).foreach {
            predicate => checkAnswer(sql(s"SELECT id FROM $search WHERE $predicate"), Seq(Row(2)))
          }
          Seq(false, true).foreach {
            refine =>
              sql(
                s"ALTER TABLE t SET TBLPROPERTIES ('global-index.filter.refine-from-data' = '$refine')")
              checkAnswer(
                sql(s"SELECT id FROM $search WHERE ST_Intersects(geom, ${geom(point)})"),
                Seq(Row(2)))
          }
        }
    }
  }

  test("ST_Intersects prunes indexed geometries and refines envelope matches") {
    def geometry(wkb: String): String = s"ST_SetSrid(ST_GeomFromWKB(unhex('$wkb')), 4326)"
    val diagonal =
      geometry("0102000000020000000000000000000000000000000000000000000000000000400000000000000040")
    val boundary =
      geometry("0102000000020000000000000000000000000000000000F03F0000000000000040000000000000F03F")
    val point = geometry("01010000000000000000000000000000000000F03F")
    val far = geometry("010100000000000000000022400000000000002240")
    withTable("t") {
      sql("""CREATE TABLE t (id INT, geom GEOMETRY(4326)) TBLPROPERTIES (
            | 'bucket' = '-1', 'row-tracking.enabled' = 'true',
            | 'data-evolution.enabled' = 'true')""".stripMargin)
      sql(s"INSERT INTO t VALUES (1, $diagonal), (2, $boundary), (3, $far), (4, NULL)")
      sql(
        "CALL sys.create_global_index(table => 'test.t', index_column => 'geom', " +
          "index_type => 'rtree')").collect()

      Seq(s"geom, $point", s"$point, geom").foreach {
        args =>
          val query = sql(s"SELECT id FROM t WHERE ST_Intersects($args)")
          val projection = sql(s"SELECT ST_Intersects($args) FROM t").queryExecution.optimizedPlan
          val Seq(call) = projection.expressions.flatMap(_.collect {
            case f: ApplyFunctionExpression => f
          })
          // Binding the optimized children directly must select the same prepared geometry.
          assert(call.children.count(_.foldable) == 1)
          val rebound = spark.sessionState.functionRegistry
            .lookupFunction(FunctionIdentifier("st_intersects"), call.children)
          assert(call.function == rebound.asInstanceOf[ApplyFunctionExpression].function)
          val plan = query.queryExecution.executedPlan.toString()
          assert(plan.contains("BBoxIntersects"), plan)
          checkAnswer(query, Seq(Row(2)))
      }
      checkAnswer(
        sql(s"SELECT id FROM t WHERE NOT ST_Intersects(geom, $point)"),
        Seq(Row(1), Row(3)))
      checkAnswer(
        sql(s"SELECT id FROM t WHERE ST_Intersects(geom, $point) OR id = 3"),
        Seq(Row(2), Row(3)))
      checkAnswer(
        sql(s"SELECT id, ST_Intersects(geom, $point) FROM t"),
        Seq(Row(1, false), Row(2, true), Row(3, false), Row(4, null)))
      checkAnswer(
        sql("SELECT id FROM t WHERE ST_Intersects(geom, geom)"),
        Seq(Row(1), Row(2), Row(3)))
      checkAnswer(sql(s"SELECT ST_Intersects(NULL, $point)"), Seq(Row(null)))
      val intersects = s"ST_Intersects(geom, $point)"
      checkAnswer(
        sql(s"SELECT $intersects, COUNT(*) FROM t GROUP BY $intersects"),
        Seq(Row(false, 2L), Row(true, 1L), Row(null, 1L)))
      val empty = geometry("010700000000000000")
      checkAnswer(sql(s"SELECT ST_Intersects($point, $empty)"), Seq(Row(false)))
      val error = intercept[Exception] {
        sql(s"SELECT id FROM t WHERE ST_Intersects(geom, ST_SetSrid($far, 3857))").collect()
      }
      assert(
        Iterator
          .iterate[Throwable](error)(_.getCause)
          .takeWhile(_ != null)
          .exists(e => Option(e.getMessage).exists(_.contains("same SRID"))))

      // A matching append outside the published index distinguishes an indexed fast scan
      // from a full scan. Full mode must also find the newly appended geometry.
      sql(s"INSERT INTO t VALUES (5, $point)")
      val select = s"SELECT id FROM t WHERE ST_Intersects(geom, $point)"
      checkAnswer(sql(select), Seq(Row(2)))
      sql("ALTER TABLE t SET TBLPROPERTIES ('scalar-index.search-mode' = 'full')")
      checkAnswer(sql(select), Seq(Row(2), Row(5)))
    }
  }
}
