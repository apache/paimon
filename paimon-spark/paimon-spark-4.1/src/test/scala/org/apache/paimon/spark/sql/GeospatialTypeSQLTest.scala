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
}
