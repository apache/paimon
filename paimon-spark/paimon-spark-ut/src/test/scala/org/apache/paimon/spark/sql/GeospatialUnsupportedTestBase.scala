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

import org.apache.paimon.catalog.Identifier
import org.apache.paimon.schema.Schema
import org.apache.paimon.spark.PaimonSparkTestBase
import org.apache.paimon.types.DataTypes

abstract class GeospatialUnsupportedTestBase extends PaimonSparkTestBase {

  test("Spark SQL rejects geospatial columns before Spark 4.1") {
    val identifier = Identifier.create(dbName0, "geospatial_table")
    paimonCatalog.createTable(
      identifier,
      Schema.newBuilder
        .column("id", DataTypes.INT())
        .column("geom", DataTypes.GEOMETRY())
        .column("geog", DataTypes.GEOGRAPHY())
        .build,
      false
    )

    try {
      val error = intercept[UnsupportedOperationException] {
        sql("SELECT * FROM geospatial_table")
      }
      assert(error.getMessage.contains("Geometry and geography require Spark 4.1 or later"))
    } finally {
      paimonCatalog.dropTable(identifier, true)
    }
  }
}
