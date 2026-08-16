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

package org.apache.paimon.flink;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.EdgeAlgorithm;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests Flink SQL interoperability with Paimon geospatial columns. */
public class GeospatialTypeTableITCase extends CatalogITCaseBase {

    private static final String TABLE_NAME = "geospatial_table";

    @Test
    public void testRejectGeospatialColumnsInFlinkSql() throws Exception {
        createGeospatialTable();

        assertUnsupported(() -> batchSql("SELECT * FROM %s", TABLE_NAME));
        assertUnsupported(
                () ->
                        batchSql(
                                "CREATE TABLE geospatial_like LIKE %s (EXCLUDING OPTIONS)",
                                TABLE_NAME));
        assertUnsupported(
                () -> batchSql("CREATE TABLE geospatial_ctas AS SELECT * FROM %s", TABLE_NAME));
    }

    private void createGeospatialTable() throws Exception {
        flinkCatalog()
                .catalog()
                .createTable(
                        Identifier.create(tEnv.getCurrentDatabase(), TABLE_NAME),
                        Schema.newBuilder()
                                .column("id", DataTypes.INT())
                                .column("geom", DataTypes.GEOMETRY("EPSG:3857"))
                                .column(
                                        "geog",
                                        DataTypes.GEOGRAPHY("OGC:CRS84", EdgeAlgorithm.SPHERICAL))
                                .option(
                                        CoreOptions.FILE_FORMAT.key(),
                                        CoreOptions.FILE_FORMAT_PARQUET)
                                .build(),
                        false);
    }

    private void assertUnsupported(org.assertj.core.api.ThrowableAssert.ThrowingCallable callable) {
        assertThatThrownBy(callable)
                .hasStackTraceContaining("Flink SQL does not support Paimon geospatial type")
                .hasStackTraceContaining("Exposing it as VARBINARY would lose its CRS");
    }
}
