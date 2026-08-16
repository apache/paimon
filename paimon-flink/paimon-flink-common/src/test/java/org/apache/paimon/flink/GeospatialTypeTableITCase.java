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

import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests Flink SQL interoperability with Paimon geospatial columns. */
public class GeospatialTypeTableITCase extends CatalogITCaseBase {

    private static final String TABLE_NAME = "geospatial_table";

    private static final byte[] POINT_1_2_WKB =
            new byte[] {
                1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, (byte) 0xf0, 0x3f, 0, 0, 0, 0, 0, 0, 0, 0x40
            };

    private static final byte[] POINT_3_4_WKB =
            new byte[] {1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0x08, 0x40, 0, 0, 0, 0, 0, 0, 0x10, 0x40};

    @Test
    public void testReadWriteGeospatialColumnsAsWkb() throws Exception {
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

        List<org.apache.flink.table.types.DataType> columnTypes =
                tEnv.from(TABLE_NAME).getResolvedSchema().getColumnDataTypes();
        assertThat(columnTypes.get(0).getLogicalType().is(LogicalTypeRoot.INTEGER)).isTrue();
        assertThat(columnTypes.get(1).getLogicalType().is(LogicalTypeRoot.VARBINARY)).isTrue();
        assertThat(columnTypes.get(2).getLogicalType().is(LogicalTypeRoot.VARBINARY)).isTrue();

        batchSql(
                "INSERT INTO %s VALUES "
                        + "(1, X'0101000000000000000000F03F0000000000000040', "
                        + "X'010100000000000000000008400000000000001040'), "
                        + "(2, CAST(NULL AS BYTES), "
                        + "X'0101000000000000000000F03F0000000000000040')",
                TABLE_NAME);

        List<Row> rows = batchSql("SELECT * FROM %s ORDER BY id", TABLE_NAME);
        assertThat(rows)
                .containsExactly(
                        Row.of(1, POINT_1_2_WKB, POINT_3_4_WKB), Row.of(2, null, POINT_1_2_WKB));

        assertThat(paimonTable(TABLE_NAME).schema().fields().get(1).type())
                .isEqualTo(DataTypes.GEOMETRY("EPSG:3857"));
        assertThat(paimonTable(TABLE_NAME).schema().fields().get(2).type())
                .isEqualTo(DataTypes.GEOGRAPHY("OGC:CRS84", EdgeAlgorithm.SPHERICAL));
    }
}
