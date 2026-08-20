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

package org.apache.paimon.spark;

import org.apache.paimon.data.GenericRow;
import org.apache.paimon.spark.data.SparkInternalRow;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.EdgeAlgorithm;
import org.apache.paimon.types.RowType;

import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.catalyst.util.STUtils;
import org.apache.spark.sql.types.Geography;
import org.apache.spark.sql.types.GeographyType;
import org.apache.spark.sql.types.Geometry;
import org.apache.spark.sql.types.GeometryType;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests Spark 4.1 geometry and geography interoperability. */
class GeospatialTypeTest {

    private static final byte[] POINT_WKB =
            new byte[] {1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, (byte) 0xf0, 0x3f, 0, 0, 0, 0, 0, 0, 0x40};

    @Test
    void testTypeRoundTrip() {
        RowType paimonType =
                DataTypes.ROW(
                        DataTypes.FIELD(0, "geom", DataTypes.GEOMETRY()),
                        DataTypes.FIELD(
                                1,
                                "geog",
                                DataTypes.GEOGRAPHY("OGC:CRS84", EdgeAlgorithm.SPHERICAL)));

        StructType sparkType = SparkTypeUtils.fromPaimonRowType(paimonType);
        assertThat(sparkType.apply("geom").dataType()).isInstanceOf(GeometryType.class);
        assertThat(((GeometryType) sparkType.apply("geom").dataType()).crs())
                .isEqualTo("OGC:CRS84");
        assertThat(sparkType.apply("geog").dataType()).isInstanceOf(GeographyType.class);
        assertThat(((GeographyType) sparkType.apply("geog").dataType()).crs())
                .isEqualTo("OGC:CRS84");
        assertThat(((GeographyType) sparkType.apply("geog").dataType()).algorithm().toString())
                .isEqualTo("SPHERICAL");
        assertThat(SparkTypeUtils.toPaimonType(sparkType)).isEqualTo(paimonType);

        assertThatThrownBy(
                        () ->
                                SparkTypeUtils.fromPaimonType(
                                        DataTypes.GEOGRAPHY("OGC:CRS84", EdgeAlgorithm.KARNEY)))
                .hasMessageContaining("karney");
    }

    @Test
    void testWkbReadWriteRoundTrip() {
        RowType paimonType =
                DataTypes.ROW(
                        DataTypes.FIELD(0, "geom", DataTypes.GEOMETRY()),
                        DataTypes.FIELD(1, "geog", DataTypes.GEOGRAPHY()));
        StructType sparkType = SparkTypeUtils.fromPaimonRowType(paimonType);

        SparkInternalRow sparkRow =
                SparkInternalRow.create(paimonType).replace(GenericRow.of(POINT_WKB, POINT_WKB));
        assertThat(STUtils.stAsBinary(sparkRow.getGeometry(0))).isEqualTo(POINT_WKB);
        assertThat(STUtils.stSrid(sparkRow.getGeometry(0))).isEqualTo(4326);
        assertThat(STUtils.stAsBinary(sparkRow.getGeography(1))).isEqualTo(POINT_WKB);
        assertThat(STUtils.stSrid(sparkRow.getGeography(1))).isEqualTo(4326);

        SparkInternalRowWrapper internalWrapper =
                new SparkInternalRowWrapper(sparkType, 2).replace(sparkRow);
        assertThat(internalWrapper.getBinary(0)).isEqualTo(POINT_WKB);
        assertThat(internalWrapper.getBinary(1)).isEqualTo(POINT_WKB);

        SparkRow externalWrapper =
                new SparkRow(
                        paimonType,
                        RowFactory.create(
                                Geometry.fromWKB(POINT_WKB, 4326),
                                Geography.fromWKB(POINT_WKB, 4326)));
        assertThat(externalWrapper.getBinary(0)).isEqualTo(POINT_WKB);
        assertThat(externalWrapper.getBinary(1)).isEqualTo(POINT_WKB);
    }
}
