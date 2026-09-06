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

package org.apache.paimon.types;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class GeospatialTypeTest {

    @Test
    void testIcebergCompatibleDefaultsAndFormatting() {
        assertThat(new GeometryType().asSQLString()).isEqualTo("GEOMETRY(OGC:CRS84)");
        assertThat(new GeographyType().asSQLString()).isEqualTo("GEOGRAPHY(OGC:CRS84, spherical)");
        assertThat(new GeographyType().notNull().asSQLString())
                .isEqualTo("GEOGRAPHY(OGC:CRS84, spherical) NOT NULL");
    }

    @Test
    void testCrsEqualityIsCaseInsensitive() {
        assertThat(new GeometryType("OGC:CRS84")).isEqualTo(new GeometryType("ogc:crs84"));
        assertThat(new GeometryType("OGC:CRS84").hashCode())
                .isEqualTo(new GeometryType("ogc:crs84").hashCode());
        assertThat(new GeographyType("OGC:CRS84", EdgeAlgorithm.KARNEY))
                .isEqualTo(new GeographyType("ogc:crs84", EdgeAlgorithm.KARNEY));
        assertThat(new GeographyType("OGC:CRS84", EdgeAlgorithm.KARNEY))
                .isNotEqualTo(new GeographyType("OGC:CRS84", EdgeAlgorithm.SPHERICAL));
        GeometryType custom = new GeometryType("custom, crs's definition");
        assertThat(DataTypeJsonParser.parseAtomicTypeSQLString(custom.asSQLString()))
                .isEqualTo(custom);
    }

    @Test
    void testInvalidParameters() {
        assertThatThrownBy(() -> new GeometryType(""))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid CRS");
        assertThatThrownBy(() -> EdgeAlgorithm.fromName("rhumb"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid edge interpolation algorithm");
    }

    @Test
    void testOnlyIdenticalGeospatialTypesCanBeCast() {
        assertThat(
                        DataTypeCasts.supportsCast(
                                new GeometryType("OGC:CRS84"),
                                new GeometryType("ogc:crs84").notNull(),
                                true))
                .isTrue();
        assertThat(
                        DataTypeCasts.supportsCast(
                                new GeometryType("OGC:CRS84"), new GeometryType("EPSG:3857"), true))
                .isFalse();
        assertThat(
                        DataTypeCasts.supportsCompatibleCast(
                                new GeographyType("OGC:CRS84", EdgeAlgorithm.SPHERICAL),
                                new GeographyType("OGC:CRS84", EdgeAlgorithm.KARNEY)))
                .isFalse();
    }
}
