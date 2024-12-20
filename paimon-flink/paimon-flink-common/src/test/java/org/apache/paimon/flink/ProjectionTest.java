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

import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;

import static org.apache.paimon.flink.LogicalTypeConversion.toLogicalType;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link Projection}. */
public class ProjectionTest {
    @Test
    public void testNestedProjection() {
        RowType writeType =
                DataTypes.ROW(
                        DataTypes.FIELD(0, "f0", DataTypes.INT()),
                        DataTypes.FIELD(
                                1,
                                "f1",
                                DataTypes.ROW(
                                        DataTypes.FIELD(2, "f0", DataTypes.INT()),
                                        DataTypes.FIELD(3, "f1", DataTypes.INT()),
                                        DataTypes.FIELD(4, "f2", DataTypes.INT()))));

        // skip read f0, f1.f1
        RowType readType =
                DataTypes.ROW(
                        DataTypes.FIELD(
                                1,
                                "f1",
                                DataTypes.ROW(
                                        DataTypes.FIELD(2, "f0", DataTypes.INT()),
                                        DataTypes.FIELD(4, "f2", DataTypes.INT()))));

        Projection projection = Projection.of(new int[][] {{1, 0}, {1, 2}});
        assertThat(projection.project(writeType)).isEqualTo(readType);

        RowType readTypeForFlink =
                DataTypes.ROW(
                        DataTypes.FIELD(
                                0,
                                "f1",
                                DataTypes.ROW(
                                        DataTypes.FIELD(0, "f0", DataTypes.INT()),
                                        DataTypes.FIELD(1, "f2", DataTypes.INT()))));

        NestedProjectedRowData rowData = projection.getOuterProjectRow(writeType);

        assertThat(rowData.getRowType()).isEqualTo(toLogicalType(readTypeForFlink));

        assertThat(rowData.getProjectedFields()).isEqualTo(new int[][] {{0, 0}, {0, 1}});
    }
}
