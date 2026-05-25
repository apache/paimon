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

package org.apache.paimon.format.vortex;

import org.apache.paimon.table.SpecialFields;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

/** Tests for Vortex records reader schema handling. */
public class VortexRecordsReaderTest {

    @Test
    public void testPhysicalReadRowTypePrunesDataColumnsWithVirtualRowTracking() {
        RowType dataSchemaRowType =
                RowType.builder()
                        .field("f_int", DataTypes.INT())
                        .field("f_string", DataTypes.STRING())
                        .field("f_double", DataTypes.DOUBLE())
                        .build();
        RowType projectedRowType =
                SpecialFields.rowTypeWithRowId(dataSchemaRowType)
                        .project(Arrays.asList("f_string", SpecialFields.ROW_ID.name()));

        RowType physicalReadRowType =
                VortexRecordsReader.physicalReadRowType(dataSchemaRowType, projectedRowType);
        int[] physicalFieldMapping =
                VortexRecordsReader.physicalFieldMapping(physicalReadRowType, projectedRowType);

        assertEquals(1, physicalReadRowType.getFieldCount());
        assertEquals("f_string", physicalReadRowType.getFields().get(0).name());
        assertArrayEquals(new int[] {0, -1}, physicalFieldMapping);
    }

    @Test
    public void testPhysicalFieldMappingPrefersFieldIdOverName() {
        RowType dataSchemaRowType =
                RowType.of(
                        new DataField(1, "old_name", DataTypes.STRING()),
                        new DataField(2, "new_name", DataTypes.INT()));
        RowType projectedRowType =
                RowType.of(new DataField(1, "new_name", DataTypes.STRING()), SpecialFields.ROW_ID);

        RowType physicalReadRowType =
                VortexRecordsReader.physicalReadRowType(dataSchemaRowType, projectedRowType);
        int[] physicalFieldMapping =
                VortexRecordsReader.physicalFieldMapping(physicalReadRowType, projectedRowType);

        assertEquals(1, physicalReadRowType.getFieldCount());
        assertEquals("old_name", physicalReadRowType.getFields().get(0).name());
        assertArrayEquals(new int[] {0, -1}, physicalFieldMapping);
    }
}
