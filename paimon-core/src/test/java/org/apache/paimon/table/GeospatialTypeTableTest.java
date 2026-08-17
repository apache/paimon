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

package org.apache.paimon.table;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.stats.SimpleStats;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.EdgeAlgorithm;

import org.junit.jupiter.api.Test;

import java.util.Comparator;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests table read and write with Iceberg-compatible geospatial types. */
public class GeospatialTypeTableTest extends TableTestBase {

    private static final byte[] POINT_1_2_WKB =
            new byte[] {
                1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, (byte) 0xf0, 0x3f, 0, 0, 0, 0, 0, 0, 0, 0x40
            };

    private static final byte[] POINT_3_4_WKB =
            new byte[] {1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0x08, 0x40, 0, 0, 0, 0, 0, 0, 0x10, 0x40};

    @Test
    public void testReadWriteAndStats() throws Exception {
        createTableDefault();
        FileStoreTable table = getTableDefault();

        assertThat(table.schema().fields().get(1).type())
                .isEqualTo(DataTypes.GEOMETRY("EPSG:3857"));
        assertThat(table.schema().fields().get(2).type())
                .isEqualTo(DataTypes.GEOGRAPHY("OGC:CRS84", EdgeAlgorithm.KARNEY));

        write(
                table,
                GenericRow.of(
                        1,
                        POINT_1_2_WKB,
                        POINT_3_4_WKB,
                        new GenericArray(new Object[] {POINT_1_2_WKB, null, POINT_3_4_WKB})),
                GenericRow.of(2, null, POINT_1_2_WKB, new GenericArray(new Object[0])),
                GenericRow.of(3, POINT_3_4_WKB, null, null));

        List<InternalRow> rows = read(table);
        rows.sort(Comparator.comparingInt(row -> row.getInt(0)));

        assertThat(rows).hasSize(3);
        assertThat(rows.get(0).getBinary(1)).isEqualTo(POINT_1_2_WKB);
        assertThat(rows.get(0).getBinary(2)).isEqualTo(POINT_3_4_WKB);
        InternalArray geometries = rows.get(0).getArray(3);
        assertThat(geometries.size()).isEqualTo(3);
        assertThat(geometries.getBinary(0)).isEqualTo(POINT_1_2_WKB);
        assertThat(geometries.isNullAt(1)).isTrue();
        assertThat(geometries.getBinary(2)).isEqualTo(POINT_3_4_WKB);

        assertThat(rows.get(1).isNullAt(1)).isTrue();
        assertThat(rows.get(1).getBinary(2)).isEqualTo(POINT_1_2_WKB);
        assertThat(rows.get(1).getArray(3).size()).isZero();
        assertThat(rows.get(2).getBinary(1)).isEqualTo(POINT_3_4_WKB);
        assertThat(rows.get(2).isNullAt(2)).isTrue();
        assertThat(rows.get(2).isNullAt(3)).isTrue();

        DataSplit split = (DataSplit) table.newScan().plan().splits().get(0);
        assertThat(split.dataFiles()).hasSize(1);
        DataFileMeta file = split.dataFiles().get(0);
        assertThat(file.fileFormat()).isEqualTo(CoreOptions.FILE_FORMAT_PARQUET);

        SimpleStats stats = file.valueStats();
        assertThat(stats.minValues().isNullAt(1)).isTrue();
        assertThat(stats.maxValues().isNullAt(1)).isTrue();
        assertThat(stats.nullCounts().getLong(1)).isEqualTo(1L);
        assertThat(stats.minValues().isNullAt(2)).isTrue();
        assertThat(stats.maxValues().isNullAt(2)).isTrue();
        assertThat(stats.nullCounts().getLong(2)).isEqualTo(1L);
    }

    @Override
    protected Schema schemaDefault() {
        return Schema.newBuilder()
                .column("id", DataTypes.INT())
                .column("geom", DataTypes.GEOMETRY("EPSG:3857"))
                .column("geog", DataTypes.GEOGRAPHY("OGC:CRS84", EdgeAlgorithm.KARNEY))
                .column("geometries", DataTypes.ARRAY(DataTypes.GEOMETRY()))
                .option(CoreOptions.FILE_FORMAT.key(), CoreOptions.FILE_FORMAT_PARQUET)
                .build();
    }
}
