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

package org.apache.paimon.format.mosaic;

import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.format.FileFormatFactory;
import org.apache.paimon.format.FormatWriter;
import org.apache.paimon.format.FormatWriterFactory;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.options.Options;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.FormatTable;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/** End-to-end tests for a {@link FormatTable} backed by mosaic files. */
class FormatTableMosaicReadTest {

    @TempDir java.nio.file.Path tempPath;

    @BeforeAll
    static void checkNativeLibrary() {
        assumeTrue(isNativeAvailable(), "Mosaic native library not available");
    }

    @Test
    void testReadUnpartitioned() throws Exception {
        RowType rowType =
                RowType.builder()
                        .field("a", DataTypes.STRING())
                        .field("b", DataTypes.BIGINT())
                        .field("c", DataTypes.DOUBLE())
                        .build();

        Path tablePath = new Path(tempPath.toUri());
        LocalFileIO fileIO = LocalFileIO.create();
        FormatTable table =
                buildFormatTable(
                        fileIO, tablePath, rowType, Collections.emptyList(), new HashMap<>());

        List<InternalRow> rows =
                Arrays.asList(
                        GenericRow.of(BinaryString.fromString("a1"), 1L, 1.1),
                        GenericRow.of(BinaryString.fromString("a2"), 2L, 2.2),
                        GenericRow.of(BinaryString.fromString("a3"), 3L, 3.3));
        writeMosaicFile(fileIO, new Path(tablePath, "data-0.mosaic"), rowType, rows);

        List<InternalRow> result = readAll(table, rowType);

        assertThat(result).hasSize(3);
        assertThat(result.get(0).getString(0).toString()).isEqualTo("a1");
        assertThat(result.get(0).getLong(1)).isEqualTo(1L);
        assertThat(result.get(0).getDouble(2)).isEqualTo(1.1);
        assertThat(result.get(2).getString(0).toString()).isEqualTo("a3");
    }

    @Test
    void testReadPartitioned() throws Exception {
        // Schema lays out the partition column last, following the FormatTable convention.
        RowType rowType =
                RowType.builder()
                        .field("a", DataTypes.STRING())
                        .field("b", DataTypes.BIGINT())
                        .field("dt", DataTypes.STRING())
                        .build();
        // Data fields only — partition value comes from the directory name.
        RowType dataRowType =
                RowType.builder()
                        .field("a", DataTypes.STRING())
                        .field("b", DataTypes.BIGINT())
                        .build();

        Path tablePath = new Path(tempPath.toUri());
        LocalFileIO fileIO = LocalFileIO.create();
        FormatTable table =
                buildFormatTable(
                        fileIO,
                        tablePath,
                        rowType,
                        Collections.singletonList("dt"),
                        new HashMap<>());

        writeMosaicFile(
                fileIO,
                new Path(tablePath, "dt=20260608/data-0.mosaic"),
                dataRowType,
                Arrays.asList(
                        GenericRow.of(BinaryString.fromString("a1"), 1L),
                        GenericRow.of(BinaryString.fromString("a2"), 2L)));
        writeMosaicFile(
                fileIO,
                new Path(tablePath, "dt=20260609/data-0.mosaic"),
                dataRowType,
                Collections.singletonList(GenericRow.of(BinaryString.fromString("a3"), 3L)));

        List<InternalRow> result = readAll(table, rowType);

        assertThat(result).hasSize(3);
        // Partition column is appended by the scan; the (a, dt) pairs are deterministic per file.
        List<String> aDt = new ArrayList<>();
        for (InternalRow row : result) {
            aDt.add(row.getString(0).toString() + "/" + row.getString(2).toString());
        }
        assertThat(aDt).containsExactlyInAnyOrder("a1/20260608", "a2/20260608", "a3/20260609");
    }

    private static FormatTable buildFormatTable(
            LocalFileIO fileIO,
            Path tablePath,
            RowType rowType,
            List<String> partitionKeys,
            Map<String, String> options) {
        options.put("file.format", "mosaic");
        return FormatTable.builder()
                .fileIO(fileIO)
                .identifier(Identifier.create("default", "t"))
                .rowType(rowType)
                .partitionKeys(partitionKeys)
                .location(tablePath.toString())
                .format(FormatTable.Format.MOSAIC)
                .options(options)
                .build();
    }

    private static void writeMosaicFile(
            LocalFileIO fileIO, Path file, RowType rowType, List<InternalRow> rows)
            throws Exception {
        fileIO.mkdirs(file.getParent());
        MosaicFileFormat mosaic =
                new MosaicFileFormat(
                        new FileFormatFactory.FormatContext(new Options(), 1024, 1024));
        FormatWriterFactory writerFactory = mosaic.createWriterFactory(rowType);
        try (PositionOutputStream out = fileIO.newOutputStream(file, false);
                FormatWriter writer = writerFactory.create(out, "zstd")) {
            for (InternalRow row : rows) {
                writer.addElement(row);
            }
        }
    }

    private static List<InternalRow> readAll(FormatTable table, RowType rowType) throws Exception {
        ReadBuilder readBuilder = table.newReadBuilder();
        List<Split> splits = readBuilder.newScan().plan().splits();
        assertThat(splits).isNotEmpty();

        List<InternalRow> result = new ArrayList<>();
        InternalRowSerializer serializer = new InternalRowSerializer(rowType);
        for (Split split : splits) {
            try (RecordReader<InternalRow> reader = readBuilder.newRead().createReader(split)) {
                reader.forEachRemaining(row -> result.add(serializer.copy(row)));
            }
        }
        return result;
    }

    private static boolean isNativeAvailable() {
        try {
            Class.forName("org.apache.paimon.mosaic.NativeLib");
            return true;
        } catch (Throwable t) {
            return false;
        }
    }
}
