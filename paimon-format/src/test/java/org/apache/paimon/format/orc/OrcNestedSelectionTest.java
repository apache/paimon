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

package org.apache.paimon.format.orc;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.GenericMap;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.format.FileFormatFactory;
import org.apache.paimon.format.FormatReaderContext;
import org.apache.paimon.format.FormatWriter;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests nested values after ORC row filtering. */
class OrcNestedSelectionTest {

    @TempDir File folder;

    @ParameterizedTest
    @CsvSource({
        "row, true", "row, false", "array, true", "array, false",
        "map, true", "map, false", "row-array, true", "row-array, false",
        "map-row, true", "map-row, false", "scalar, true", "scalar, false"
    })
    void testNestedValuesAfterSelection(String kind, boolean useSelected) throws Exception {
        DataType payloadType;
        switch (kind) {
            case "row":
                payloadType = DataTypes.ROW(DataTypes.INT());
                break;
            case "array":
                payloadType = DataTypes.ARRAY(DataTypes.INT());
                break;
            case "map":
                payloadType = DataTypes.MAP(DataTypes.INT(), DataTypes.INT());
                break;
            case "row-array":
                payloadType = DataTypes.ROW(DataTypes.ARRAY(DataTypes.INT()));
                break;
            case "map-row":
                payloadType = DataTypes.MAP(DataTypes.INT(), DataTypes.ROW(DataTypes.INT()));
                break;
            default:
                payloadType = DataTypes.INT();
        }
        RowType rowType =
                RowType.builder()
                        .field("id", DataTypes.INT())
                        .field("payload", payloadType)
                        .build();
        InternalRowSerializer serializer = new InternalRowSerializer(rowType);
        List<Integer> selectedIds = Arrays.asList(1, 3, 6, 8, 9, 12, 15);
        Predicate filter = new PredicateBuilder(rowType).in(0, new ArrayList<Object>(selectedIds));
        Options options = new Options();
        options.setString("orc.sarg.to.filter", "true");
        options.setString("orc.filter.use.selected", Boolean.toString(useSelected));
        OrcFileFormat format =
                new OrcFileFormat(new FileFormatFactory.FormatContext(options, 4, 16));
        LocalFileIO fileIO = new LocalFileIO();
        Path file = new Path(folder.toString(), kind + ".orc");
        List<BinaryRow> expected = new ArrayList<>();
        try (PositionOutputStream out = fileIO.newOutputStream(file, false);
                FormatWriter writer = format.createWriterFactory(rowType).create(out, "none")) {
            for (int i = 0; i < 16; i++) {
                GenericRow row = GenericRow.of(i, payload(kind, i));
                writer.addElement(row);
                if (selectedIds.contains(i)) {
                    expected.add(serializer.toBinaryRow(row).copy());
                }
            }
        }
        List<BinaryRow> actual = new ArrayList<>();
        try (RecordReader<InternalRow> reader =
                format.createReaderFactory(rowType, rowType, Collections.singletonList(filter))
                        .createReader(
                                new FormatReaderContext(
                                        fileIO, file, fileIO.getFileSize(file), null, null))) {
            // Format filters are best effort. Apply the residual just as a table/engine read does.
            reader.forEachRemaining(
                    row -> {
                        if (selectedIds.contains(row.getInt(0))) {
                            actual.add(serializer.toBinaryRow(row).copy());
                        }
                    });
        }
        assertThat(actual).containsExactlyElementsOf(expected);
    }

    private static Object payload(String kind, int id) {
        if (id % 5 == 3) {
            return null;
        }
        Integer value = id % 4 == 1 ? null : 100 + id;
        switch (kind) {
            case "row":
                return GenericRow.of(value);
            case "array":
                return new GenericArray(id % 4 == 0 ? new Integer[0] : new Integer[] {value, id});
            case "map":
                return new GenericMap(
                        id % 4 == 0 ? Collections.emptyMap() : Collections.singletonMap(id, value));
            case "row-array":
                return GenericRow.of(new GenericArray(new Integer[] {value, id}));
            case "map-row":
                return new GenericMap(Collections.singletonMap(id, GenericRow.of(value)));
            default:
                return value;
        }
    }
}
