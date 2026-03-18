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

package org.apache.paimon.table.format;

import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.format.FileFormatFactory;
import org.apache.paimon.format.FormatWriter;
import org.apache.paimon.format.FormatWriterFactory;
import org.apache.paimon.format.csv.CsvFileFormat;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.FormatTable;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.InstantiationUtil;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;

/** Test for {@link FormatReadBuilder} serialization. */
public class FormatReadBuilderTest {

    @TempDir java.nio.file.Path tempPath;

    @Test
    public void testSerializeAndDeserialize() throws IOException, ClassNotFoundException {
        RowType rowType =
                RowType.builder()
                        .field("partition_key", DataTypes.STRING())
                        .field("id", DataTypes.BIGINT())
                        .field("data", DataTypes.STRING())
                        .field("timestamp", DataTypes.TIMESTAMP())
                        .build();

        Map<String, String> options = new HashMap<>();
        options.put("file.format", "orc");
        options.put("file.compression", "zstd");

        Path tablePath = new Path(tempPath.toUri());
        FormatTable table =
                FormatTable.builder()
                        .fileIO(LocalFileIO.create())
                        .identifier(Identifier.create("test_db", "complex_table"))
                        .rowType(rowType)
                        .partitionKeys(Arrays.asList("partition_key"))
                        .location(tablePath.toString())
                        .format(FormatTable.Format.ORC)
                        .options(options)
                        .build();

        FormatReadBuilder readBuilder = new FormatReadBuilder(table);

        assertThatNoException().isThrownBy(() -> InstantiationUtil.serializeObject(readBuilder));
        assertThatNoException()
                .isThrownBy(
                        () ->
                                InstantiationUtil.deserializeObject(
                                        InstantiationUtil.serializeObject(readBuilder),
                                        getClass().getClassLoader()));

        // Add multiple filters
        PredicateBuilder predicateBuilder = new PredicateBuilder(rowType);
        Predicate filter1 = predicateBuilder.greaterThan(1, 1000L); // id > 1000
        Predicate filter2 = predicateBuilder.isNotNull(2); // data is not null
        readBuilder.withFilter(filter1);
        readBuilder.withFilter(filter2);

        // Add partition filter
        Map<String, String> partitionSpec = new HashMap<>();
        partitionSpec.put("partition_key", "test_partition");
        readBuilder.withPartitionFilter(partitionSpec);

        // Add projection and limit
        int[] projection = {1, 2, 3}; // project id, data, timestamp
        readBuilder.withProjection(projection);
        readBuilder.withLimit(500);

        // Test Java serialization
        byte[] serialized = InstantiationUtil.serializeObject(readBuilder);
        FormatReadBuilder deserialized =
                InstantiationUtil.deserializeObject(serialized, getClass().getClassLoader());

        // Verify the deserialized object maintains all configurations
        assertThat(deserialized.tableName()).isEqualTo(table.name());

        RowType expectedProjectedType =
                RowType.builder()
                        .field("id", DataTypes.BIGINT())
                        .field("data", DataTypes.STRING())
                        .field("timestamp", DataTypes.TIMESTAMP())
                        .build();
        assertThat(deserialized.readType().getFieldCount())
                .isEqualTo(expectedProjectedType.getFieldCount());
        assertThat(deserialized.readType().getFieldNames())
                .isEqualTo(expectedProjectedType.getFieldNames());
        assertThat(deserialized.readType().getFieldTypes())
                .isEqualTo(expectedProjectedType.getFieldTypes());

        // Verify that scan and read can be created
        assertThat(deserialized.newScan()).isNotNull();
        assertThat(deserialized.newRead()).isNotNull();
    }

    @Test
    public void testCreateReaderWithCsvSplit() throws IOException {
        RowType rowType =
                RowType.builder()
                        .field("id", DataTypes.INT())
                        .field("name", DataTypes.STRING())
                        .field("score", DataTypes.DOUBLE())
                        .build();

        Map<String, String> options = new HashMap<>();
        options.put("file.format", "csv");

        Path tablePath = new Path(tempPath.toUri());
        LocalFileIO fileIO = LocalFileIO.create();
        FormatTable table =
                FormatTable.builder()
                        .fileIO(fileIO)
                        .identifier(Identifier.create("test_db", "csv_table"))
                        .rowType(rowType)
                        .partitionKeys(Arrays.asList())
                        .location(tablePath.toString())
                        .format(FormatTable.Format.CSV)
                        .options(options)
                        .build();

        FormatReadBuilder readBuilder = new FormatReadBuilder(table);

        // Create CSV test data
        Path csvFile = new Path(tablePath, "test_data.csv");
        List<InternalRow> testData =
                Arrays.asList(
                        GenericRow.of(1, BinaryString.fromString("Alice"), 95.5),
                        GenericRow.of(2, BinaryString.fromString("Bob"), 87.3),
                        GenericRow.of(3, BinaryString.fromString("Charlie"), 92.8));

        // Write CSV file
        CsvFileFormat csvFormat =
                new CsvFileFormat(new FileFormatFactory.FormatContext(new Options(), 1024, 1024));
        FormatWriterFactory writerFactory = csvFormat.createWriterFactory(rowType);
        try (PositionOutputStream out = fileIO.newOutputStream(csvFile, false);
                FormatWriter writer = writerFactory.create(out, "none")) {
            for (InternalRow row : testData) {
                writer.addElement(row);
            }
        }

        long fileSize = fileIO.getFileSize(csvFile);

        // Test 1: Read entire CSV file (offset = 0, length = fileSize)
        FormatDataSplit fullSplit = new FormatDataSplit(csvFile, fileSize, null);
        RecordReader<InternalRow> fullReader = readBuilder.createReader(fullSplit);
        List<InternalRow> fullResult = readAllRows(fullReader, rowType);

        assertThat(fullResult).hasSize(3);
        assertThat(fullResult.get(0).getInt(0)).isEqualTo(1);
        assertThat(fullResult.get(0).getString(1).toString()).isEqualTo("Alice");
        assertThat(fullResult.get(0).getDouble(2)).isEqualTo(95.5);
        assertThat(fullResult.get(1).getInt(0)).isEqualTo(2);
        assertThat(fullResult.get(1).getString(1).toString()).isEqualTo("Bob");
        assertThat(fullResult.get(1).getDouble(2)).isEqualTo(87.3);
        assertThat(fullResult.get(2).getInt(0)).isEqualTo(3);
        assertThat(fullResult.get(2).getString(1).toString()).isEqualTo("Charlie");
        assertThat(fullResult.get(2).getDouble(2)).isEqualTo(92.8);

        // Test 2: Read CSV with offset and length (partial read)
        // Read from offset 0 with a limited length (first 2 lines approximately)
        long partialLength = fileSize / 2;
        FormatDataSplit partialSplit =
                new FormatDataSplit(csvFile, fileSize, 0, partialLength, null);
        RecordReader<InternalRow> partialReader = readBuilder.createReader(partialSplit);
        List<InternalRow> partialResult = readAllRows(partialReader, rowType);

        // Verify we get at least some rows (exact count depends on line boundaries)
        assertThat(partialResult).isNotEmpty();
        assertThat(partialResult.get(0).getInt(0)).isEqualTo(1);
        assertThat(partialResult.get(0).getString(1).toString()).isEqualTo("Alice");
    }

    private List<InternalRow> readAllRows(RecordReader<InternalRow> reader, RowType rowType)
            throws IOException {
        InternalRowSerializer serializer = new InternalRowSerializer(rowType);
        List<InternalRow> result = new ArrayList<>();
        try (RecordReader<InternalRow> r = reader) {
            r.forEachRemaining(row -> result.add(serializer.copy(row)));
        }
        return result;
    }
}
