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

package org.apache.paimon.format.parquet;

import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.format.FormatReaderContext;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.options.Options;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test case-insensitive column matching in {@link ParquetReaderFactory}. */
class ParquetCaseInsensitiveReadTest {

    @TempDir File folder;

    @Test
    void testCaseInsensitiveColumnMatching() throws Exception {
        Path path = writeMixedCaseParquet();

        // Read with lowercase column names + caseSensitive=false.
        RowType readType =
                RowType.builder()
                        .field("event_name", DataTypes.STRING())
                        .field("campaign_id", DataTypes.STRING())
                        .field("amount", DataTypes.BIGINT())
                        .build();

        List<InternalRow> rows = read(path, readType, false);

        assertThat(rows).hasSize(3);
        assertThat(rows.get(0).getString(0).toString()).isEqualTo("install");
        assertThat(rows.get(0).getString(1).toString()).isEqualTo("c001");
        assertThat(rows.get(0).getLong(2)).isEqualTo(100L);
    }

    @Test
    void testCaseSensitiveReturnsNull() throws Exception {
        Path path = writeMixedCaseParquet();

        RowType readType =
                RowType.builder()
                        .field("event_name", DataTypes.STRING())
                        .field("amount", DataTypes.BIGINT())
                        .build();

        // caseSensitive=true: lowercase names won't match mixed-case parquet columns.
        List<InternalRow> rows = read(path, readType, true);

        assertThat(rows).hasSize(3);
        assertThat(rows.get(0).isNullAt(0)).isTrue();
        assertThat(rows.get(0).isNullAt(1)).isTrue();
    }

    @Test
    void testNestedCaseInsensitiveColumnMatching() throws Exception {
        Path path = writeNestedMixedCaseParquet();

        // Lowercase top-level and nested field names + caseSensitive=false.
        RowType readType =
                RowType.builder()
                        .field("event_name", DataTypes.STRING())
                        .field(
                                "user_info",
                                RowType.builder()
                                        .field("user_name", DataTypes.STRING())
                                        .field("user_age", DataTypes.BIGINT())
                                        .build())
                        .build();

        List<InternalRow> rows = read(path, readType, false);

        assertThat(rows).hasSize(2);
        assertThat(rows.get(0).getString(0).toString()).isEqualTo("install");
        InternalRow nested = rows.get(0).getRow(1, 2);
        assertThat(nested.getString(0).toString()).isEqualTo("alice");
        assertThat(nested.getLong(1)).isEqualTo(20L);
    }

    @Test
    void testAmbiguousCaseInsensitiveMatchFails() throws Exception {
        Path path = writeDuplicateCaseParquet();

        RowType readType = RowType.builder().field("col", DataTypes.STRING()).build();

        assertThatThrownBy(() -> read(path, readType, false))
                .hasMessageContaining("Found duplicate field(s)")
                .hasMessageContaining("col");
    }

    private List<InternalRow> read(Path path, RowType readType, boolean caseSensitive)
            throws Exception {
        Options options = new Options();
        options.set(CatalogOptions.CASE_SENSITIVE, caseSensitive);
        ParquetReaderFactory factory = new ParquetReaderFactory(options, readType, 1024, null);
        LocalFileIO fileIO = new LocalFileIO();
        List<InternalRow> rows = new ArrayList<>();
        try (RecordReader<InternalRow> reader =
                factory.createReader(
                        new FormatReaderContext(fileIO, path, fileIO.getFileSize(path)))) {
            // Materialize a copy because InternalRow instances are reused across iterations.
            reader.forEachRemaining(row -> rows.add(copy(row, readType)));
        }
        return rows;
    }

    private static InternalRow copy(InternalRow row, RowType rowType) {
        Object[] values = new Object[rowType.getFieldCount()];
        for (int i = 0; i < values.length; i++) {
            if (row.isNullAt(i)) {
                continue;
            }
            switch (rowType.getTypeAt(i).getTypeRoot()) {
                case VARCHAR:
                case CHAR:
                    values[i] = row.getString(i).copy();
                    break;
                case BIGINT:
                    values[i] = row.getLong(i);
                    break;
                case ROW:
                    RowType child = (RowType) rowType.getTypeAt(i);
                    values[i] = copy(row.getRow(i, child.getFieldCount()), child);
                    break;
                default:
                    throw new UnsupportedOperationException(
                            "Unhandled type in test: " + rowType.getTypeAt(i));
            }
        }
        return GenericRow.of(values);
    }

    private Path writeMixedCaseParquet() throws Exception {
        MessageType schema =
                MessageTypeParser.parseMessageType(
                        "message root {\n"
                                + "  optional binary Event_Name (UTF8);\n"
                                + "  optional binary Campaign_ID (UTF8);\n"
                                + "  optional int64 Amount;\n"
                                + "}");

        String[][] data = {{"install", "c001"}, {"purchase", "c002"}, {"login", "c001"}};
        long[] amounts = {100L, 200L, 300L};
        return write(
                schema,
                factory -> {
                    List<Group> groups = new ArrayList<>();
                    for (int i = 0; i < data.length; i++) {
                        groups.add(
                                factory.newGroup()
                                        .append("Event_Name", data[i][0])
                                        .append("Campaign_ID", data[i][1])
                                        .append("Amount", amounts[i]));
                    }
                    return groups;
                });
    }

    private Path writeNestedMixedCaseParquet() throws Exception {
        MessageType schema =
                MessageTypeParser.parseMessageType(
                        "message root {\n"
                                + "  optional binary Event_Name (UTF8);\n"
                                + "  optional group User_Info {\n"
                                + "    optional binary User_Name (UTF8);\n"
                                + "    optional int64 User_Age;\n"
                                + "  }\n"
                                + "}");

        String[][] data = {{"install", "alice", "20"}, {"login", "bob", "30"}};
        return write(
                schema,
                factory -> {
                    List<Group> groups = new ArrayList<>();
                    for (String[] row : data) {
                        Group g = factory.newGroup().append("Event_Name", row[0]);
                        g.addGroup("User_Info")
                                .append("User_Name", row[1])
                                .append("User_Age", Long.parseLong(row[2]));
                        groups.add(g);
                    }
                    return groups;
                });
    }

    private Path writeDuplicateCaseParquet() throws Exception {
        // An exact match ("col") coexists with a case-variant ("COL"); reading "col" must still be
        // ambiguous in case-insensitive mode, mirroring Spark.
        MessageType schema =
                MessageTypeParser.parseMessageType(
                        "message root {\n"
                                + "  optional binary col (UTF8);\n"
                                + "  optional binary COL (UTF8);\n"
                                + "}");

        return write(
                schema,
                factory ->
                        Collections.singletonList(
                                factory.newGroup().append("col", "a").append("COL", "b")));
    }

    private interface GroupSupplier {
        List<Group> get(SimpleGroupFactory factory);
    }

    private Path write(MessageType schema, GroupSupplier supplier) throws Exception {
        Path path = new Path(folder.getPath(), UUID.randomUUID().toString());
        Configuration conf = new Configuration();
        try (ParquetWriter<Group> writer =
                ExampleParquetWriter.builder(
                                HadoopOutputFile.fromPath(
                                        new org.apache.hadoop.fs.Path(path.toString()), conf))
                        .withType(schema)
                        .withConf(conf)
                        .withWriterVersion(ParquetProperties.WriterVersion.PARQUET_1_0)
                        .build()) {
            for (Group group : supplier.get(new SimpleGroupFactory(schema))) {
                writer.write(group);
            }
        }
        return path;
    }
}
