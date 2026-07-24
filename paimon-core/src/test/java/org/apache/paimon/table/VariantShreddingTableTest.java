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
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.variant.GenericVariant;
import org.apache.paimon.data.variant.VariantMetadataUtils;
import org.apache.paimon.format.parquet.ParquetUtil;
import org.apache.paimon.format.parquet.VariantShreddingReadPlanFactory;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataFilePathFactory;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.apache.paimon.data.variant.PaimonShreddingUtils.variantShreddingSchema;
import static org.assertj.core.api.Assertions.assertThat;

/** Table-level tests for adaptive Variant shredding. */
public class VariantShreddingTableTest extends TableTestBase {

    @Test
    public void testAdaptiveInferenceAcrossRollingFiles() throws Exception {
        catalog.createTable(
                identifier(),
                adaptiveTableBuilder(10, 10, 10, 0.4, 0.2)
                        .column("payload", DataTypes.VARIANT())
                        .build(),
                false);
        FileStoreTable table = (FileStoreTable) catalog.getTable(identifier());

        List<InternalRow> rows = new ArrayList<>();
        Map<Integer, String> expected = new LinkedHashMap<>();
        for (int i = 0; i < 10; i++) {
            String json =
                    i < 5
                            ? "{\"legacy\":\"value\",\"stable\":" + i + "}"
                            : "{\"stable\":" + i + "}";
            rows.add(GenericRow.of(i, GenericVariant.fromJson(json)));
            expected.put(i, json);
        }
        for (int i = 0; i < 10; i++) {
            int id = i + 10;
            String json =
                    i < 9 ? "{\"emerging\":true,\"stable\":" + id + "}" : "{\"stable\":" + id + "}";
            rows.add(GenericRow.of(id, GenericVariant.fromJson(json)));
            expected.put(id, json);
        }
        for (int i = 0; i < 10; i++) {
            int id = i + 20;
            String json = "{\"stable\":" + id + "}";
            rows.add(GenericRow.of(id, GenericVariant.fromJson(json)));
            expected.put(id, json);
        }
        write(table, rows.toArray(new InternalRow[0]));

        List<DataFileWithSplit> files = currentDataFiles(table);
        files.sort(Comparator.comparingLong(file -> file.dataFile.minSequenceNumber()));
        assertThat(files).hasSize(3);

        assertThat(readVariantFileType(table, files.get(0), "payload"))
                .isEqualTo(
                        variantShreddingSchema(
                                RowType.of(
                                        new DataType[] {DataTypes.STRING(), DataTypes.BIGINT()},
                                        new String[] {"legacy", "stable"})));
        assertThat(readVariantFileType(table, files.get(1), "payload"))
                .isEqualTo(
                        variantShreddingSchema(
                                RowType.of(
                                        new DataType[] {
                                            DataTypes.BOOLEAN(),
                                            DataTypes.STRING(),
                                            DataTypes.BIGINT()
                                        },
                                        new String[] {"emerging", "legacy", "stable"})));
        assertThat(readVariantFileType(table, files.get(2), "payload"))
                .isEqualTo(
                        variantShreddingSchema(
                                RowType.of(
                                        new DataType[] {DataTypes.BOOLEAN(), DataTypes.BIGINT()},
                                        new String[] {"emerging", "stable"})));

        Map<Integer, String> actual = new LinkedHashMap<>();
        for (InternalRow row : read(table)) {
            actual.put(row.getInt(0), row.getVariant(1).toJson());
        }
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void testAdaptiveInferenceWithMultipleVariantFields() throws Exception {
        catalog.createTable(
                identifier(),
                adaptiveTableBuilder(2, 2, 2, 0.4, 0.2)
                        .column("left_payload", DataTypes.VARIANT())
                        .column("right_payload", DataTypes.VARIANT())
                        .build(),
                false);
        FileStoreTable table = (FileStoreTable) catalog.getTable(identifier());

        String left1 = "{\"legacy\":\"a\",\"stable\":1}";
        String right1 = "{\"sparse\":\"x\",\"stable\":\"a\"}";
        String left2 = "{\"legacy\":\"b\",\"stable\":2}";
        String right2 = "{\"stable\":\"b\"}";
        String left3 = "{\"emerging\":true,\"stable\":3}";
        String right3 = "{\"stable\":\"c\"}";
        String left4 = "{\"emerging\":false,\"stable\":4}";
        String right4 = "{\"emerging\":true,\"stable\":\"d\"}";
        write(
                table,
                GenericRow.of(1, GenericVariant.fromJson(left1), GenericVariant.fromJson(right1)),
                GenericRow.of(2, GenericVariant.fromJson(left2), GenericVariant.fromJson(right2)),
                GenericRow.of(3, GenericVariant.fromJson(left3), GenericVariant.fromJson(right3)),
                GenericRow.of(4, GenericVariant.fromJson(left4), GenericVariant.fromJson(right4)));

        List<DataFileWithSplit> files = currentDataFiles(table);
        files.sort(Comparator.comparingLong(file -> file.dataFile.minSequenceNumber()));
        assertThat(files).hasSize(2);

        assertThat(readVariantFileType(table, files.get(0), "left_payload"))
                .isEqualTo(
                        variantShreddingSchema(
                                RowType.of(
                                        new DataType[] {DataTypes.STRING(), DataTypes.BIGINT()},
                                        new String[] {"legacy", "stable"})));
        assertThat(readVariantFileType(table, files.get(1), "left_payload"))
                .isEqualTo(
                        variantShreddingSchema(
                                RowType.of(
                                        new DataType[] {
                                            DataTypes.BOOLEAN(),
                                            DataTypes.STRING(),
                                            DataTypes.BIGINT()
                                        },
                                        new String[] {"emerging", "legacy", "stable"})));
        RowType rightSchema =
                variantShreddingSchema(
                        RowType.of(
                                new DataType[] {DataTypes.STRING(), DataTypes.STRING()},
                                new String[] {"sparse", "stable"}));
        assertThat(readVariantFileType(table, files.get(0), "right_payload"))
                .isEqualTo(rightSchema);
        assertThat(readVariantFileType(table, files.get(1), "right_payload"))
                .isEqualTo(rightSchema);

        Map<Integer, String> actual = new LinkedHashMap<>();
        for (InternalRow row : read(table)) {
            actual.put(
                    row.getInt(0), row.getVariant(1).toJson() + "|" + row.getVariant(2).toJson());
        }
        assertThat(actual)
                .containsEntry(1, left1 + "|" + right1)
                .containsEntry(2, left2 + "|" + right2)
                .containsEntry(3, left3 + "|" + right3)
                .containsEntry(4, left4 + "|" + right4);
    }

    @Test
    public void testAdaptiveInferenceWithNestedVariant() throws Exception {
        RowType nestedType =
                DataTypes.ROW(
                        DataTypes.FIELD(2, "label", DataTypes.STRING()),
                        DataTypes.FIELD(3, "payload", DataTypes.VARIANT()));
        catalog.createTable(
                identifier(),
                adaptiveTableBuilder(2, 2, 2, 0.4, 0.2).column("nested", nestedType).build(),
                false);
        FileStoreTable table = (FileStoreTable) catalog.getTable(identifier());

        String first = "{\"legacy\":\"a\",\"stable\":1}";
        String second = "{\"legacy\":\"b\",\"stable\":2}";
        String third = "{\"emerging\":true,\"stable\":3}";
        String fourth = "{\"emerging\":false,\"stable\":4}";
        write(
                table,
                GenericRow.of(
                        1,
                        GenericRow.of(
                                BinaryString.fromString("first"), GenericVariant.fromJson(first))),
                GenericRow.of(
                        2,
                        GenericRow.of(
                                BinaryString.fromString("second"),
                                GenericVariant.fromJson(second))),
                GenericRow.of(
                        3,
                        GenericRow.of(
                                BinaryString.fromString("third"), GenericVariant.fromJson(third))),
                GenericRow.of(
                        4,
                        GenericRow.of(
                                BinaryString.fromString("fourth"),
                                GenericVariant.fromJson(fourth))));

        List<DataFileWithSplit> files = currentDataFiles(table);
        files.sort(Comparator.comparingLong(file -> file.dataFile.minSequenceNumber()));
        assertThat(files).hasSize(2);

        assertThat(readVariantFileType(table, files.get(0), "nested", "payload"))
                .isEqualTo(
                        variantShreddingSchema(
                                RowType.of(
                                        new DataType[] {DataTypes.STRING(), DataTypes.BIGINT()},
                                        new String[] {"legacy", "stable"})));
        assertThat(readVariantFileType(table, files.get(1), "nested", "payload"))
                .isEqualTo(
                        variantShreddingSchema(
                                RowType.of(
                                        new DataType[] {
                                            DataTypes.BOOLEAN(),
                                            DataTypes.STRING(),
                                            DataTypes.BIGINT()
                                        },
                                        new String[] {"emerging", "legacy", "stable"})));

        Map<Integer, String> actual = new LinkedHashMap<>();
        for (InternalRow row : read(table)) {
            InternalRow nested = row.getRow(1, nestedType.getFieldCount());
            actual.put(
                    row.getInt(0),
                    nested.getString(0).toString() + "|" + nested.getVariant(1).toJson());
        }
        assertThat(actual)
                .containsEntry(1, "first|" + first)
                .containsEntry(2, "second|" + second)
                .containsEntry(3, "third|" + third)
                .containsEntry(4, "fourth|" + fourth);
    }

    private List<DataFileWithSplit> currentDataFiles(FileStoreTable table) throws Exception {
        List<DataFileWithSplit> files = new ArrayList<>();
        for (DataSplit split : table.newSnapshotReader().read().dataSplits()) {
            for (DataFileMeta dataFile : split.dataFiles()) {
                files.add(new DataFileWithSplit(split.partition(), split.bucket(), dataFile));
            }
        }
        return files;
    }

    private RowType readVariantFileType(
            FileStoreTable table, DataFileWithSplit file, String... fieldPath) throws Exception {
        DataFilePathFactory pathFactory =
                table.store().pathFactory().createDataFilePathFactory(file.partition, file.bucket);
        try (ParquetFileReader reader =
                ParquetUtil.getParquetReader(
                        table.fileIO(),
                        pathFactory.toPath(file.dataFile),
                        file.dataFile.fileSize(),
                        new Options())) {
            MessageType schema = reader.getFooter().getFileMetaData().getSchema();
            Type variantType = schema;
            for (String fieldName : fieldPath) {
                variantType = variantType.asGroupType().getType(fieldName);
            }
            return VariantMetadataUtils.addVariantMetadata(
                    VariantShreddingReadPlanFactory.variantFileType(variantType));
        }
    }

    private Schema.Builder adaptiveTableBuilder(
            long targetFileRowNum,
            int initialSampleRows,
            int adaptiveSampleRows,
            double admissionRatio,
            double retentionRatio) {
        return Schema.newBuilder()
                .column("id", DataTypes.INT())
                .option(CoreOptions.BUCKET.key(), "-1")
                .option(CoreOptions.FILE_FORMAT.key(), CoreOptions.FILE_FORMAT_PARQUET)
                .option(CoreOptions.WRITE_ONLY.key(), "true")
                .option(CoreOptions.TARGET_FILE_ROW_NUM.key(), String.valueOf(targetFileRowNum))
                .option(CoreOptions.VARIANT_INFER_SHREDDING_SCHEMA.key(), "true")
                .option(CoreOptions.VARIANT_SHREDDING_INFERENCE_MODE.key(), "adaptive")
                .option(
                        CoreOptions.VARIANT_SHREDDING_MAX_INFER_BUFFER_ROW.key(),
                        String.valueOf(initialSampleRows))
                .option(
                        CoreOptions.VARIANT_SHREDDING_ADAPTIVE_MAX_INFER_BUFFER_ROW.key(),
                        String.valueOf(adaptiveSampleRows))
                .option(
                        CoreOptions.VARIANT_SHREDDING_MIN_FIELD_CARDINALITY_RATIO.key(),
                        String.valueOf(admissionRatio))
                .option(
                        CoreOptions.VARIANT_SHREDDING_ADAPTIVE_RETENTION_RATIO.key(),
                        String.valueOf(retentionRatio));
    }

    private static class DataFileWithSplit {

        private final BinaryRow partition;
        private final int bucket;
        private final DataFileMeta dataFile;

        private DataFileWithSplit(BinaryRow partition, int bucket, DataFileMeta dataFile) {
            this.partition = partition;
            this.bucket = bucket;
            this.dataFile = dataFile;
        }
    }
}
