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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericMap;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.format.FormatWriter;
import org.apache.paimon.format.parquet.writer.RowDataParquetBuilder;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.options.Options;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.VarCharType;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/** Benchmark for map shredding storage size. */
public class MapShreddingStorageBenchmark {

    private static final String MAP_FIELD = "headers";
    private static final String MAP_SHREDDING_COLUMNS = "map.shredding.columns";
    private static final String MAP_SHREDDING_MAX_KEYS = "map.shredding.maxKeys";
    private static final String MAP_SHREDDING_MAX_INFER_BUFFER_MEMORY =
            "map.shredding.maxInferBufferMemory";
    private static final String MAP_SHREDDING_MAX_INFER_BUFFER_ROW =
            "map.shredding.maxInferBufferRow";
    private static final int DEFAULT_ROW_COUNT = 100_000;
    private static final int DEFAULT_HOT_KEY_COUNT = 32;
    private static final int DEFAULT_VALUE_LENGTH = 16;
    private static final int DEFAULT_KEY_PADDING_LENGTH = 128;
    private static final int DEFAULT_VALUE_RUN_LENGTH = 128;
    private static final int DEFAULT_VALUE_CARDINALITY = 4;
    private static final VarCharType STRING = new VarCharType(VarCharType.MAX_LENGTH);
    private static final RowType ROW_TYPE =
            RowType.builder()
                    .field("id", new IntType())
                    .field(MAP_FIELD, new MapType(STRING, STRING))
                    .build();

    @TempDir File folder;

    @Test
    public void benchmarkLongHotKeyStorageSize() throws Exception {
        int rowCount = intProperty("paimon.benchmark.map-shredding.rows", DEFAULT_ROW_COUNT);
        int hotKeyCount =
                intProperty("paimon.benchmark.map-shredding.hot-keys", DEFAULT_HOT_KEY_COUNT);
        int valueLength =
                intProperty("paimon.benchmark.map-shredding.value-length", DEFAULT_VALUE_LENGTH);
        int keyPaddingLength =
                intProperty(
                        "paimon.benchmark.map-shredding.key-padding-length",
                        DEFAULT_KEY_PADDING_LENGTH);

        assertStorageSaving(
                "Map shredding long header key storage benchmark",
                Scenario.LONG_HOT_KEYS,
                rowCount,
                hotKeyCount,
                valueLength,
                keyPaddingLength,
                1,
                1,
                false);
    }

    @Test
    public void benchmarkColumnarValueStorageSize() throws Exception {
        int rowCount = intProperty("paimon.benchmark.map-shredding.rows", DEFAULT_ROW_COUNT);
        int hotKeyCount =
                intProperty("paimon.benchmark.map-shredding.hot-keys", DEFAULT_HOT_KEY_COUNT);
        int valueLength =
                intProperty("paimon.benchmark.map-shredding.value-length", DEFAULT_VALUE_LENGTH);
        int valueRunLength =
                intProperty(
                        "paimon.benchmark.map-shredding.value-run-length",
                        DEFAULT_VALUE_RUN_LENGTH);
        int valueCardinality =
                intProperty(
                        "paimon.benchmark.map-shredding.value-cardinality",
                        DEFAULT_VALUE_CARDINALITY);

        assertStorageSaving(
                "Map shredding columnar header value storage benchmark",
                Scenario.COLUMNAR_VALUES,
                rowCount,
                hotKeyCount,
                valueLength,
                0,
                valueRunLength,
                valueCardinality,
                true);
    }

    private int intProperty(String key, int defaultValue) {
        String value = System.getProperties().getProperty(key);
        return value == null ? defaultValue : Integer.parseInt(value);
    }

    private void assertStorageSaving(
            String benchmarkName,
            Scenario scenario,
            int rowCount,
            int hotKeyCount,
            int valueLength,
            int keyPaddingLength,
            int valueRunLength,
            int valueCardinality,
            boolean dictionaryEnabled)
            throws Exception {
        long regularSize =
                writeFile(
                        false,
                        scenario,
                        rowCount,
                        hotKeyCount,
                        valueLength,
                        keyPaddingLength,
                        valueRunLength,
                        valueCardinality,
                        dictionaryEnabled);
        long mapShreddingSize =
                writeFile(
                        true,
                        scenario,
                        rowCount,
                        hotKeyCount,
                        valueLength,
                        keyPaddingLength,
                        valueRunLength,
                        valueCardinality,
                        dictionaryEnabled);
        long savedBytes = regularSize - mapShreddingSize;
        double savedPercent = savedBytes * 100.0D / regularSize;

        System.out.printf(
                Locale.ROOT,
                "%s: rows=%d, hotKeys=%d, valueLength=%d, keyPaddingLength=%d,"
                        + " valueRunLength=%d, valueCardinality=%d, dictionary=%s,"
                        + " regular=%d bytes, mapShredding=%d bytes, saved=%d bytes (%.2f%%)%n",
                benchmarkName,
                rowCount,
                hotKeyCount,
                valueLength,
                keyPaddingLength,
                valueRunLength,
                valueCardinality,
                dictionaryEnabled,
                regularSize,
                mapShreddingSize,
                savedBytes,
                savedPercent);

        assertThat(mapShreddingSize).isLessThan(regularSize);
    }

    private Options options(boolean mapShredding, int hotKeyCount, boolean dictionaryEnabled) {
        Options options = new Options();
        options.set(CoreOptions.FILE_FORMAT, CoreOptions.FILE_FORMAT_PARQUET);
        options.setInteger("parquet.block.size", 256 * 1024 * 1024);
        options.set("parquet.enable.dictionary", Boolean.toString(dictionaryEnabled));
        if (mapShredding) {
            options.set(MAP_SHREDDING_COLUMNS, MAP_FIELD);
            options.setInteger(MAP_SHREDDING_MAX_KEYS, hotKeyCount);
            options.setInteger(MAP_SHREDDING_MAX_INFER_BUFFER_ROW, 10_000);
            options.set(MAP_SHREDDING_MAX_INFER_BUFFER_MEMORY, "64 mb");
        }
        return options;
    }

    private long writeFile(
            boolean mapShredding,
            Scenario scenario,
            int rowCount,
            int hotKeyCount,
            int valueLength,
            int keyPaddingLength,
            int valueRunLength,
            int valueCardinality,
            boolean dictionaryEnabled)
            throws Exception {
        Options options = options(mapShredding, hotKeyCount, dictionaryEnabled);
        Path path =
                new Path(
                        folder.getPath(),
                        (mapShredding ? "map-shredding-" : "regular-") + UUID.randomUUID());
        ParquetWriterFactory factory =
                new ParquetWriterFactory(new RowDataParquetBuilder(ROW_TYPE, options));
        LocalFileIO fileIO = new LocalFileIO();
        try (FormatWriter writer = factory.create(fileIO.newOutputStream(path, false), "snappy")) {
            for (int rowId = 0; rowId < rowCount; rowId++) {
                writer.addElement(
                        row(
                                scenario,
                                rowId,
                                hotKeyCount,
                                valueLength,
                                keyPaddingLength,
                                valueRunLength,
                                valueCardinality));
            }
        }
        return fileIO.getFileSize(path);
    }

    private InternalRow row(
            Scenario scenario,
            int rowId,
            int hotKeyCount,
            int valueLength,
            int keyPaddingLength,
            int valueRunLength,
            int valueCardinality) {
        GenericRow row = new GenericRow(2);
        row.setField(0, rowId);
        row.setField(
                1,
                map(
                        scenario,
                        rowId,
                        hotKeyCount,
                        valueLength,
                        keyPaddingLength,
                        valueRunLength,
                        valueCardinality));
        return row;
    }

    private GenericMap map(
            Scenario scenario,
            int rowId,
            int hotKeyCount,
            int valueLength,
            int keyPaddingLength,
            int valueRunLength,
            int valueCardinality) {
        Map<BinaryString, BinaryString> map = new LinkedHashMap<>();
        for (int keyId = 0; keyId < hotKeyCount; keyId++) {
            map.put(
                    hotKey(keyId, keyPaddingLength),
                    value(scenario, rowId, keyId, valueLength, valueRunLength, valueCardinality));
        }
        return new GenericMap(map);
    }

    private BinaryString hotKey(int keyId, int keyPaddingLength) {
        String prefix = String.format(Locale.ROOT, "very_common_header_key_%03d_", keyId);
        StringBuilder builder = new StringBuilder(prefix);
        while (builder.length() < prefix.length() + keyPaddingLength) {
            builder.append("0123456789abcdef");
        }
        int keyLength = prefix.length() + keyPaddingLength;
        return BinaryString.fromString(builder.substring(0, Math.min(builder.length(), keyLength)));
    }

    private BinaryString value(
            Scenario scenario,
            int rowId,
            int keyId,
            int valueLength,
            int valueRunLength,
            int valueCardinality) {
        int valueId =
                scenario == Scenario.COLUMNAR_VALUES
                        ? rowId / valueRunLength % valueCardinality
                        : rowId;
        String prefix = String.format(Locale.ROOT, "v_%03d_%03d_", keyId, valueId);
        StringBuilder builder = new StringBuilder(valueLength);
        while (builder.length() < valueLength) {
            builder.append(prefix);
        }
        return BinaryString.fromString(builder.substring(0, valueLength));
    }

    private enum Scenario {
        LONG_HOT_KEYS,
        COLUMNAR_VALUES
    }
}
