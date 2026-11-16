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

package org.apache.paimon.gpu;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.KeyValue;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypeRoot;
import org.apache.paimon.types.RowType;

import uk.ac.manchester.tornado.cudf.enums.CudfType;
import uk.ac.manchester.tornado.cudf.enums.ParquetColumnType;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/**
 * Where each column of a primary-key data file lives on the device, for the read, the merge and the
 * write.
 *
 * <p>A file holds the key columns, {@code _SEQUENCE_NUMBER}, {@code _VALUE_KIND}, then the value
 * columns. Every key column and the sequence number are read as INT64 into long slots {@code
 * 0..keys}, so the sort runs over them in place. {@code _VALUE_KIND} is int slot 0.
 */
final class KvLayout {

    static final int INTS = 0;
    static final int LONGS = 1;
    static final int DOUBLES = 2;

    /** Values per output column in {@link #gatherSpec}. */
    static final int SPEC_WIDTH = 6;

    /**
     * How a TINYINT is written: INT8 where the device writer has it, else plain INT32, which Paimon
     * reads but whose schema differs from the one Paimon writes.
     */
    static final ParquetColumnType TINYINT_WRITE = tinyintWrite();

    private static final Set<DataTypeRoot> KEY_TYPES =
            EnumSet.of(DataTypeRoot.INTEGER, DataTypeRoot.BIGINT);

    private static final Set<DataTypeRoot> VALUE_TYPES =
            EnumSet.of(
                    DataTypeRoot.INTEGER,
                    DataTypeRoot.BIGINT,
                    DataTypeRoot.DOUBLE,
                    DataTypeRoot.DATE,
                    DataTypeRoot.VARCHAR,
                    DataTypeRoot.CHAR);

    final RowType keyType;
    final RowType valueType;
    final RowType writeRowType;
    final int keyCount;

    final int[] readColumns;
    final CudfType[] readTypes;
    final int[] stringColumns;
    final int intSlots;
    final int longSlots;
    final int doubleSlots;

    final String names;
    final int[] fieldIds;
    final ParquetColumnType[] writeTypes;
    final boolean[] optional;
    final int[] details;
    final int outIntSlots;
    final int outLongSlots;
    final int outDoubleSlots;

    /**
     * Per numeric output column: source buffer, source slot, target buffer, target slot, read
     * validity column (-1 for a required column), written column index.
     */
    final int[] gatherSpec;

    final int numericOutputs;

    private KvLayout(RowType keyType, RowType valueType) {
        this.keyType = keyType;
        this.valueType = valueType;
        this.writeRowType = KeyValue.schema(keyType, valueType);
        this.keyCount = keyType.getFieldCount();

        List<DataField> fields = writeRowType.getFields();
        List<Integer> reads = new ArrayList<>();
        List<CudfType> readTypeList = new ArrayList<>();
        List<Integer> strings = new ArrayList<>();
        // Per file column: where the read put it (buffer, slot, read index); null for a string.
        int[][] placed = new int[fields.size()][];
        int ints = 0;
        int longs = 0;
        int doubles = 0;
        for (int c = 0; c <= keyCount; c++) {
            placed[c] = new int[] {LONGS, longs++, reads.size()};
            reads.add(c);
            readTypeList.add(CudfType.INT64);
        }
        placed[keyCount + 1] = new int[] {INTS, ints++, reads.size()};
        reads.add(keyCount + 1);
        readTypeList.add(CudfType.INT32);
        for (int c = keyCount + 2; c < fields.size(); c++) {
            switch (fields.get(c).type().getTypeRoot()) {
                case INTEGER:
                case DATE:
                    placed[c] = new int[] {INTS, ints++, reads.size()};
                    reads.add(c);
                    readTypeList.add(CudfType.INT32);
                    break;
                case BIGINT:
                    placed[c] = new int[] {LONGS, longs++, reads.size()};
                    reads.add(c);
                    readTypeList.add(CudfType.INT64);
                    break;
                case DOUBLE:
                    placed[c] = new int[] {DOUBLES, doubles++, reads.size()};
                    reads.add(c);
                    readTypeList.add(CudfType.FLOAT64);
                    break;
                case VARCHAR:
                case CHAR:
                    strings.add(c);
                    break;
                default:
                    throw new IllegalArgumentException("Unsupported column " + fields.get(c));
            }
        }
        this.readColumns = toArray(reads);
        this.readTypes = readTypeList.toArray(new CudfType[0]);
        this.stringColumns = toArray(strings);
        this.intSlots = ints;
        this.longSlots = longs;
        this.doubleSlots = doubles;

        int numericReads = readColumns.length;
        this.fieldIds = new int[fields.size()];
        this.writeTypes = new ParquetColumnType[fields.size()];
        this.optional = new boolean[fields.size()];
        this.details = new int[fields.size()];
        StringBuilder joined = new StringBuilder();
        List<int[]> spec = new ArrayList<>();
        int outInts = 0;
        int outLongs = 0;
        int outDoubles = 0;
        int stringIndex = 0;
        for (int c = 0; c < fields.size(); c++) {
            DataField field = fields.get(c);
            fieldIds[c] = field.id();
            optional[c] = field.type().isNullable();
            if (c > 0) {
                joined.append('\n');
            }
            joined.append(field.name());
            if (placed[c] == null) {
                writeTypes[c] = ParquetColumnType.STRING;
                // Its validity as the string read wrote it, after the numeric columns'.
                details[c] = numericReads + stringIndex++;
                continue;
            }
            int target;
            int targetSlot;
            switch (field.type().getTypeRoot()) {
                case INTEGER:
                    writeTypes[c] = ParquetColumnType.INT32;
                    target = INTS;
                    targetSlot = outInts++;
                    break;
                case TINYINT:
                    writeTypes[c] = TINYINT_WRITE;
                    target = INTS;
                    targetSlot = outInts++;
                    break;
                case DATE:
                    writeTypes[c] = ParquetColumnType.DATE;
                    target = INTS;
                    targetSlot = outInts++;
                    break;
                case BIGINT:
                    writeTypes[c] = ParquetColumnType.INT64;
                    target = LONGS;
                    targetSlot = outLongs++;
                    break;
                case DOUBLE:
                    writeTypes[c] = ParquetColumnType.FLOAT64;
                    target = DOUBLES;
                    targetSlot = outDoubles++;
                    break;
                default:
                    throw new IllegalArgumentException("Unsupported column " + field);
            }
            spec.add(
                    new int[] {
                        placed[c][0],
                        placed[c][1],
                        target,
                        targetSlot,
                        optional[c] ? placed[c][2] : -1,
                        c
                    });
        }
        this.names = joined.toString();
        this.outIntSlots = outInts;
        this.outLongSlots = outLongs;
        this.outDoubleSlots = outDoubles;
        this.numericOutputs = spec.size();
        this.gatherSpec = new int[spec.size() * SPEC_WIDTH];
        for (int o = 0; o < spec.size(); o++) {
            System.arraycopy(spec.get(o), 0, gatherSpec, o * SPEC_WIDTH, SPEC_WIDTH);
        }
    }

    int validColumns() {
        return readColumns.length + stringColumns.length;
    }

    /**
     * The layout for a table whose compactions the device can run exactly as Paimon's merge-tree
     * rewriter does, or null with the reason appended to {@code why}.
     */
    @Nullable
    static KvLayout of(RowType keyType, RowType valueType, CoreOptions options, StringBuilder why) {
        String reason = unsupported(keyType, valueType, options);
        if (reason != null) {
            why.append(reason);
            return null;
        }
        return new KvLayout(keyType, valueType);
    }

    @Nullable
    private static String unsupported(RowType keyType, RowType valueType, CoreOptions options) {
        if (options.mergeEngine() != CoreOptions.MergeEngine.DEDUPLICATE) {
            return "merge-engine " + options.mergeEngine();
        }
        if (!options.sequenceField().isEmpty()) {
            return "sequence.field";
        }
        if (options.ignoreDelete()) {
            return "ignore-delete";
        }
        if (options.recordLevelExpireTime() != null) {
            return "record-level.expire-time";
        }
        if (options.dataFileThinMode()) {
            return "data-file.thin-mode";
        }
        if (!"parquet".equals(options.fileFormatString().toLowerCase(Locale.ROOT))
                || !options.fileFormatPerLevel().isEmpty()) {
            return "a file format other than parquet";
        }
        if (!options.fileCompressionPerLevel().isEmpty() || codec(options.fileCompression()) < 0) {
            return "file.compression " + options.fileCompression();
        }
        if (!options.statsModePerLevel().isEmpty()) {
            return "metadata.stats-mode.per.level";
        }
        if (options.dataFileExternalPaths() != null) {
            return "data-file.external-paths";
        }
        for (String key : options.toConfiguration().keySet()) {
            if (key.startsWith("file-index.")) {
                return "file-index";
            }
        }
        for (DataField field : keyType.getFields()) {
            if (!KEY_TYPES.contains(field.type().getTypeRoot())) {
                return "key column " + field;
            }
        }
        for (DataField field : valueType.getFields()) {
            if (!VALUE_TYPES.contains(field.type().getTypeRoot())) {
                return "value column " + field;
            }
        }
        return null;
    }

    /** The device writer's codec for {@code file.compression}, or -1 when it has none. */
    static int codec(String compression) {
        switch (compression.toLowerCase(Locale.ROOT)) {
            case "zstd":
                return 2;
            case "snappy":
                return 1;
            case "none":
            case "uncompressed":
                return 0;
            default:
                return -1;
        }
    }

    private static ParquetColumnType tinyintWrite() {
        return Arrays.stream(ParquetColumnType.values())
                .filter(t -> t.name().equals("INT8"))
                .findFirst()
                .orElse(ParquetColumnType.INT32);
    }

    private static int[] toArray(List<Integer> values) {
        int[] array = new int[values.size()];
        for (int i = 0; i < array.length; i++) {
            array[i] = values.get(i);
        }
        return array;
    }
}
