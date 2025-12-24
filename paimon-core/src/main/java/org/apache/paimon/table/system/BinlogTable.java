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

package org.apache.paimon.table.system;

import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.reader.PackChangelogReader;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.SpecialFields;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.InnerTableRead;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import static org.apache.paimon.catalog.Identifier.SYSTEM_TABLE_SPLITTER;

/**
 * A {@link Table} for reading binlog of table. The binlog format is as below.
 *
 * <p>INSERT: [+I, seq_num, [col1], [col2]]
 *
 * <p>UPDATE: [+U, seq_num, [col1_ub, col1_ua], [col2_ub, col2_ua]]
 *
 * <p>DELETE: [-D, seq_num, [col1], [col2]]
 */
public class BinlogTable extends AuditLogTable {

    public static final String BINLOG = "binlog";

    private final FileStoreTable wrapped;

    public BinlogTable(FileStoreTable wrapped) {
        super(wrapped);
        this.wrapped = wrapped;
    }

    @Override
    public String name() {
        return wrapped.name() + SYSTEM_TABLE_SPLITTER + BINLOG;
    }

    @Override
    public RowType rowType() {
        List<DataField> fields = new ArrayList<>();
        fields.add(SpecialFields.ROW_KIND);
        fields.add(
                SpecialFields.SEQUENCE_NUMBER.newType(
                        SpecialFields.SEQUENCE_NUMBER.type().nullable()));
        for (DataField field : wrapped.rowType().getFields()) {
            // convert to nullable
            fields.add(field.newType(new ArrayType(field.type().nullable())));
        }
        return new RowType(fields);
    }

    @Override
    public InnerTableRead newRead() {
        return new BinlogRead(wrapped.newRead()).withReadType(rowType());
    }

    @Override
    public Table copy(Map<String, String> dynamicOptions) {
        return new BinlogTable(wrapped.copy(dynamicOptions));
    }

    private class BinlogRead extends AuditLogRead {

        private int[] outputMapping; // naturalIndex -> outputIndex
        private int systemFieldCount;
        private int physicalFieldCount;
        private int rowkindOutputIndex = -1;
        private InternalRow.FieldGetter[] fieldGetters; // Pre-created field getters
        private RowType underlyingType;

        private BinlogRead(InnerTableRead dataRead) {
            super(dataRead);
        }

        @Override
        public InnerTableRead withReadType(RowType readType) {
            List<Integer> systemIndices = new ArrayList<>();
            List<Integer> physicalIndices = new ArrayList<>();
            List<DataField> underlyingFields = new ArrayList<>();

            for (int i = 0; i < readType.getFieldCount(); i++) {
                DataField field = readType.getFields().get(i);
                if (SpecialFields.isSystemField(field.name())) {
                    systemIndices.add(i);
                    underlyingFields.add(field);
                    if (field.name().equals(SpecialFields.ROW_KIND.name())) {
                        rowkindOutputIndex = i;
                    }
                } else {
                    physicalIndices.add(i);
                    // Convert Array type back to element type for underlying read
                    DataField origin = field.newType(((ArrayType) field.type()).getElementType());
                    underlyingFields.add(origin);
                }
            }

            systemFieldCount = systemIndices.size();
            physicalFieldCount = physicalIndices.size();

            // Build mapping: outputMapping[naturalIndex] = outputIndex
            outputMapping = new int[systemIndices.size() + physicalIndices.size()];
            for (int i = 0; i < systemIndices.size(); i++) {
                outputMapping[i] = systemIndices.get(i);
            }
            for (int i = 0; i < physicalIndices.size(); i++) {
                outputMapping[systemIndices.size() + i] = physicalIndices.get(i);
            }

            // Pre-create field getters for natural order [system fields... + physical fields...]
            this.underlyingType = new RowType(underlyingFields);
            this.fieldGetters =
                    IntStream.range(0, underlyingType.getFieldCount())
                            .mapToObj(
                                    i ->
                                            InternalRow.createFieldGetter(
                                                    underlyingType.getTypeAt(i), i))
                            .toArray(InternalRow.FieldGetter[]::new);

            // Pass natural order to underlying read
            return super.withReadType(underlyingType);
        }

        @Override
        public RecordReader<InternalRow> createReader(Split split) throws IOException {
            DataSplit dataSplit = (DataSplit) split;

            if (dataSplit.isStreaming()) {
                return new PackChangelogReader(
                        dataRead.createReader(split), this::convertToArray, this.underlyingType);
            } else {
                return dataRead.createReader(split).transform(row -> convertToArray(row, null));
            }
        }

        /**
         * Convert changelog rows to binlog array format with field reordering.
         *
         * @param row1 the before row or the after row (if row2 is null)
         * @param row2 the after row (null for insert/delete)
         * @return the converted row in user-requested field order
         */
        private InternalRow convertToArray(InternalRow row1, @Nullable InternalRow row2) {
            // Bottom layer row is already in natural order: [system fields... + physical fields...]
            InternalRow systemSource = row2 != null ? row2 : row1;

            GenericRow output = new GenericRow(outputMapping.length);
            int naturalIndex = 0;

            // 1. Copy system fields by mapping (use pre-created FieldGetters)
            for (int i = 0; i < systemFieldCount; i++) {
                int outputPos = outputMapping[naturalIndex++];
                output.setField(outputPos, fieldGetters[i].getFieldOrNull(systemSource));
            }

            // 2. Pack physical fields into Array by mapping
            for (int i = 0; i < physicalFieldCount; i++) {
                int outputPos = outputMapping[naturalIndex++];
                int naturalPos = systemFieldCount + i;

                Object v1 = fieldGetters[naturalPos].getFieldOrNull(row1);
                if (row2 != null) {
                    Object v2 = fieldGetters[naturalPos].getFieldOrNull(row2);
                    output.setField(outputPos, new GenericArray(new Object[] {v1, v2}));
                } else {
                    output.setField(outputPos, new GenericArray(new Object[] {v1}));
                }
            }

            output.setRowKind(org.apache.paimon.types.RowKind.INSERT);

            // 3. Fallback for append-only tables: rowkind field is null, wrap to return "+I"
            if (rowkindOutputIndex >= 0 && output.isNullAt(rowkindOutputIndex)) {
                return new AuditLogTable.AuditLogRow(output, rowkindOutputIndex);
            }

            return output;
        }
    }
}
