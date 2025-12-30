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
 * <p>INSERT: [+I, [co1], [col2]]
 *
 * <p>UPDATE: [+U, [co1_ub, col1_ua], [col2_ub, col2_ua]]
 *
 * <p>DELETE: [-D, [co1], [col2]]
 */
public class BinlogTable extends AuditLogTable {

    public static final String BINLOG = "binlog";

    public BinlogTable(FileStoreTable wrapped) {
        super(wrapped);
    }

    @Override
    public String name() {
        return wrapped.name() + SYSTEM_TABLE_SPLITTER + BINLOG;
    }

    @Override
    public RowType rowType() {
        List<DataField> fields = new ArrayList<>();
        fields.add(SpecialFields.ROW_KIND);
        if (specialFieldCount > 1) {
            fields.add(SpecialFields.SEQUENCE_NUMBER);
        }
        for (DataField field : wrapped.rowType().getFields()) {
            // convert to nullable
            fields.add(field.newType(new ArrayType(field.type().nullable())));
        }
        return new RowType(fields);
    }

    @Override
    public InnerTableRead newRead() {
        return new BinlogRead(wrapped.newRead());
    }

    @Override
    public Table copy(Map<String, String> dynamicOptions) {
        return new BinlogTable(wrapped.copy(dynamicOptions));
    }

    private class BinlogRead extends AuditLogRead {

        private RowType wrappedReadType = wrapped.rowType();

        private BinlogRead(InnerTableRead dataRead) {
            super(dataRead);
        }

        @Override
        public InnerTableRead withReadType(RowType readType) {
            List<DataField> fields = new ArrayList<>();
            List<DataField> wrappedReadFields = new ArrayList<>();
            for (DataField field : readType.getFields()) {
                if (SpecialFields.isSystemField(field.name())) {
                    fields.add(field);
                } else {
                    DataField origin = field.newType(((ArrayType) field.type()).getElementType());
                    fields.add(origin);
                    wrappedReadFields.add(origin);
                }
            }
            this.wrappedReadType = this.wrappedReadType.copy(wrappedReadFields);
            return super.withReadType(readType.copy(fields));
        }

        @Override
        public RecordReader<InternalRow> createReader(Split split) throws IOException {
            DataSplit dataSplit = (DataSplit) split;
            // When sequence number is enabled, the underlying data layout is:
            // [_SEQUENCE_NUMBER, pk, pt, col1, ...]
            // We need to offset the field index to skip the sequence number field.
            int offset = specialFieldCount - 1;
            InternalRow.FieldGetter[] fieldGetters =
                    IntStream.range(0, wrappedReadType.getFieldCount())
                            .mapToObj(
                                    i ->
                                            InternalRow.createFieldGetter(
                                                    wrappedReadType.getTypeAt(i), i + offset))
                            .toArray(InternalRow.FieldGetter[]::new);

            if (dataSplit.isStreaming()) {
                return new PackChangelogReader(
                        dataRead.createReader(split),
                        (row1, row2) ->
                                new AuditLogRow(
                                        readProjection, convertToArray(row1, row2, fieldGetters)),
                        this.wrappedReadType);
            } else {
                return dataRead.createReader(split)
                        .transform(
                                (row) ->
                                        new AuditLogRow(
                                                readProjection,
                                                convertToArray(row, null, fieldGetters)));
            }
        }

        private InternalRow convertToArray(
                InternalRow row1,
                @Nullable InternalRow row2,
                InternalRow.FieldGetter[] fieldGetters) {
            // seqOffset is 1 if sequence number is enabled, 0 otherwise
            int seqOffset = specialFieldCount - 1;
            GenericRow row = new GenericRow(fieldGetters.length + seqOffset);

            // Copy sequence number if enabled (it's at index 0 in input row)
            if (seqOffset > 0) {
                row.setField(0, row1.getLong(0));
            }

            for (int i = 0; i < fieldGetters.length; i++) {
                Object o1 = fieldGetters[i].getFieldOrNull(row1);
                Object o2;
                if (row2 != null) {
                    o2 = fieldGetters[i].getFieldOrNull(row2);
                    row.setField(i + seqOffset, new GenericArray(new Object[] {o1, o2}));
                } else {
                    row.setField(i + seqOffset, new GenericArray(new Object[] {o1}));
                }
            }
            // If no row2 provided, then follow the row1 kind.
            if (row2 == null) {
                row.setRowKind(row1.getRowKind());
            } else {
                row.setRowKind(row2.getRowKind());
            }
            return row;
        }
    }
}
