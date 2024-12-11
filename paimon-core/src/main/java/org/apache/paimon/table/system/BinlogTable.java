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

import static org.apache.paimon.catalog.Catalog.SYSTEM_TABLE_SPLITTER;

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
        for (DataField field : wrapped.rowType().getFields()) {
            DataField newField =
                    new DataField(
                            field.id(),
                            field.name(),
                            new ArrayType(field.type().nullable()), // convert to nullable
                            field.description());
            fields.add(newField);
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

        private BinlogRead(InnerTableRead dataRead) {
            super(dataRead);
        }

        @Override
        public InnerTableRead withReadType(RowType readType) {
            List<DataField> fields = new ArrayList<>();
            for (DataField field : readType.getFields()) {
                if (field.name().equals(SpecialFields.ROW_KIND.name())) {
                    fields.add(field);
                } else {
                    fields.add(
                            new DataField(
                                    field.id(),
                                    field.name(),
                                    ((ArrayType) field.type()).getElementType()));
                }
            }
            return super.withReadType(readType.copy(fields));
        }

        @Override
        public RecordReader<InternalRow> createReader(Split split) throws IOException {
            DataSplit dataSplit = (DataSplit) split;
            if (dataSplit.isStreaming()) {
                return new PackChangelogReader(
                        dataRead.createReader(split),
                        (row1, row2) ->
                                new AuditLogRow(
                                        readProjection,
                                        convertToArray(
                                                row1, row2, wrapped.rowType().fieldGetters())),
                        wrapped.rowType());
            } else {
                return dataRead.createReader(split)
                        .transform(
                                (row) ->
                                        new AuditLogRow(
                                                readProjection,
                                                convertToArray(
                                                        row,
                                                        null,
                                                        wrapped.rowType().fieldGetters())));
            }
        }

        private InternalRow convertToArray(
                InternalRow row1,
                @Nullable InternalRow row2,
                InternalRow.FieldGetter[] fieldGetters) {
            GenericRow row = new GenericRow(row1.getFieldCount());
            for (int i = 0; i < row1.getFieldCount(); i++) {
                Object o1 = fieldGetters[i].getFieldOrNull(row1);
                Object o2;
                if (row2 != null) {
                    o2 = fieldGetters[i].getFieldOrNull(row2);
                    row.setField(i, new GenericArray(new Object[] {o1, o2}));
                } else {
                    row.setField(i, new GenericArray(new Object[] {o1}));
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
