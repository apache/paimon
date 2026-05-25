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

package org.apache.paimon.cli.sql;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.TableRead;
import org.apache.paimon.table.source.TableScan;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.RowType;

import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.ProjectableFilterableTable;
import org.apache.calcite.schema.impl.AbstractTable;

import java.util.List;

/** Bridges a Paimon table to Calcite's query engine via ProjectableFilterableTable. */
public class PaimonCalciteTable extends AbstractTable implements ProjectableFilterableTable {

    private final Table paimonTable;

    public PaimonCalciteTable(Table paimonTable) {
        this.paimonTable = paimonTable;
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        return PaimonTypeMapping.toCalciteRowType(typeFactory, paimonTable.rowType());
    }

    @Override
    public Enumerable<Object[]> scan(DataContext root, List<RexNode> filters, int[] projects) {
        ReadBuilder builder = paimonTable.newReadBuilder();

        if (projects != null && projects.length > 0) {
            builder.withProjection(projects);
        }

        Predicate predicate = RexToPredicate.convert(filters, paimonTable.rowType());
        if (predicate != null) {
            builder.withFilter(predicate);
        }

        RowType fullRowType = paimonTable.rowType();
        List<DataField> readFields;
        if (projects != null && projects.length > 0) {
            List<DataField> allFields = fullRowType.getFields();
            readFields = new java.util.ArrayList<>(projects.length);
            for (int idx : projects) {
                readFields.add(allFields.get(idx));
            }
        } else {
            readFields = fullRowType.getFields();
        }

        return new AbstractEnumerable<Object[]>() {
            @Override
            public Enumerator<Object[]> enumerator() {
                return new PaimonEnumerator(builder, readFields);
            }
        };
    }

    private static class PaimonEnumerator implements Enumerator<Object[]> {

        private final ReadBuilder builder;
        private final List<DataField> fields;
        private final DataType[] fieldTypes;

        private List<Split> splits;
        private int splitIndex;
        private TableRead tableRead;
        private RecordReader<InternalRow> currentReader;
        private RecordReader.RecordIterator<InternalRow> currentBatch;
        private Object[] current;

        PaimonEnumerator(ReadBuilder builder, List<DataField> fields) {
            this.builder = builder;
            this.fields = fields;
            this.fieldTypes = new DataType[fields.size()];
            for (int i = 0; i < fields.size(); i++) {
                this.fieldTypes[i] = fields.get(i).type();
            }
            this.splitIndex = 0;
        }

        @Override
        public Object[] current() {
            return current;
        }

        @Override
        public boolean moveNext() {
            try {
                if (splits == null) {
                    TableScan scan = builder.newScan();
                    splits = scan.plan().splits();
                    tableRead = builder.newRead();
                }

                while (true) {
                    if (currentBatch != null) {
                        InternalRow row = currentBatch.next();
                        if (row != null) {
                            current = convertRow(row);
                            return true;
                        }
                        currentBatch.releaseBatch();
                        currentBatch = null;
                    }

                    if (currentReader != null) {
                        currentBatch = currentReader.readBatch();
                        if (currentBatch != null) {
                            continue;
                        }
                        currentReader.close();
                        currentReader = null;
                    }

                    if (splitIndex >= splits.size()) {
                        return false;
                    }

                    currentReader = tableRead.createReader(splits.get(splitIndex++));
                }
            } catch (Exception e) {
                throw new RuntimeException("Error reading Paimon table", e);
            }
        }

        @Override
        public void reset() {
            close();
            splits = null;
            splitIndex = 0;
            currentReader = null;
            currentBatch = null;
            current = null;
        }

        @Override
        public void close() {
            try {
                if (currentBatch != null) {
                    currentBatch.releaseBatch();
                    currentBatch = null;
                }
                if (currentReader != null) {
                    currentReader.close();
                    currentReader = null;
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        private Object[] convertRow(InternalRow row) {
            Object[] values = new Object[fieldTypes.length];
            for (int i = 0; i < fieldTypes.length; i++) {
                values[i] = PaimonTypeMapping.getValue(row, i, fieldTypes[i]);
            }
            return values;
        }
    }
}
