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

import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.ReadonlyTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.TableSnapshot;
import org.apache.paimon.table.source.InnerTableRead;
import org.apache.paimon.table.source.InnerTableScan;
import org.apache.paimon.table.source.ReadOnceTableScan;
import org.apache.paimon.table.source.SingletonSplit;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.TableRead;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.IteratorRecordReader;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.ProjectedRow;

import org.apache.paimon.shade.guava30.com.google.common.collect.Iterators;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalLong;

import static org.apache.paimon.CoreOptions.TYPE;
import static org.apache.paimon.rest.responses.AuditRESTResponse.FIELD_CREATED_AT;
import static org.apache.paimon.rest.responses.AuditRESTResponse.FIELD_CREATED_BY;
import static org.apache.paimon.rest.responses.AuditRESTResponse.FIELD_OWNER;
import static org.apache.paimon.rest.responses.AuditRESTResponse.FIELD_UPDATED_AT;
import static org.apache.paimon.rest.responses.AuditRESTResponse.FIELD_UPDATED_BY;

/** This is a system table to display all the database-tables. */
public class AllTablesTable implements ReadonlyTable {

    public static final String ALL_TABLES = "tables";

    public static final RowType TABLE_TYPE =
            new RowType(
                    Arrays.asList(
                            new DataField(0, "database_name", DataTypes.STRING()),
                            new DataField(1, "table_name", DataTypes.STRING()),
                            new DataField(2, "table_type", DataTypes.STRING()),
                            new DataField(3, "partitioned", DataTypes.BOOLEAN()),
                            new DataField(4, "primary_key", DataTypes.BOOLEAN()),
                            new DataField(5, "owner", DataTypes.STRING()),
                            new DataField(6, "created_at", DataTypes.BIGINT()),
                            new DataField(7, "created_by", DataTypes.STRING()),
                            new DataField(8, "updated_at", DataTypes.BIGINT()),
                            new DataField(9, "updated_by", DataTypes.STRING()),
                            new DataField(10, "record_count", DataTypes.BIGINT()),
                            new DataField(11, "file_size_in_bytes", DataTypes.BIGINT()),
                            new DataField(12, "file_count", DataTypes.BIGINT()),
                            new DataField(13, "last_file_creation_time", DataTypes.BIGINT())));

    private final List<GenericRow> rows;

    public AllTablesTable(List<GenericRow> rows) {
        this.rows = rows;
    }

    public static AllTablesTable fromTables(List<Pair<Table, TableSnapshot>> tables) {
        List<GenericRow> rows = new ArrayList<>();
        for (Pair<Table, TableSnapshot> pair : tables) {
            Table table = pair.getKey();
            TableSnapshot snapshot = pair.getValue();
            Identifier identifier = Identifier.fromString(table.fullName());
            Map<String, String> options = table.options();
            rows.add(
                    GenericRow.of(
                            BinaryString.fromString(identifier.getDatabaseName()),
                            BinaryString.fromString(identifier.getObjectName()),
                            BinaryString.fromString(
                                    options.getOrDefault(
                                            TYPE.key(), TYPE.defaultValue().toString())),
                            !table.partitionKeys().isEmpty(),
                            !table.primaryKeys().isEmpty(),
                            BinaryString.fromString(options.get(FIELD_OWNER)),
                            parseLong(options.get(FIELD_CREATED_AT)),
                            BinaryString.fromString(options.get(FIELD_CREATED_BY)),
                            parseLong(options.get(FIELD_UPDATED_AT)),
                            BinaryString.fromString(options.get(FIELD_UPDATED_BY)),
                            snapshot == null ? null : snapshot.recordCount(),
                            snapshot == null ? null : snapshot.fileSizeInBytes(),
                            snapshot == null ? null : snapshot.fileCount(),
                            snapshot == null ? null : snapshot.lastFileCreationTime()));
        }
        return new AllTablesTable(rows);
    }

    private static Long parseLong(String s) {
        if (s == null) {
            return null;
        }
        return Long.parseLong(s);
    }

    @Override
    public String name() {
        return ALL_TABLES;
    }

    @Override
    public RowType rowType() {
        return TABLE_TYPE;
    }

    @Override
    public List<String> primaryKeys() {
        return Arrays.asList(TABLE_TYPE.getField(0).name(), TABLE_TYPE.getField(1).name());
    }

    @Override
    public FileIO fileIO() {
        // pass a useless file io, should never use this.
        return new LocalFileIO();
    }

    @Override
    public InnerTableScan newScan() {
        return new AllTablesScan();
    }

    @Override
    public InnerTableRead newRead() {
        return new AllTablesRead();
    }

    @Override
    public Table copy(Map<String, String> dynamicOptions) {
        return new AllTablesTable(rows);
    }

    private class AllTablesScan extends ReadOnceTableScan {

        @Override
        public InnerTableScan withFilter(Predicate predicate) {
            return this;
        }

        @Override
        public Plan innerPlan() {
            return () -> Collections.singletonList(new AllTablesSplit(rows));
        }
    }

    private static class AllTablesSplit extends SingletonSplit {

        private static final long serialVersionUID = 1L;

        private final List<GenericRow> rows;

        private AllTablesSplit(List<GenericRow> rows) {
            this.rows = rows;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            AllTablesSplit that = (AllTablesSplit) o;
            return Objects.equals(rows, that.rows);
        }

        @Override
        public int hashCode() {
            return Objects.hash(rows);
        }

        @Override
        public OptionalLong mergedRowCount() {
            return OptionalLong.empty();
        }
    }

    private static class AllTablesRead implements InnerTableRead {

        private RowType readType;

        @Override
        public InnerTableRead withFilter(Predicate predicate) {
            return this;
        }

        @Override
        public InnerTableRead withReadType(RowType readType) {
            this.readType = readType;
            return this;
        }

        @Override
        public TableRead withIOManager(IOManager ioManager) {
            return this;
        }

        @Override
        public RecordReader<InternalRow> createReader(Split split) {
            if (!(split instanceof AllTablesSplit)) {
                throw new IllegalArgumentException("Unsupported split: " + split.getClass());
            }
            List<? extends InternalRow> rows = ((AllTablesSplit) split).rows;
            Iterator<? extends InternalRow> iterator = rows.iterator();
            if (readType != null) {
                iterator =
                        Iterators.transform(
                                iterator,
                                row -> ProjectedRow.from(readType, TABLE_TYPE).replaceRow(row));
            }
            //noinspection ReassignedVariable,unchecked
            return new IteratorRecordReader<>((Iterator<InternalRow>) iterator);
        }
    }
}
