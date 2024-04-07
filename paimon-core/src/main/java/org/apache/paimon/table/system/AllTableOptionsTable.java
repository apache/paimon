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

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.table.ReadonlyTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.InnerTableRead;
import org.apache.paimon.table.source.InnerTableScan;
import org.apache.paimon.table.source.ReadOnceTableScan;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.TableRead;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.VarCharType;
import org.apache.paimon.utils.IteratorRecordReader;
import org.apache.paimon.utils.ProjectedRow;

import org.apache.paimon.shade.guava30.com.google.common.collect.Iterators;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * This is a system table to display all the database-table properties.
 *
 * <pre>
 *  For example:
 *     If we select * from sys.all_table_options, we will get
 *     databasename       tablename       key      value
 *         default           test0         a         b
 *         my_db             test1         c         d
 *          ...               ...         ...       ...
 *     We can write sql to fetch the information we need.
 * </pre>
 */
public class AllTableOptionsTable implements ReadonlyTable {

    public static final String ALL_TABLE_OPTIONS = "all_table_options";

    private final FileIO fileIO;
    private final Map<String, Map<String, Path>> allTablePaths;

    public AllTableOptionsTable(FileIO fileIO, Map<String, Map<String, Path>> allTablePaths) {
        // allTablePath is the map of  <database, <table_name, properties>>
        this.fileIO = fileIO;
        this.allTablePaths = allTablePaths;
    }

    @Override
    public String name() {
        return ALL_TABLE_OPTIONS;
    }

    @Override
    public RowType rowType() {
        List<DataField> fields = new ArrayList<>();
        fields.add(new DataField(0, "database_name", new VarCharType(VarCharType.MAX_LENGTH)));
        fields.add(new DataField(1, "table_name", new VarCharType(VarCharType.MAX_LENGTH)));
        fields.add(new DataField(2, "key", new VarCharType(VarCharType.MAX_LENGTH)));
        fields.add(new DataField(3, "value", new VarCharType(VarCharType.MAX_LENGTH)));
        return new RowType(fields);
    }

    @Override
    public List<String> primaryKeys() {
        return Collections.singletonList("table_name");
    }

    @Override
    public InnerTableScan newScan() {
        return new AllTableOptionsScan();
    }

    @Override
    public InnerTableRead newRead() {
        return new AllTableOptionsRead(fileIO);
    }

    @Override
    public Table copy(Map<String, String> dynamicOptions) {
        return new AllTableOptionsTable(fileIO, allTablePaths);
    }

    private class AllTableOptionsScan extends ReadOnceTableScan {

        @Override
        public InnerTableScan withFilter(Predicate predicate) {
            return this;
        }

        @Override
        public Plan innerPlan() {
            return () ->
                    Collections.singletonList(
                            new AllTableSplit(
                                    options(fileIO, allTablePaths).values().stream()
                                            .flatMap(t -> t.values().stream())
                                            .reduce(0, (a, b) -> a + b.size(), Integer::sum),
                                    allTablePaths));
        }
    }

    private static class AllTableSplit implements Split {

        private static final long serialVersionUID = 1L;

        private final long rowCount;
        private final Map<String, Map<String, Path>> allTablePaths;

        private AllTableSplit(long rowCount, Map<String, Map<String, Path>> allTablePaths) {
            this.rowCount = rowCount;
            this.allTablePaths = allTablePaths;
        }

        @Override
        public long rowCount() {
            return rowCount;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            AllTableSplit that = (AllTableSplit) o;
            return Objects.equals(allTablePaths, that.allTablePaths);
        }

        @Override
        public int hashCode() {
            return Objects.hash(allTablePaths);
        }
    }

    private static class AllTableOptionsRead implements InnerTableRead {

        private final FileIO fileIO;
        private int[][] projection;

        public AllTableOptionsRead(FileIO fileIO) {
            this.fileIO = fileIO;
        }

        @Override
        public InnerTableRead withFilter(Predicate predicate) {
            return this;
        }

        @Override
        public InnerTableRead withProjection(int[][] projection) {
            this.projection = projection;
            return this;
        }

        @Override
        public TableRead withIOManager(IOManager ioManager) {
            return this;
        }

        @Override
        public RecordReader<InternalRow> createReader(Split split) throws IOException {
            if (!(split instanceof AllTableSplit)) {
                throw new IllegalArgumentException("Unsupported split: " + split.getClass());
            }
            Map<String, Map<String, Path>> location = ((AllTableSplit) split).allTablePaths;
            Iterator<InternalRow> rows = toRow(options(fileIO, location));
            if (projection != null) {
                rows =
                        Iterators.transform(
                                rows, row -> ProjectedRow.from(projection).replaceRow(row));
            }
            return new IteratorRecordReader<>(rows);
        }
    }

    protected static Iterator<InternalRow> toRow(
            Map<String, Map<String, Map<String, String>>> option) {
        List<InternalRow> rows = new ArrayList<>();
        for (Map.Entry<String, Map<String, Map<String, String>>> entry0 : option.entrySet()) {
            String database = entry0.getKey();
            for (Map.Entry<String, Map<String, String>> entry1 : entry0.getValue().entrySet()) {
                String tableName = entry1.getKey();
                for (Map.Entry<String, String> entry2 : entry1.getValue().entrySet()) {
                    String key = entry2.getKey();
                    String value = entry2.getValue();
                    rows.add(
                            GenericRow.of(
                                    BinaryString.fromString(database),
                                    BinaryString.fromString(tableName),
                                    BinaryString.fromString(key),
                                    BinaryString.fromString(value)));
                }
            }
        }
        return rows.iterator();
    }

    protected static Map<String, Map<String, Map<String, String>>> options(
            FileIO fileIO, Map<String, Map<String, Path>> allTablePaths) {
        Map<String, Map<String, Map<String, String>>> allOptions = new HashMap<>();
        for (Map.Entry<String, Map<String, Path>> entry0 : allTablePaths.entrySet()) {
            Map<String, Map<String, String>> m0 =
                    allOptions.computeIfAbsent(entry0.getKey(), k -> new HashMap<>());
            for (Map.Entry<String, Path> entry1 : entry0.getValue().entrySet()) {
                Map<String, String> options =
                        new SchemaManager(fileIO, entry1.getValue())
                                .latest()
                                .orElseThrow(() -> new RuntimeException("Table not exists."))
                                .options();
                m0.put(entry1.getKey(), options);
            }
        }
        return allOptions;
    }
}
