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
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.ReadonlyTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.InnerTableRead;
import org.apache.paimon.table.source.InnerTableScan;
import org.apache.paimon.table.source.ReadOnceTableScan;
import org.apache.paimon.table.source.SingletonSplit;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.TableRead;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.VarCharType;
import org.apache.paimon.utils.IteratorRecordReader;
import org.apache.paimon.utils.ProjectedRow;

import org.apache.paimon.shade.guava30.com.google.common.collect.Iterators;

import java.util.ArrayList;
import java.util.Collections;
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

    private final Map<Identifier, Map<String, String>> allOptions;

    public AllTableOptionsTable(Map<Identifier, Map<String, String>> allOptions) {
        this.allOptions = allOptions;
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
        return new AllTableOptionsRead();
    }

    @Override
    public Table copy(Map<String, String> dynamicOptions) {
        return new AllTableOptionsTable(allOptions);
    }

    private class AllTableOptionsScan extends ReadOnceTableScan {

        @Override
        public InnerTableScan withFilter(Predicate predicate) {
            return this;
        }

        @Override
        public Plan innerPlan() {
            return () -> Collections.singletonList(new AllTableSplit(allOptions));
        }
    }

    private static class AllTableSplit extends SingletonSplit {

        private static final long serialVersionUID = 1L;

        private final Map<Identifier, Map<String, String>> allOptions;

        private AllTableSplit(Map<Identifier, Map<String, String>> allOptions) {
            this.allOptions = allOptions;
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
            return Objects.equals(allOptions, that.allOptions);
        }

        @Override
        public int hashCode() {
            return Objects.hash(allOptions);
        }
    }

    private static class AllTableOptionsRead implements InnerTableRead {

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
            if (!(split instanceof AllTableSplit)) {
                throw new IllegalArgumentException("Unsupported split: " + split.getClass());
            }
            List<InternalRow> rows = new ArrayList<>();
            for (Map.Entry<Identifier, Map<String, String>> entry :
                    ((AllTableSplit) split).allOptions.entrySet()) {
                String database = entry.getKey().getDatabaseName();
                String tableName = entry.getKey().getTableName();
                for (Map.Entry<String, String> entry2 : entry.getValue().entrySet()) {
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
            Iterator<InternalRow> iterator = rows.iterator();
            if (readType != null) {
                iterator =
                        Iterators.transform(
                                iterator,
                                row ->
                                        ProjectedRow.from(
                                                        readType, AggregationFieldsTable.TABLE_TYPE)
                                                .replaceRow(row));
            }
            return new IteratorRecordReader<>(iterator);
        }
    }
}
