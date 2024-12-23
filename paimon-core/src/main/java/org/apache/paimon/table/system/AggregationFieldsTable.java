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
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
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
import org.apache.paimon.utils.IteratorRecordReader;
import org.apache.paimon.utils.ProjectedRow;
import org.apache.paimon.utils.SerializationUtils;

import org.apache.paimon.shade.guava30.com.google.common.collect.ArrayListMultimap;
import org.apache.paimon.shade.guava30.com.google.common.collect.Iterators;
import org.apache.paimon.shade.guava30.com.google.common.collect.Multimap;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

import static org.apache.paimon.catalog.Catalog.SYSTEM_TABLE_SPLITTER;

/** A {@link Table} for showing Aggregation of table. */
public class AggregationFieldsTable implements ReadonlyTable {

    private static final long serialVersionUID = 1L;

    public static final String AGGREGATION_FIELDS = "aggregation_fields";

    public static final RowType TABLE_TYPE =
            new RowType(
                    Arrays.asList(
                            new DataField(0, "field_name", SerializationUtils.newStringType(false)),
                            new DataField(1, "field_type", SerializationUtils.newStringType(false)),
                            new DataField(2, "function", SerializationUtils.newStringType(true)),
                            new DataField(
                                    3, "function_options", SerializationUtils.newStringType(true)),
                            new DataField(4, "comment", SerializationUtils.newStringType(true))));

    private final FileIO fileIO;
    private final Path location;

    private final FileStoreTable dataTable;

    public AggregationFieldsTable(FileStoreTable dataTable) {
        this.fileIO = dataTable.fileIO();
        this.location = dataTable.tableDataPath();
        this.dataTable = dataTable;
    }

    @Override
    public String name() {
        return location.getName() + SYSTEM_TABLE_SPLITTER + AGGREGATION_FIELDS;
    }

    @Override
    public RowType rowType() {
        return TABLE_TYPE;
    }

    @Override
    public List<String> primaryKeys() {
        return Collections.singletonList("field_name");
    }

    @Override
    public InnerTableScan newScan() {
        return new SchemasScan();
    }

    @Override
    public InnerTableRead newRead() {
        return new SchemasRead(fileIO);
    }

    @Override
    public Table copy(Map<String, String> dynamicOptions) {
        return new AggregationFieldsTable(dataTable.copy(dynamicOptions));
    }

    private class SchemasScan extends ReadOnceTableScan {

        @Override
        public InnerTableScan withFilter(Predicate predicate) {
            return this;
        }

        @Override
        public Plan innerPlan() {
            return () -> Collections.singletonList(new AggregationSplit(location));
        }
    }

    /** {@link Split} implementation for {@link AggregationFieldsTable}. */
    private static class AggregationSplit extends SingletonSplit {

        private static final long serialVersionUID = 1L;

        private final Path location;

        private AggregationSplit(Path location) {
            this.location = location;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            AggregationSplit that = (AggregationSplit) o;
            return Objects.equals(location, that.location);
        }

        @Override
        public int hashCode() {
            return Objects.hash(location);
        }
    }

    /** {@link TableRead} implementation for {@link AggregationFieldsTable}. */
    private class SchemasRead implements InnerTableRead {

        private final FileIO fileIO;
        private RowType readType;

        public SchemasRead(FileIO fileIO) {
            this.fileIO = fileIO;
        }

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
            if (!(split instanceof AggregationSplit)) {
                throw new IllegalArgumentException("Unsupported split: " + split.getClass());
            }
            TableSchema schemas = dataTable.schemaManager().latest().get();
            Iterator<InternalRow> rows = createInternalRowIterator(schemas);
            if (readType != null) {
                rows =
                        Iterators.transform(
                                rows,
                                row ->
                                        ProjectedRow.from(
                                                        readType, AggregationFieldsTable.TABLE_TYPE)
                                                .replaceRow(row));
            }
            return new IteratorRecordReader<>(rows);
        }

        private Iterator<InternalRow> createInternalRowIterator(TableSchema schema) {
            Multimap<String, String> function =
                    extractFieldMultimap(schema.options(), Map.Entry::getValue);
            Multimap<String, String> functionOptions =
                    extractFieldMultimap(schema.options(), Map.Entry::getKey);
            List<InternalRow> internalRows = new ArrayList<>();

            GenericRow genericRow;
            for (int i = 0; i < schema.fields().size(); i++) {
                String fieldName = schema.fields().get(i).name();
                genericRow =
                        GenericRow.of(
                                BinaryString.fromString(fieldName),
                                BinaryString.fromString(schema.fields().get(i).type().toString()),
                                BinaryString.fromString(function.get(fieldName).toString()),
                                BinaryString.fromString(functionOptions.get(fieldName).toString()),
                                BinaryString.fromString(schema.fields().get(i).description()));
                internalRows.add(genericRow);
            }
            return internalRows.iterator();
        }
    }

    protected static Multimap<String, String> extractFieldMultimap(
            Map<String, String> inputMap,
            Function<Map.Entry<String, String>, String> contentSelector) {
        Multimap<String, String> fieldMultimap = ArrayListMultimap.create();

        for (Map.Entry<String, String> entry : inputMap.entrySet()) {
            String[] keys = entry.getKey().split("\\.");
            if (keys.length > 2 && keys[0].equals("fields")) {
                String fieldKey = keys[1];
                String content = contentSelector.apply(entry);
                fieldMultimap.put(fieldKey, content);
            }
        }
        return fieldMultimap;
    }
}
