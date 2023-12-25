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
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.ReadonlyTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.InnerTableRead;
import org.apache.paimon.table.source.InnerTableScan;
import org.apache.paimon.table.source.ReadOnceTableScan;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.TableRead;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.IteratorRecordReader;
import org.apache.paimon.utils.ProjectedRow;
import org.apache.paimon.utils.SerializationUtils;

import org.apache.paimon.shade.guava30.com.google.common.collect.Iterators;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.apache.paimon.catalog.Catalog.SYSTEM_TABLE_SPLITTER;

/** A {@link Table} for showing Aggregation of table. */
public class AggregationTable implements ReadonlyTable {

    private static final long serialVersionUID = 1L;

    public static final String AGGREGATION = "aggregation";

    public static final RowType TABLE_TYPE =
            new RowType(
                    Arrays.asList(
                            new DataField(0, "field_name", SerializationUtils.newStringType(false)),
                            new DataField(1, "type", SerializationUtils.newStringType(false)),
                            new DataField(
                                    2,
                                    "AggregationFunction",
                                    SerializationUtils.newStringType(true)),
                            new DataField(3, "comment", SerializationUtils.newStringType(true))));

    private final FileIO fileIO;
    private final Path location;

    public AggregationTable(FileIO fileIO, Path location) {
        this.fileIO = fileIO;
        this.location = location;
    }

    @Override
    public String name() {
        return location.getName() + SYSTEM_TABLE_SPLITTER + AGGREGATION;
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
        return new AggregationTable(fileIO, location);
    }

    private class SchemasScan extends ReadOnceTableScan {

        @Override
        public InnerTableScan withFilter(Predicate predicate) {
            return this;
        }

        @Override
        public Plan innerPlan() {
            return () -> Collections.singletonList(new AggregationSplit(fileIO, location));
        }
    }

    /** {@link Split} implementation for {@link AggregationTable}. */
    private static class AggregationSplit implements Split {

        private static final long serialVersionUID = 1L;

        private final FileIO fileIO;
        private final Path location;

        private AggregationSplit(FileIO fileIO, Path location) {
            this.fileIO = fileIO;
            this.location = location;
        }

        @Override
        public long rowCount() {
            return new SchemaManager(fileIO, location).listAllIds().size();
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

    /** {@link TableRead} implementation for {@link AggregationTable}. */
    private static class SchemasRead implements InnerTableRead {

        private final FileIO fileIO;
        private int[][] projection;

        public SchemasRead(FileIO fileIO) {
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
        public RecordReader<InternalRow> createReader(Split split) {
            if (!(split instanceof AggregationSplit)) {
                throw new IllegalArgumentException("Unsupported split: " + split.getClass());
            }
            Path location = ((AggregationSplit) split).location;
            TableSchema schemas = new SchemaManager(fileIO, location).latest().get();
            Iterator<InternalRow> rows = createInternalRowIterator(schemas);
            if (projection != null) {
                rows =
                        Iterators.transform(
                                rows, row -> ProjectedRow.from(projection).replaceRow(row));
            }
            return new IteratorRecordReader<>(rows);
        }

        private Iterator<InternalRow> createInternalRowIterator(TableSchema schema) {
            Map<String, String> fieldMap = extractFieldMap(schema.options());
            List<InternalRow> internalRows = new ArrayList<>();

            GenericRow genericRow;
            for (int i = 0; i < schema.fields().size(); i++) {
                String fieldName = schema.fields().get(i).name();
                genericRow =
                        GenericRow.of(
                                BinaryString.fromString(fieldName),
                                BinaryString.fromString(schema.fields().get(i).type().toString()),
                                BinaryString.fromString(fieldMap.get(fieldName)),
                                BinaryString.fromString(schema.fields().get(i).description()));
                internalRows.add(genericRow);
            }
            return internalRows.iterator();
        }
    }

    protected static Map<String, String> extractFieldMap(Map<String, String> inputMap) {
        Map<String, String> fieldMap = new HashMap<>();

        for (Map.Entry<String, String> entry : inputMap.entrySet()) {
            String[] keys = entry.getKey().split("\\.");
            if (keys.length > 2 && keys[0].equals("fields")) {
                String fieldKey = keys[1];
                String value = entry.getValue();
                fieldMap.put(fieldKey, value);
            }
        }
        return fieldMap;
    }
}
