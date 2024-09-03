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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.predicate.Equal;
import org.apache.paimon.predicate.LeafPredicate;
import org.apache.paimon.predicate.LeafPredicateExtractor;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.SchemaManager;
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
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.TimestampType;
import org.apache.paimon.utils.IteratorRecordReader;
import org.apache.paimon.utils.JsonSerdeUtil;
import org.apache.paimon.utils.ProjectedRow;
import org.apache.paimon.utils.SerializationUtils;

import org.apache.paimon.shade.guava30.com.google.common.collect.Iterators;

import javax.annotation.Nullable;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.apache.paimon.catalog.Catalog.SYSTEM_TABLE_SPLITTER;

/** A {@link Table} for showing schemas of table. */
public class SchemasTable implements ReadonlyTable {

    private static final long serialVersionUID = 1L;

    public static final String SCHEMAS = "schemas";

    public static final RowType TABLE_TYPE =
            new RowType(
                    Arrays.asList(
                            new DataField(0, "schema_id", new BigIntType(false)),
                            new DataField(1, "fields", SerializationUtils.newStringType(false)),
                            new DataField(
                                    2, "partition_keys", SerializationUtils.newStringType(false)),
                            new DataField(
                                    3, "primary_keys", SerializationUtils.newStringType(false)),
                            new DataField(4, "options", SerializationUtils.newStringType(false)),
                            new DataField(5, "comment", SerializationUtils.newStringType(true)),
                            new DataField(6, "update_time", new TimestampType(false, 3))));

    private final FileIO fileIO;
    private final Path location;
    private final String branch;

    public SchemasTable(FileStoreTable dataTable) {
        this(
                dataTable.fileIO(),
                dataTable.location(),
                CoreOptions.branch(dataTable.schema().options()));
    }

    public SchemasTable(FileIO fileIO, Path location, String branchName) {
        this.fileIO = fileIO;
        this.location = location;
        this.branch = branchName;
    }

    @Override
    public String name() {
        return location.getName() + SYSTEM_TABLE_SPLITTER + SCHEMAS;
    }

    @Override
    public RowType rowType() {
        return TABLE_TYPE;
    }

    @Override
    public List<String> primaryKeys() {
        return Collections.singletonList("schema_id");
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
        return new SchemasTable(fileIO, location, branch);
    }

    private class SchemasScan extends ReadOnceTableScan {
        private @Nullable LeafPredicate schemaId;

        @Override
        public InnerTableScan withFilter(Predicate predicate) {
            if (predicate == null) {
                return this;
            }

            Map<String, LeafPredicate> leafPredicates =
                    predicate.visit(LeafPredicateExtractor.INSTANCE);
            schemaId = leafPredicates.get("schema_id");
            return this;
        }

        @Override
        public Plan innerPlan() {
            return () -> Collections.singletonList(new SchemasSplit(location, schemaId));
        }
    }

    /** {@link Split} implementation for {@link SchemasTable}. */
    private static class SchemasSplit extends SingletonSplit {

        private static final long serialVersionUID = 1L;

        private final Path location;

        private final @Nullable LeafPredicate schemaId;

        private SchemasSplit(Path location, @Nullable LeafPredicate schemaId) {
            this.location = location;
            this.schemaId = schemaId;
        }

        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            SchemasSplit that = (SchemasSplit) o;
            return Objects.equals(location, that.location)
                    && Objects.equals(schemaId, that.schemaId);
        }

        @Override
        public int hashCode() {
            return Objects.hash(location, schemaId);
        }
    }

    /** {@link TableRead} implementation for {@link SchemasTable}. */
    private class SchemasRead implements InnerTableRead {

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
            if (!(split instanceof SchemasSplit)) {
                throw new IllegalArgumentException("Unsupported split: " + split.getClass());
            }
            SchemasSplit schemasSplit = (SchemasSplit) split;
            LeafPredicate predicate = schemasSplit.schemaId;
            Path location = schemasSplit.location;
            SchemaManager manager = new SchemaManager(fileIO, location, branch);

            Collection<TableSchema> tableSchemas = Collections.emptyList();
            if (predicate != null
                    && predicate.function() instanceof Equal
                    && predicate.literals().get(0) instanceof Long) {
                Long equalValue = (Long) predicate.literals().get(0);
                if (manager.schemaExists(equalValue)) {
                    tableSchemas = Collections.singletonList(manager.schema(equalValue));
                }
            } else {
                tableSchemas = manager.listAll();
            }
            Iterator<InternalRow> rows = Iterators.transform(tableSchemas.iterator(), this::toRow);
            if (projection != null) {
                rows =
                        Iterators.transform(
                                rows, row -> ProjectedRow.from(projection).replaceRow(row));
            }
            return new IteratorRecordReader<>(rows);
        }

        private InternalRow toRow(TableSchema schema) {
            return GenericRow.of(
                    schema.id(),
                    toJson(schema.fields()),
                    toJson(schema.partitionKeys()),
                    toJson(schema.primaryKeys()),
                    toJson(schema.options()),
                    BinaryString.fromString(schema.comment()),
                    Timestamp.fromLocalDateTime(
                            LocalDateTime.ofInstant(
                                    Instant.ofEpochMilli(schema.timeMillis()),
                                    ZoneId.systemDefault())));
        }

        private BinaryString toJson(Object obj) {
            return BinaryString.fromString(JsonSerdeUtil.toFlatJson(obj));
        }
    }
}
