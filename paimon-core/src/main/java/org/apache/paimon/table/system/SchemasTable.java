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
import org.apache.paimon.predicate.And;
import org.apache.paimon.predicate.CompoundPredicate;
import org.apache.paimon.predicate.Equal;
import org.apache.paimon.predicate.GreaterOrEqual;
import org.apache.paimon.predicate.GreaterThan;
import org.apache.paimon.predicate.LeafPredicate;
import org.apache.paimon.predicate.LeafPredicateExtractor;
import org.apache.paimon.predicate.LessOrEqual;
import org.apache.paimon.predicate.LessThan;
import org.apache.paimon.predicate.Or;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateUtils;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

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
        private RowType readType;

        private Optional<Long> optionalFilterSchemaIdMax = Optional.empty();
        private Optional<Long> optionalFilterSchemaIdMin = Optional.empty();
        private List<Long> schemaIds = new ArrayList<>();

        public SchemasRead(FileIO fileIO) {
            this.fileIO = fileIO;
        }

        @Override
        public InnerTableRead withFilter(Predicate predicate) {
            if (predicate == null) {
                return this;
            }

            String leafName = "schema_id";
            if (predicate instanceof CompoundPredicate) {
                CompoundPredicate compoundPredicate = (CompoundPredicate) predicate;
                if ((compoundPredicate.function()) instanceof And) {
                    PredicateUtils.traverseCompoundPredicate(
                            predicate,
                            leafName,
                            (Predicate p) -> {
                                handleLeafPredicate(p, leafName);
                            },
                            null);
                }

                // optimize for IN filter
                if ((compoundPredicate.function()) instanceof Or) {
                    PredicateUtils.traverseCompoundPredicate(
                            predicate,
                            leafName,
                            (Predicate p) -> {
                                if (schemaIds != null) {
                                    schemaIds.add((Long) ((LeafPredicate) p).literals().get(0));
                                }
                            },
                            (Predicate p) -> {
                                schemaIds = null;
                            });
                }
            } else {
                handleLeafPredicate(predicate, leafName);
            }

            return this;
        }

        public void handleLeafPredicate(Predicate predicate, String leafName) {
            LeafPredicate snapshotPred =
                    predicate.visit(LeafPredicateExtractor.INSTANCE).get(leafName);
            if (snapshotPred != null) {
                if (snapshotPred.function() instanceof Equal) {
                    optionalFilterSchemaIdMin = Optional.of((Long) snapshotPred.literals().get(0));
                    optionalFilterSchemaIdMax = Optional.of((Long) snapshotPred.literals().get(0));
                }

                if (snapshotPred.function() instanceof GreaterThan) {
                    optionalFilterSchemaIdMin =
                            Optional.of((Long) snapshotPred.literals().get(0) + 1);
                }

                if (snapshotPred.function() instanceof GreaterOrEqual) {
                    optionalFilterSchemaIdMin = Optional.of((Long) snapshotPred.literals().get(0));
                }

                if (snapshotPred.function() instanceof LessThan) {
                    optionalFilterSchemaIdMax =
                            Optional.of((Long) snapshotPred.literals().get(0) - 1);
                }

                if (snapshotPred.function() instanceof LessOrEqual) {
                    optionalFilterSchemaIdMax = Optional.of((Long) snapshotPred.literals().get(0));
                }
            }
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
            if (!(split instanceof SchemasSplit)) {
                throw new IllegalArgumentException("Unsupported split: " + split.getClass());
            }
            SchemasSplit schemasSplit = (SchemasSplit) split;
            Path location = schemasSplit.location;
            SchemaManager manager = new SchemaManager(fileIO, location, branch);

            Collection<TableSchema> tableSchemas;
            if (schemaIds != null && !schemaIds.isEmpty()) {
                tableSchemas = manager.schemasWithId(schemaIds);
            } else {
                tableSchemas =
                        manager.listWithRange(optionalFilterSchemaIdMax, optionalFilterSchemaIdMin);
            }

            Iterator<InternalRow> rows = Iterators.transform(tableSchemas.iterator(), this::toRow);
            if (readType != null) {
                rows =
                        Iterators.transform(
                                rows,
                                row ->
                                        ProjectedRow.from(readType, SchemasTable.TABLE_TYPE)
                                                .replaceRow(row));
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
