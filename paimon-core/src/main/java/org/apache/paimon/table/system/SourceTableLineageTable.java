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
import org.apache.paimon.lineage.LineageMeta;
import org.apache.paimon.lineage.LineageMetaFactory;
import org.apache.paimon.lineage.TableLineageEntity;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.ReadonlyTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.InnerTableRead;
import org.apache.paimon.table.source.InnerTableScan;
import org.apache.paimon.table.source.ReadOnceTableScan;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.TableRead;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.TimestampType;
import org.apache.paimon.types.VarCharType;
import org.apache.paimon.utils.IteratorRecordReader;
import org.apache.paimon.utils.ProjectedRow;

import org.apache.paimon.shade.guava30.com.google.common.collect.Iterators;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.apache.paimon.utils.Preconditions.checkNotNull;

/**
 * This is a system table to display all the source table lineages.
 *
 * <pre>
 *  For example:
 *     If we select * from sys.source_table_lineage, we will get
 *     database_name       table_name       job_name      create_time
 *        default            test0            job1    2023-10-22 20:35:12
 *       database1           test1            job1    2023-10-28 21:35:52
 *          ...               ...             ...             ...
 *     We can write sql to fetch the information we need.
 * </pre>
 */
public class SourceTableLineageTable implements ReadonlyTable {

    public static final String SOURCE_TABLE_LINEAGE = "source_table_lineage";

    private final LineageMetaFactory lineageMetaFactory;
    private final Map<String, String> options;

    public SourceTableLineageTable(
            LineageMetaFactory lineageMetaFactory, Map<String, String> options) {
        this.lineageMetaFactory = lineageMetaFactory;
        this.options = options;
    }

    @Override
    public InnerTableScan newScan() {
        return new ReadOnceTableScan() {
            @Override
            public InnerTableScan withFilter(Predicate predicate) {
                return this;
            }

            @Override
            protected Plan innerPlan() {
                /// TODO get the real row count for plan.
                return () -> Collections.singletonList((Split) () -> 1L);
            }
        };
    }

    @Override
    public InnerTableRead newRead() {
        return new SourceTableLineageRead(lineageMetaFactory, options);
    }

    @Override
    public String name() {
        return SOURCE_TABLE_LINEAGE;
    }

    @Override
    public RowType rowType() {
        List<DataField> fields = new ArrayList<>();
        fields.add(new DataField(0, "database_name", new VarCharType(VarCharType.MAX_LENGTH)));
        fields.add(new DataField(1, "table_name", new VarCharType(VarCharType.MAX_LENGTH)));
        fields.add(new DataField(2, "job_name", new VarCharType(VarCharType.MAX_LENGTH)));
        fields.add(new DataField(3, "create_time", new TimestampType()));
        return new RowType(fields);
    }

    @Override
    public List<String> primaryKeys() {
        return Arrays.asList("database_name", "table_name", "job_name");
    }

    @Override
    public Table copy(Map<String, String> dynamicOptions) {
        return new SourceTableLineageTable(lineageMetaFactory, options);
    }

    /** Source table lineage read. */
    private static class SourceTableLineageRead implements InnerTableRead {
        private final LineageMetaFactory lineageMetaFactory;
        private final Map<String, String> options;
        @Nullable private Predicate predicate;
        private int[][] projection;

        private SourceTableLineageRead(
                LineageMetaFactory lineageMetaFactory, Map<String, String> options) {
            this.lineageMetaFactory = lineageMetaFactory;
            this.options = options;
            this.predicate = null;
        }

        @Override
        public InnerTableRead withFilter(Predicate predicate) {
            this.predicate = predicate;
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
            try (LineageMeta lineageMeta =
                    lineageMetaFactory.create(() -> Options.fromMap(options))) {
                Iterator<TableLineageEntity> sourceTableLineages =
                        lineageMeta.sourceTableLineages(predicate);
                return new IteratorRecordReader<>(
                        Iterators.transform(
                                sourceTableLineages,
                                entity -> {
                                    checkNotNull(entity);
                                    GenericRow row =
                                            GenericRow.of(
                                                    BinaryString.fromString(entity.getDatabase()),
                                                    BinaryString.fromString(entity.getTable()),
                                                    BinaryString.fromString(entity.getJob()),
                                                    entity.getCreateTime());
                                    if (projection != null) {
                                        return ProjectedRow.from(projection).replaceRow(row);
                                    } else {
                                        return row;
                                    }
                                }));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }
}
