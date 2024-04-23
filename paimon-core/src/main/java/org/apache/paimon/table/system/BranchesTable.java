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

import org.apache.paimon.branch.TableBranch;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.ReadonlyTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.InnerTableRead;
import org.apache.paimon.table.source.InnerTableScan;
import org.apache.paimon.table.source.ReadOnceTableScan;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.TableRead;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.TimestampType;
import org.apache.paimon.utils.DateTimeUtils;
import org.apache.paimon.utils.IteratorRecordReader;
import org.apache.paimon.utils.ProjectedRow;
import org.apache.paimon.utils.SerializationUtils;

import org.apache.paimon.shade.guava30.com.google.common.collect.Iterators;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.apache.paimon.catalog.Catalog.SYSTEM_TABLE_SPLITTER;

/** A {@link Table} for showing branches of table. */
public class BranchesTable implements ReadonlyTable {

    private static final long serialVersionUID = 1L;

    public static final String BRANCHES = "branches";

    public static final RowType TABLE_TYPE =
            new RowType(
                    Arrays.asList(
                            new DataField(
                                    0, "branch_name", SerializationUtils.newStringType(false)),
                            new DataField(
                                    1, "created_from_tag", SerializationUtils.newStringType(false)),
                            new DataField(2, "created_from_snapshot", new BigIntType(false)),
                            new DataField(3, "create_time", new TimestampType(false, 3))));

    private final FileIO fileIO;
    private final Path location;

    public BranchesTable(FileIO fileIO, Path location) {
        this.fileIO = fileIO;
        this.location = location;
    }

    @Override
    public String name() {
        return location.getName() + SYSTEM_TABLE_SPLITTER + BRANCHES;
    }

    @Override
    public RowType rowType() {
        return TABLE_TYPE;
    }

    @Override
    public List<String> primaryKeys() {
        return Arrays.asList("branch_name", "tag_name");
    }

    @Override
    public InnerTableScan newScan() {
        return new BranchesScan();
    }

    @Override
    public InnerTableRead newRead() {
        return new BranchesRead(fileIO);
    }

    @Override
    public Table copy(Map<String, String> dynamicOptions) {
        return new BranchesTable(fileIO, location);
    }

    private class BranchesScan extends ReadOnceTableScan {

        @Override
        public InnerTableScan withFilter(Predicate predicate) {
            // TODO
            return this;
        }

        @Override
        public Plan innerPlan() {
            FileStoreTable table = FileStoreTableFactory.create(fileIO, location);
            long rowCount = table.branchManager().branchCount();
            return () -> Collections.singletonList(new BranchesSplit(rowCount, location));
        }
    }

    private static class BranchesSplit implements Split {
        private static final long serialVersionUID = 1L;

        private final long rowCount;
        private final Path location;

        private BranchesSplit(long rowCount, Path location) {
            this.rowCount = rowCount;
            this.location = location;
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
            BranchesSplit that = (BranchesSplit) o;
            return Objects.equals(location, that.location);
        }

        @Override
        public int hashCode() {
            return Objects.hash(location);
        }
    }

    private static class BranchesRead implements InnerTableRead {

        private final FileIO fileIO;
        private int[][] projection;

        public BranchesRead(FileIO fileIO) {
            this.fileIO = fileIO;
        }

        @Override
        public InnerTableRead withFilter(Predicate predicate) {
            // TODO
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
            if (!(split instanceof BranchesSplit)) {
                throw new IllegalArgumentException("Unsupported split: " + split.getClass());
            }
            Path location = ((BranchesSplit) split).location;
            FileStoreTable table = FileStoreTableFactory.create(fileIO, location);
            List<TableBranch> branches = table.branchManager().branches();
            Iterator<InternalRow> rows = Iterators.transform(branches.iterator(), this::toRow);
            if (projection != null) {
                rows =
                        Iterators.transform(
                                rows, row -> ProjectedRow.from(projection).replaceRow(row));
            }
            return new IteratorRecordReader<>(rows);
        }

        private InternalRow toRow(TableBranch branch) {
            return GenericRow.of(
                    BinaryString.fromString(branch.getBranchName()),
                    BinaryString.fromString(branch.getCreatedFromTag()),
                    branch.getCreatedFromSnapshot(),
                    Timestamp.fromLocalDateTime(
                            DateTimeUtils.toLocalDateTime(branch.getCreateTime())));
        }
    }
}
