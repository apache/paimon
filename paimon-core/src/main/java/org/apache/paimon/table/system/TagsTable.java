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
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
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
import org.apache.paimon.tag.Tag;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.TimestampType;
import org.apache.paimon.utils.DateTimeUtils;
import org.apache.paimon.utils.IteratorRecordReader;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.ProjectedRow;
import org.apache.paimon.utils.SerializationUtils;
import org.apache.paimon.utils.TagManager;

import org.apache.paimon.shade.guava30.com.google.common.collect.Iterators;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static org.apache.paimon.catalog.Catalog.SYSTEM_TABLE_SPLITTER;
import static org.apache.paimon.utils.BranchManager.DEFAULT_MAIN_BRANCH;

/** A {@link Table} for showing tags of table. */
public class TagsTable implements ReadonlyTable {

    private static final long serialVersionUID = 1L;

    public static final String TAGS = "tags";

    public static final RowType TABLE_TYPE =
            new RowType(
                    Arrays.asList(
                            new DataField(0, "tag_name", SerializationUtils.newStringType(false)),
                            new DataField(1, "snapshot_id", new BigIntType(false)),
                            new DataField(2, "schema_id", new BigIntType(false)),
                            new DataField(3, "commit_time", new TimestampType(false, 3)),
                            new DataField(4, "record_count", new BigIntType(true)),
                            new DataField(5, "create_time", new TimestampType(true, 3)),
                            new DataField(
                                    6, "time_retained", SerializationUtils.newStringType(true))));

    private final FileIO fileIO;
    private final Path location;
    private final String branch;

    public TagsTable(FileIO fileIO, Path location) {
        this(fileIO, location, DEFAULT_MAIN_BRANCH);
    }

    public TagsTable(FileIO fileIO, Path location, String branchName) {
        this.fileIO = fileIO;
        this.location = location;
        this.branch = branchName;
    }

    @Override
    public String name() {
        return location.getName() + SYSTEM_TABLE_SPLITTER + TAGS;
    }

    @Override
    public RowType rowType() {
        return TABLE_TYPE;
    }

    @Override
    public List<String> primaryKeys() {
        return Collections.singletonList("tag_name");
    }

    @Override
    public InnerTableScan newScan() {
        return new TagsScan();
    }

    @Override
    public InnerTableRead newRead() {
        return new TagsRead(fileIO);
    }

    @Override
    public Table copy(Map<String, String> dynamicOptions) {
        return new TagsTable(fileIO, location, branch);
    }

    private class TagsScan extends ReadOnceTableScan {

        @Override
        public InnerTableScan withFilter(Predicate predicate) {
            // TODO
            return this;
        }

        @Override
        public Plan innerPlan() {
            return () -> Collections.singletonList(new TagsSplit(location));
        }
    }

    private class TagsSplit extends SingletonSplit {

        private static final long serialVersionUID = 1L;

        private final Path location;

        private TagsSplit(Path location) {
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
            TagsSplit that = (TagsSplit) o;
            return Objects.equals(location, that.location);
        }

        @Override
        public int hashCode() {
            return Objects.hash(location);
        }
    }

    private class TagsRead implements InnerTableRead {

        private final FileIO fileIO;
        private int[][] projection;

        public TagsRead(FileIO fileIO) {
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
            if (!(split instanceof TagsSplit)) {
                throw new IllegalArgumentException("Unsupported split: " + split.getClass());
            }
            Path location = ((TagsSplit) split).location;
            List<Pair<Tag, String>> tags = new TagManager(fileIO, location, branch).tagObjects();
            Map<String, Tag> nameToSnapshot = new LinkedHashMap<>();
            for (Pair<Tag, String> tag : tags) {
                nameToSnapshot.put(tag.getValue(), tag.getKey());
            }

            Iterator<InternalRow> rows =
                    Iterators.transform(nameToSnapshot.entrySet().iterator(), this::toRow);
            if (projection != null) {
                rows =
                        Iterators.transform(
                                rows, row -> ProjectedRow.from(projection).replaceRow(row));
            }
            return new IteratorRecordReader<>(rows);
        }

        private InternalRow toRow(Map.Entry<String, Tag> snapshot) {
            Tag tag = snapshot.getValue();
            return GenericRow.of(
                    BinaryString.fromString(snapshot.getKey()),
                    tag.id(),
                    tag.schemaId(),
                    Timestamp.fromLocalDateTime(DateTimeUtils.toLocalDateTime(tag.timeMillis())),
                    tag.totalRecordCount(),
                    Optional.ofNullable(tag.getTagCreateTime())
                            .map(Timestamp::fromLocalDateTime)
                            .orElse(null),
                    Optional.ofNullable(tag.getTagTimeRetained())
                            .map(Object::toString)
                            .map(BinaryString::fromString)
                            .orElse(null));
        }
    }
}
