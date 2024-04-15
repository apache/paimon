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
import org.apache.paimon.options.Options;
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
import org.apache.paimon.tag.Tag;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.TimestampType;
import org.apache.paimon.utils.DateTimeUtils;
import org.apache.paimon.utils.IteratorRecordReader;
import org.apache.paimon.utils.ProjectedRow;
import org.apache.paimon.utils.SerializationUtils;
import org.apache.paimon.utils.TagManager;

import org.apache.paimon.shade.guava30.com.google.common.collect.Iterators;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.SortedMap;

import static org.apache.paimon.catalog.Catalog.SYSTEM_TABLE_SPLITTER;

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
                            new DataField(5, "branches", SerializationUtils.newStringType(true)),
                            new DataField(6, "create_time", new TimestampType(false, 3)),
                            new DataField(
                                    7, "time_retained", SerializationUtils.newStringType(true))));

    private final FileIO fileIO;
    private final Path location;

    public TagsTable(FileIO fileIO, Path location) {
        this.fileIO = fileIO;
        this.location = location;
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
        return new TagsTable(fileIO, location);
    }

    private class TagsScan extends ReadOnceTableScan {

        @Override
        public InnerTableScan withFilter(Predicate predicate) {
            // TODO
            return this;
        }

        @Override
        public Plan innerPlan() {
            return () ->
                    Collections.singletonList(
                            new TagsSplit(new TagManager(fileIO, location).tagCount(), location));
        }
    }

    private static class TagsSplit implements Split {
        private static final long serialVersionUID = 1L;

        private final long rowCount;
        private final Path location;

        private TagsSplit(long rowCount, Path location) {
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
            TagsSplit that = (TagsSplit) o;
            return Objects.equals(location, that.location);
        }

        @Override
        public int hashCode() {
            return Objects.hash(location);
        }
    }

    private static class TagsRead implements InnerTableRead {

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
            Options options = new Options();
            options.set(CoreOptions.PATH, location.toUri().toString());
            FileStoreTable table = FileStoreTableFactory.create(fileIO, options);
            SortedMap<Tag, List<String>> tags = table.tagManager().tagsWithTimeRetained();
            Map<String, Tag> nameToSnapshot = new LinkedHashMap<>();
            for (Map.Entry<Tag, List<String>> tag : tags.entrySet()) {
                for (String tagName : tag.getValue()) {
                    nameToSnapshot.put(tagName, tag.getKey());
                }
            }
            Map<String, List<String>> tagBranches = new HashMap<>();
            table.branchManager()
                    .branches()
                    .forEach(
                            branch ->
                                    tagBranches
                                            .computeIfAbsent(
                                                    branch.getCreatedFromTag(),
                                                    key -> new ArrayList<>())
                                            .add(branch.getBranchName()));

            Iterator<InternalRow> rows =
                    Iterators.transform(
                            nameToSnapshot.entrySet().iterator(), tag -> toRow(tag, tagBranches));
            if (projection != null) {
                rows =
                        Iterators.transform(
                                rows, row -> ProjectedRow.from(projection).replaceRow(row));
            }
            return new IteratorRecordReader<>(rows);
        }

        private InternalRow toRow(
                Map.Entry<String, Tag> snapshot, Map<String, List<String>> tagBranches) {
            Tag tag = snapshot.getValue();
            List<String> branches = tagBranches.get(snapshot.getKey());
            return GenericRow.of(
                    BinaryString.fromString(snapshot.getKey()),
                    tag.id(),
                    tag.schemaId(),
                    Timestamp.fromLocalDateTime(DateTimeUtils.toLocalDateTime(tag.timeMillis())),
                    tag.totalRecordCount(),
                    BinaryString.fromString(branches == null ? "[]" : branches.toString()),
                    Timestamp.fromLocalDateTime(
                            tag.getTagCreateTime() == null
                                    ? LocalDateTime.MIN
                                    : tag.getTagCreateTime()),
                    BinaryString.fromString(
                            tag.getTagTimeRetained() == null
                                    ? ""
                                    : tag.getTagTimeRetained().toString()));
        }
    }
}
