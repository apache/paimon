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

package org.apache.paimon.table.object;

import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.FileStatus;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.RemoteIterator;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.stats.Statistics;
import org.apache.paimon.table.ReadonlyTable;
import org.apache.paimon.table.source.InnerTableRead;
import org.apache.paimon.table.source.InnerTableScan;
import org.apache.paimon.table.source.ReadOnceTableScan;
import org.apache.paimon.table.source.SingletonSplit;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.TableRead;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.IteratorRecordReader;
import org.apache.paimon.utils.ProjectedRow;

import org.apache.paimon.shade.guava30.com.google.common.collect.Iterators;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;

import static org.apache.paimon.data.BinaryString.fromString;

/** An implementation for {@link ObjectTable}. */
public class ObjectTableImpl implements ReadonlyTable, ObjectTable {

    private final Identifier identifier;
    private final FileIO fileIO;
    private final String location;
    @Nullable private final String comment;

    public ObjectTableImpl(
            Identifier identifier, FileIO fileIO, String location, @Nullable String comment) {
        this.identifier = identifier;
        this.fileIO = fileIO;
        this.location = location;
        this.comment = comment;
    }

    @Override
    public String name() {
        return identifier.getTableName();
    }

    @Override
    public String fullName() {
        return identifier.getFullName();
    }

    @Override
    public RowType rowType() {
        return SCHEMA;
    }

    @Override
    public List<String> partitionKeys() {
        return Collections.emptyList();
    }

    @Override
    public List<String> primaryKeys() {
        return Collections.emptyList();
    }

    @Override
    public Map<String, String> options() {
        return Collections.emptyMap();
    }

    @Override
    public Optional<String> comment() {
        return Optional.ofNullable(comment);
    }

    @Override
    public Optional<Statistics> statistics() {
        return ReadonlyTable.super.statistics();
    }

    @Override
    public FileIO fileIO() {
        return fileIO;
    }

    @Override
    public String location() {
        return location;
    }

    @Override
    public ObjectTable copy(Map<String, String> dynamicOptions) {
        return new ObjectTableImpl(identifier, fileIO, location, comment);
    }

    @Override
    public InnerTableScan newScan() {
        return new ObjectScan(this);
    }

    @Override
    public InnerTableRead newRead() {
        return new ObjectRead();
    }

    private static class ObjectScan extends ReadOnceTableScan {

        private final ObjectTable objectTable;

        public ObjectScan(ObjectTable objectTable) {
            this.objectTable = objectTable;
        }

        @Override
        public InnerTableScan withFilter(Predicate predicate) {
            return this;
        }

        @Override
        public Plan innerPlan() {
            return () ->
                    Collections.singletonList(
                            new ObjectSplit(objectTable.fileIO(), objectTable.location()));
        }
    }

    private static class ObjectSplit extends SingletonSplit {

        private static final long serialVersionUID = 1L;

        private final FileIO fileIO;
        private final String location;

        public ObjectSplit(FileIO fileIO, String location) {
            this.fileIO = fileIO;
            this.location = location;
        }

        public FileIO fileIO() {
            return fileIO;
        }

        public String location() {
            return location;
        }

        @Override
        public boolean equals(Object o) {
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            ObjectSplit that = (ObjectSplit) o;
            return Objects.equals(location, that.location);
        }

        @Override
        public int hashCode() {
            return Objects.hash(location);
        }

        @Override
        public OptionalLong mergedRowCount() {
            return OptionalLong.empty();
        }
    }

    private static class ObjectRead implements InnerTableRead {

        private @Nullable RowType readType;

        @Override
        public InnerTableRead withFilter(Predicate predicate) {
            // TODO
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
        public RecordReader<InternalRow> createReader(Split split) throws IOException {
            if (!(split instanceof ObjectSplit)) {
                throw new IllegalArgumentException("Unsupported split: " + split.getClass());
            }

            ObjectSplit objSplit = (ObjectSplit) split;

            FileIO fileIO = objSplit.fileIO();
            String location = objSplit.location();

            RemoteIterator<FileStatus> objIter =
                    fileIO.listFilesIterative(new Path(location), true);
            String prefix = new Path(location).toUri().getPath();
            Iterator<InternalRow> iterator =
                    new Iterator<InternalRow>() {
                        @Override
                        public boolean hasNext() {
                            try {
                                return objIter.hasNext();
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                        }

                        @Override
                        public InternalRow next() {
                            try {
                                return toRow(prefix, objIter.next());
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                        }
                    };
            if (readType != null) {
                iterator =
                        Iterators.transform(
                                iterator,
                                row ->
                                        ProjectedRow.from(readType, ObjectTable.SCHEMA)
                                                .replaceRow(row));
            }
            return new IteratorRecordReader<>(iterator);
        }
    }

    private static InternalRow toRow(String prefix, FileStatus file) {
        String path = file.getPath().toUri().getPath();
        if (!path.startsWith(prefix)) {
            throw new IllegalArgumentException(
                    String.format("File path '%s' does not contain prefix '%s'", path, prefix));
        }
        String relative = path.substring(prefix.length());
        if (relative.startsWith("/")) {
            relative = relative.substring(1);
        }
        return GenericRow.of(
                fromString(relative),
                fromString(file.getPath().getName()),
                file.getLen(),
                file.getModificationTime(),
                file.getAccessTime(),
                fromString(file.getOwner()));
    }
}
