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

package org.apache.flink.table.store.table.system;

import org.apache.flink.core.fs.Path;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.store.file.predicate.Predicate;
import org.apache.flink.table.store.file.schema.SchemaManager;
import org.apache.flink.table.store.file.utils.IteratorRecordReader;
import org.apache.flink.table.store.file.utils.RecordReader;
import org.apache.flink.table.store.table.Table;
import org.apache.flink.table.store.table.source.Split;
import org.apache.flink.table.store.table.source.TableRead;
import org.apache.flink.table.store.table.source.TableScan;
import org.apache.flink.table.store.utils.ProjectedRowData;
import org.apache.flink.table.types.logical.RowType;

import org.apache.flink.shaded.guava30.com.google.common.collect.Iterators;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;

import static org.apache.flink.table.store.file.catalog.Catalog.SYSTEM_TABLE_SPLITTER;
import static org.apache.flink.table.store.file.utils.SerializationUtils.newStringType;

/** A {@link Table} for showing options of table. */
public class OptionsTable implements Table {

    private static final long serialVersionUID = 1L;

    public static final String OPTIONS = "options";

    public static final RowType TABLE_TYPE =
            new RowType(
                    Arrays.asList(
                            new RowType.RowField("key", newStringType(false)),
                            new RowType.RowField("value", newStringType(false))));

    private final Path location;

    public OptionsTable(Path location) {
        this.location = location;
    }

    @Override
    public String name() {
        return location.getName() + SYSTEM_TABLE_SPLITTER + OPTIONS;
    }

    @Override
    public RowType rowType() {
        return TABLE_TYPE;
    }

    @Override
    public Path location() {
        return location;
    }

    @Override
    public TableScan newScan() {
        return new OptionsScan();
    }

    @Override
    public TableRead newRead() {
        return new OptionsRead();
    }

    @Override
    public Table copy(Map<String, String> dynamicOptions) {
        return new OptionsTable(location);
    }

    private class OptionsScan implements TableScan {

        @Override
        public TableScan withFilter(Predicate predicate) {
            return this;
        }

        @Override
        public Plan plan() {
            return () -> Collections.singletonList(new OptionsSplit(location));
        }
    }

    private static class OptionsSplit implements Split {

        private static final long serialVersionUID = 1L;

        private final Path location;

        private OptionsSplit(Path location) {
            this.location = location;
        }

        @Override
        public long rowCount() {
            return options(location).size();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            OptionsSplit that = (OptionsSplit) o;
            return Objects.equals(location, that.location);
        }

        @Override
        public int hashCode() {
            return Objects.hash(location);
        }
    }

    private static class OptionsRead implements TableRead {

        private int[][] projection;

        @Override
        public TableRead withFilter(Predicate predicate) {
            return this;
        }

        @Override
        public TableRead withProjection(int[][] projection) {
            this.projection = projection;
            return this;
        }

        @Override
        public RecordReader<RowData> createReader(Split split) throws IOException {
            if (!(split instanceof OptionsSplit)) {
                throw new IllegalArgumentException("Unsupported split: " + split.getClass());
            }
            Path location = ((OptionsSplit) split).location;
            Iterator<RowData> rows =
                    Iterators.transform(options(location).entrySet().iterator(), this::toRow);
            if (projection != null) {
                rows =
                        Iterators.transform(
                                rows, row -> ProjectedRowData.from(projection).replaceRow(row));
            }
            return new IteratorRecordReader<>(rows);
        }

        private RowData toRow(Map.Entry<String, String> option) {
            return GenericRowData.of(
                    StringData.fromString(option.getKey()),
                    StringData.fromString(option.getValue()));
        }
    }

    private static Map<String, String> options(Path location) {
        return new SchemaManager(location)
                .latest()
                .orElseThrow(() -> new RuntimeException("Table not exists."))
                .options();
    }
}
