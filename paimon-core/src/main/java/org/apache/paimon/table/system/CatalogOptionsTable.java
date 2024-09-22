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
import org.apache.paimon.options.Options;
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
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.IteratorRecordReader;
import org.apache.paimon.utils.ProjectedRow;

import org.apache.paimon.shade.guava30.com.google.common.collect.Iterators;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.apache.paimon.utils.SerializationUtils.newStringType;

/** This is a system {@link Table} to display catalog options. */
public class CatalogOptionsTable implements ReadonlyTable {

    public static final String CATALOG_OPTIONS = "catalog_options";

    private final Options catalogOptions;

    public static final RowType TABLE_TYPE =
            new RowType(
                    Arrays.asList(
                            new DataField(0, "key", newStringType(false)),
                            new DataField(1, "value", newStringType(false))));

    public CatalogOptionsTable(Options catalogOptions) {
        this.catalogOptions = catalogOptions;
    }

    /** A name to identify this table. */
    @Override
    public String name() {
        return CATALOG_OPTIONS;
    }

    /** Returns the row type of this table. */
    @Override
    public RowType rowType() {
        return TABLE_TYPE;
    }

    @Override
    public InnerTableScan newScan() {
        return new CatalogOptionsScan();
    }

    @Override
    public InnerTableRead newRead() {
        return new CatalogOptionsRead();
    }

    /** Primary keys of this table. */
    @Override
    public List<String> primaryKeys() {
        return Collections.singletonList("key");
    }

    @Override
    public Table copy(Map<String, String> dynamicOptions) {
        return new CatalogOptionsTable(catalogOptions);
    }

    private class CatalogOptionsScan extends ReadOnceTableScan {

        @Override
        public InnerTableScan withFilter(Predicate predicate) {
            return this;
        }

        @Override
        public Plan innerPlan() {
            return () ->
                    Collections.singletonList(
                            new CatalogOptionsTable.CatalogOptionsSplit(catalogOptions));
        }
    }

    private static class CatalogOptionsSplit extends SingletonSplit {

        private static final long serialVersionUID = 1L;

        private final Map<String, String> catalogOptions;

        private CatalogOptionsSplit(Options catalogOptions) {
            this.catalogOptions = catalogOptions.toMap();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            CatalogOptionsTable.CatalogOptionsSplit that =
                    (CatalogOptionsTable.CatalogOptionsSplit) o;
            return catalogOptions.equals(that.catalogOptions);
        }

        @Override
        public int hashCode() {
            return catalogOptions.hashCode();
        }
    }

    private static class CatalogOptionsRead implements InnerTableRead {

        private RowType readType;

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
            if (!(split instanceof CatalogOptionsTable.CatalogOptionsSplit)) {
                throw new IllegalArgumentException("Unsupported split: " + split.getClass());
            }
            Iterator<InternalRow> rows =
                    Iterators.transform(
                            ((CatalogOptionsSplit) split).catalogOptions.entrySet().iterator(),
                            this::toRow);
            if (readType != null) {
                rows =
                        Iterators.transform(
                                rows,
                                row ->
                                        ProjectedRow.from(readType, CatalogOptionsTable.TABLE_TYPE)
                                                .replaceRow(row));
            }
            return new IteratorRecordReader<>(rows);
        }

        private InternalRow toRow(Map.Entry<String, String> option) {
            return GenericRow.of(
                    BinaryString.fromString(option.getKey()),
                    BinaryString.fromString(option.getValue()));
        }
    }
}
