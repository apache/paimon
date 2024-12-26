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

import org.apache.paimon.Snapshot;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.index.DeletionVectorMeta;
import org.apache.paimon.index.IndexFileHandler;
import org.apache.paimon.index.IndexFileMetaSerializer;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.ReadonlyTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.InnerTableRead;
import org.apache.paimon.table.source.InnerTableScan;
import org.apache.paimon.table.source.ReadOnceTableScan;
import org.apache.paimon.table.source.SingletonSplit;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.TableRead;
import org.apache.paimon.table.source.snapshot.TimeTravelUtil;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.IteratorRecordReader;
import org.apache.paimon.utils.ProjectedRow;
import org.apache.paimon.utils.RowDataToObjectArrayConverter;
import org.apache.paimon.utils.SerializationUtils;

import org.apache.paimon.shade.guava30.com.google.common.collect.Iterators;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.apache.paimon.catalog.Catalog.SYSTEM_TABLE_SPLITTER;
import static org.apache.paimon.utils.SerializationUtils.newStringType;

/** A {@link Table} for showing committing snapshots of table. */
public class IndexesTable implements ReadonlyTable {

    private static final Logger LOG = LoggerFactory.getLogger(IndexesTable.class);

    public static final String INDEXES = "indexes";

    public static final RowType TABLE_TYPE =
            new RowType(
                    Arrays.asList(
                            new DataField(0, "partition", SerializationUtils.newStringType(true)),
                            new DataField(1, "bucket", new IntType(false)),
                            new DataField(2, "index_type", newStringType(false)),
                            new DataField(3, "file_name", newStringType(false)),
                            new DataField(4, "file_size", new BigIntType(false)),
                            new DataField(5, "row_count", new BigIntType(false)),
                            new DataField(
                                    6,
                                    "dv_ranges",
                                    new ArrayType(true, DeletionVectorMeta.SCHEMA))));

    private final FileStoreTable dataTable;

    public IndexesTable(FileStoreTable dataTable) {
        this.dataTable = dataTable;
    }

    @Override
    public InnerTableScan newScan() {
        return new IndexesScan();
    }

    @Override
    public InnerTableRead newRead() {
        return new IndexesRead(dataTable);
    }

    @Override
    public String name() {
        return dataTable.name() + SYSTEM_TABLE_SPLITTER + INDEXES;
    }

    @Override
    public RowType rowType() {
        return TABLE_TYPE;
    }

    @Override
    public List<String> primaryKeys() {
        return Collections.singletonList("file_name");
    }

    @Override
    public Table copy(Map<String, String> dynamicOptions) {
        return new IndexesTable(dataTable.copy(dynamicOptions));
    }

    private static class IndexesScan extends ReadOnceTableScan {

        @Override
        public InnerTableScan withFilter(Predicate predicate) {
            return this;
        }

        @Override
        protected Plan innerPlan() {
            return () -> Collections.singletonList(new IndexesSplit());
        }
    }

    private static class IndexesSplit extends SingletonSplit {

        private static final long serialVersionUID = 1L;

        private IndexesSplit() {}

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            return o != null && getClass() == o.getClass();
        }
    }

    private static class IndexesRead implements InnerTableRead {

        private RowType readType;

        private final FileStoreTable dataTable;

        public IndexesRead(FileStoreTable dataTable) {
            this.dataTable = dataTable;
        }

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
            if (!(split instanceof IndexesSplit)) {
                throw new IllegalArgumentException("Unsupported split: " + split.getClass());
            }
            List<IndexManifestEntry> manifestFileMetas = allIndexEntries(dataTable);

            RowDataToObjectArrayConverter partitionConverter =
                    new RowDataToObjectArrayConverter(dataTable.schema().logicalPartitionType());

            Iterator<InternalRow> rows =
                    Iterators.transform(
                            manifestFileMetas.iterator(),
                            indexManifestEntry -> toRow(indexManifestEntry, partitionConverter));
            if (readType != null) {
                rows =
                        Iterators.transform(
                                rows,
                                row ->
                                        ProjectedRow.from(readType, IndexesTable.TABLE_TYPE)
                                                .replaceRow(row));
            }
            return new IteratorRecordReader<>(rows);
        }

        private InternalRow toRow(
                IndexManifestEntry indexManifestEntry,
                RowDataToObjectArrayConverter partitionConverter) {
            LinkedHashMap<String, DeletionVectorMeta> dvMetas =
                    indexManifestEntry.indexFile().deletionVectorMetas();
            return GenericRow.of(
                    BinaryString.fromString(
                            Arrays.toString(
                                    partitionConverter.convert(indexManifestEntry.partition()))),
                    indexManifestEntry.bucket(),
                    BinaryString.fromString(indexManifestEntry.indexFile().indexType()),
                    BinaryString.fromString(indexManifestEntry.indexFile().fileName()),
                    indexManifestEntry.indexFile().fileSize(),
                    indexManifestEntry.indexFile().rowCount(),
                    dvMetas == null
                            ? null
                            : IndexFileMetaSerializer.dvMetasToRowArrayData(dvMetas.values()));
        }
    }

    private static List<IndexManifestEntry> allIndexEntries(FileStoreTable dataTable) {
        IndexFileHandler indexFileHandler = dataTable.store().newIndexFileHandler();
        Snapshot snapshot = TimeTravelUtil.resolveSnapshot(dataTable);
        if (snapshot == null) {
            LOG.warn("Check if your snapshot is empty.");
            return Collections.emptyList();
        }
        String indexManifest = snapshot.indexManifest();
        if (indexManifest == null || !indexFileHandler.existsManifest(indexManifest)) {
            LOG.warn("indexManifest doesn't exist.");
            return Collections.emptyList();
        }

        return indexFileHandler.readManifest(indexManifest);
    }
}
