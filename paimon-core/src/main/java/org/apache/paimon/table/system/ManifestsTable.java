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
import org.apache.paimon.Snapshot;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.manifest.ManifestList;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.FileStoreTable;
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
import org.apache.paimon.utils.FileStorePathFactory;
import org.apache.paimon.utils.IteratorRecordReader;
import org.apache.paimon.utils.ProjectedRow;
import org.apache.paimon.utils.SerializationUtils;
import org.apache.paimon.utils.SnapshotManager;

import org.apache.paimon.shade.guava30.com.google.common.collect.Iterators;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.apache.paimon.catalog.Catalog.SYSTEM_TABLE_SPLITTER;

/** A {@link Table} for showing committing snapshots of table. */
public class ManifestsTable implements ReadonlyTable {
    private static final long serialVersionUID = 1L;

    public static final String MANIFESTS = "manifests";

    public static final RowType TABLE_TYPE =
            new RowType(
                    Arrays.asList(
                            new DataField(0, "file_name", SerializationUtils.newStringType(false)),
                            new DataField(1, "file_size", new BigIntType(false)),
                            new DataField(2, "num_added_files", new BigIntType(false)),
                            new DataField(3, "num_deleted_files", new BigIntType(false)),
                            new DataField(4, "schema_id", new BigIntType(false))));

    private final FileStoreTable dataTable;

    public ManifestsTable(FileStoreTable dataTable) {
        this.dataTable = dataTable;
    }

    @Override
    public InnerTableScan newScan() {
        return new ManifestsScan();
    }

    @Override
    public InnerTableRead newRead() {
        return new ManifestsRead(dataTable);
    }

    @Override
    public String name() {
        return dataTable.name() + SYSTEM_TABLE_SPLITTER + MANIFESTS;
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
        return new ManifestsTable(dataTable.copy(dynamicOptions));
    }

    private class ManifestsScan extends ReadOnceTableScan {

        @Override
        public InnerTableScan withFilter(Predicate predicate) {
            // TODO
            return this;
        }

        @Override
        protected Plan innerPlan() {
            return () ->
                    Collections.singletonList(new ManifestsSplit(allManifests(dataTable).size()));
        }
    }

    private static class ManifestsSplit implements Split {

        private static final long serialVersionUID = 1L;

        private final long rowCount;

        private ManifestsSplit(long rowCount) {
            this.rowCount = rowCount;
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
            ManifestsSplit that = (ManifestsSplit) o;
            return Objects.equals(rowCount, that.rowCount);
        }

        @Override
        public int hashCode() {
            return Objects.hash(rowCount);
        }
    }

    private static class ManifestsRead implements InnerTableRead {

        private int[][] projection;

        private final FileStoreTable dataTable;

        public ManifestsRead(FileStoreTable dataTable) {
            this.dataTable = dataTable;
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
        public RecordReader<InternalRow> createReader(Split split) throws IOException {
            if (!(split instanceof ManifestsSplit)) {
                throw new IllegalArgumentException("Unsupported split: " + split.getClass());
            }
            List<ManifestFileMeta> manifestFileMetas = allManifests(dataTable);

            Iterator<InternalRow> rows =
                    Iterators.transform(manifestFileMetas.iterator(), this::toRow);
            if (projection != null) {
                rows =
                        Iterators.transform(
                                rows, row -> ProjectedRow.from(projection).replaceRow(row));
            }
            return new IteratorRecordReader<>(rows);
        }

        private InternalRow toRow(ManifestFileMeta manifestFileMeta) {
            return GenericRow.of(
                    BinaryString.fromString(manifestFileMeta.fileName()),
                    manifestFileMeta.fileSize(),
                    manifestFileMeta.numAddedFiles(),
                    manifestFileMeta.numDeletedFiles(),
                    manifestFileMeta.schemaId());
        }
    }

    private static List<ManifestFileMeta> allManifests(FileStoreTable dataTable) {
        CoreOptions coreOptions = CoreOptions.fromMap(dataTable.options());
        SnapshotManager snapshotManager = dataTable.snapshotManager();
        Long snapshotId = coreOptions.scanSnapshotId();
        Snapshot snapshot = null;
        if (snapshotId != null && snapshotManager.snapshotExists(snapshotId)) {
            snapshot = snapshotManager.snapshot(snapshotId);
        } else if (snapshotId == null) {
            snapshot = snapshotManager.latestSnapshot();
        }

        if (snapshot == null) {
            return Collections.emptyList();
        }
        FileStorePathFactory fileStorePathFactory = dataTable.store().pathFactory();
        FileFormat fileFormat = coreOptions.manifestFormat();
        ManifestList manifestList =
                new ManifestList.Factory(dataTable.fileIO(), fileFormat, fileStorePathFactory, null)
                        .create();
        return snapshot.allManifests(manifestList);
    }
}
