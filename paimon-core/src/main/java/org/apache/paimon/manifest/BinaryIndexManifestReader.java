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

package org.apache.paimon.manifest;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.fs.Path;
import org.apache.paimon.index.GlobalIndexMeta;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.CloseableIterator;
import org.apache.paimon.utils.FileUtils;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Reader for projected index manifest entries without materializing index metadata objects. */
public final class BinaryIndexManifestReader {

    private static final RowType FULL_TYPE =
            ManifestSchemaUtils.withFormatIdentifier(IndexManifestEntry.SCHEMA);
    private static final RowType PROJECTED_TYPE = projectedType();

    private final FileStoreTable table;
    private final FileFormat fileFormat;

    public BinaryIndexManifestReader(FileStoreTable table) {
        this.table = table;
        this.fileFormat = FileFormat.manifestFormat(table.coreOptions());
    }

    /**
     * Scans the projected fields of an index manifest.
     *
     * <p>The returned iterator reuses the same mutable {@link BinaryIndexManifestEntry} for all
     * records. An entry is only valid until the next call to {@link CloseableIterator#hasNext()},
     * {@link CloseableIterator#next()}, or {@link CloseableIterator#close()}, and must not be
     * retained. The caller must close the iterator.
     */
    public CloseableIterator<BinaryIndexManifestEntry> scan(String fileName) {
        Path path = table.store().pathFactory().indexManifestFileFactory().toPath(fileName);
        BinaryIndexManifestEntry entry = new BinaryIndexManifestEntry();
        try {
            CloseableIterator<InternalRow> rows =
                    FileUtils.createFormatReader(
                                    table.fileIO(),
                                    fileFormat.createReaderFactory(
                                            FULL_TYPE, PROJECTED_TYPE, Collections.emptyList()),
                                    path,
                                    null)
                            .toCloseableIterator();
            return new CloseableIterator<BinaryIndexManifestEntry>() {
                @Override
                public boolean hasNext() {
                    entry.clear();
                    return rows.hasNext();
                }

                @Override
                public BinaryIndexManifestEntry next() {
                    entry.clear();
                    InternalRow row = rows.next();
                    return row == null ? null : entry.replace(row);
                }

                @Override
                public void close() throws Exception {
                    entry.clear();
                    rows.close();
                }
            };
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to read index manifest " + fileName, e);
        }
    }

    private static RowType projectedType() {
        List<DataField> fields = new ArrayList<>();
        fields.add(FULL_TYPE.getField(ManifestSchemaUtils.FORMAT_IDENTIFIER));
        fields.add(FULL_TYPE.getField(IndexManifestEntry.KIND));
        fields.add(FULL_TYPE.getField(IndexManifestEntry.PARTITION));
        fields.add(FULL_TYPE.getField(IndexManifestEntry.BUCKET));
        fields.add(FULL_TYPE.getField(IndexManifestEntry.INDEX_TYPE));
        fields.add(
                FULL_TYPE
                        .getField(IndexManifestEntry.GLOBAL_INDEX)
                        .newType(
                                GlobalIndexMeta.SCHEMA.project(
                                        GlobalIndexMeta.ROW_RANGE_START,
                                        GlobalIndexMeta.ROW_RANGE_END,
                                        GlobalIndexMeta.INDEX_FIELD_ID,
                                        GlobalIndexMeta.EXTRA_FIELD_IDS)));
        return new RowType(false, fields);
    }
}
