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

package org.apache.paimon.format.blob;

import org.apache.paimon.data.Blob;
import org.apache.paimon.data.BlobData;
import org.apache.paimon.data.BlobRef;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.format.FormatReaderContext;
import org.apache.paimon.format.FormatReaderFactory;
import org.apache.paimon.format.FormatWriter;
import org.apache.paimon.format.FormatWriterFactory;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.RoaringBitmap32;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link BlobFileFormat}. */
public class BlobFileFormatTest {

    @TempDir java.nio.file.Path tempPath;

    protected FileIO fileIO;
    protected Path file;
    protected Path parent;

    @BeforeEach
    public void beforeEach() {
        this.fileIO = LocalFileIO.create();
        this.parent = new Path(tempPath.toUri());
        this.file = new Path(new Path(tempPath.toUri()), UUID.randomUUID().toString());
    }

    @Test
    public void testBlobAsDescriptor() throws IOException {
        innerTest(true);
    }

    @Test
    public void testReadBlobInlineBytes() throws IOException {
        innerTest(false);
    }

    private void innerTest(boolean blobAsDescriptor) throws IOException {
        BlobFileFormat format = new BlobFileFormat(blobAsDescriptor);
        RowType rowType = RowType.of(DataTypes.BLOB());

        // write
        FormatWriterFactory writerFactory = format.createWriterFactory(rowType);
        List<byte[]> blobs =
                Arrays.asList("hello".getBytes(), null, "world".getBytes(), new byte[0]);
        try (PositionOutputStream out = fileIO.newOutputStream(file, false)) {
            FormatWriter formatWriter = writerFactory.create(out, null);
            for (byte[] bytes : blobs) {
                if (bytes == null) {
                    formatWriter.addElement(GenericRow.of((Object) null));
                    continue;
                } else {
                    formatWriter.addElement(GenericRow.of(new BlobData(bytes)));
                }
            }
            formatWriter.close();
        }

        // read
        FormatReaderFactory readerFactory = format.createReaderFactory(null, rowType, null);
        FormatReaderContext context =
                new FormatReaderContext(fileIO, file, fileIO.getFileSize(file));
        List<byte[]> result = new ArrayList<>();
        readerFactory
                .createReader(context)
                .forEachRemaining(
                        row -> {
                            if (row.isNullAt(0)) {
                                result.add(null);
                            } else {
                                Blob blob = row.getBlob(0);
                                if (blobAsDescriptor) {
                                    assertThat(blob).isInstanceOf(BlobRef.class);
                                } else {
                                    assertThat(blob).isInstanceOf(BlobData.class);
                                }
                                result.add(blob.toData());
                            }
                        });

        // assert
        assertThat(result).containsExactlyElementsOf(blobs);

        // read with selection
        RoaringBitmap32 selection = new RoaringBitmap32();
        selection.add(2);
        context = new FormatReaderContext(fileIO, file, fileIO.getFileSize(file), selection);
        result.clear();
        readerFactory
                .createReader(context)
                .forEachRemaining(row -> result.add(row.getBlob(0).toData()));

        // assert
        assertThat(result).containsOnly(blobs.get(2));
    }

    @Test
    public void testReadWithProjectedRowTypeContainingExtraFields() throws IOException {
        BlobFileFormat format = new BlobFileFormat(false);
        RowType writeRowType = RowType.of(DataTypes.BLOB());

        // write blob data
        List<byte[]> blobs = Arrays.asList("hello".getBytes(), "world".getBytes());
        try (PositionOutputStream out = fileIO.newOutputStream(file, false)) {
            FormatWriter formatWriter = format.createWriterFactory(writeRowType).create(out, null);
            for (byte[] bytes : blobs) {
                formatWriter.addElement(GenericRow.of(new BlobData(bytes)));
            }
            formatWriter.close();
        }

        // read with a projectedRowType that has extra fields (simulating _ROW_ID scenario)
        // projectedRowType: <BIGINT, BLOB> — blob is at index 1
        RowType projectedRowType = RowType.of(DataTypes.BIGINT(), DataTypes.BLOB());
        FormatReaderFactory readerFactory =
                format.createReaderFactory(null, projectedRowType, null);
        FormatReaderContext context =
                new FormatReaderContext(fileIO, file, fileIO.getFileSize(file));

        List<InternalRow> rows = new ArrayList<>();
        readerFactory.createReader(context).forEachRemaining(rows::add);

        assertThat(rows).hasSize(2);
        for (InternalRow row : rows) {
            // row should have 2 fields
            assertThat(row.getFieldCount()).isEqualTo(2);
            // field 0 (BIGINT) should be null (default value)
            assertThat(row.isNullAt(0)).isTrue();
            // field 1 (BLOB) should contain data
            assertThat(row.isNullAt(1)).isFalse();
        }
        assertThat(rows.get(0).getBlob(1).toData()).isEqualTo("hello".getBytes());
        assertThat(rows.get(1).getBlob(1).toData()).isEqualTo("world".getBytes());
    }
}
