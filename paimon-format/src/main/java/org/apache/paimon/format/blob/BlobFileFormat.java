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

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.format.EmptyStatsExtractor;
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.format.FormatReaderFactory;
import org.apache.paimon.format.FormatWriter;
import org.apache.paimon.format.FormatWriterFactory;
import org.apache.paimon.format.SimpleStatsExtractor;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.reader.FileRecordReader;
import org.apache.paimon.statistics.SimpleColStatsCollector;
import org.apache.paimon.types.DataTypeRoot;
import org.apache.paimon.types.RowType;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** {@link FileFormat} for blob file. */
public class BlobFileFormat extends FileFormat {

    private final boolean blobAsDescriptor;

    public BlobFileFormat() {
        this(false);
    }

    public BlobFileFormat(boolean blobAsDescriptor) {
        super(BlobFileFormatFactory.IDENTIFIER);
        this.blobAsDescriptor = blobAsDescriptor;
    }

    public static boolean isBlobFile(String fileName) {
        return fileName.endsWith("." + BlobFileFormatFactory.IDENTIFIER);
    }

    @Override
    public FormatReaderFactory createReaderFactory(
            RowType dataSchemaRowType,
            RowType projectedRowType,
            @Nullable List<Predicate> filters) {
        return new BlobFormatReaderFactory(blobAsDescriptor);
    }

    @Override
    public FormatWriterFactory createWriterFactory(RowType type) {
        return new BlobFormatWriterFactory();
    }

    @Override
    public void validateDataFields(RowType rowType) {
        checkArgument(rowType.getFieldCount() == 1, "BlobFileFormat only support one field.");
        checkArgument(
                rowType.getField(0).type().getTypeRoot() == DataTypeRoot.BLOB,
                "BlobFileFormat only support blob type.");
    }

    @Override
    public Optional<SimpleStatsExtractor> createStatsExtractor(
            RowType type, SimpleColStatsCollector.Factory[] statsCollectors) {
        // return a empty stats extractor to avoid stats calculation
        return Optional.of(new EmptyStatsExtractor());
    }

    private static class BlobFormatWriterFactory implements FormatWriterFactory {

        @Override
        public FormatWriter create(PositionOutputStream out, String compression) {
            return new BlobFormatWriter(out);
        }
    }

    private static class BlobFormatReaderFactory implements FormatReaderFactory {

        private final boolean blobAsDescriptor;

        public BlobFormatReaderFactory(boolean blobAsDescriptor) {
            this.blobAsDescriptor = blobAsDescriptor;
        }

        @Override
        public FileRecordReader<InternalRow> createReader(Context context) throws IOException {
            return new BlobFormatReader(
                    context.fileIO(),
                    context.filePath(),
                    context.fileSize(),
                    context.selection(),
                    blobAsDescriptor);
        }
    }
}
