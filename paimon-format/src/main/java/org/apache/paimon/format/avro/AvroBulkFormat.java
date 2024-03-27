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

package org.apache.paimon.format.avro;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.format.FormatReaderFactory;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.IOUtils;
import org.apache.paimon.utils.IteratorResultIterator;
import org.apache.paimon.utils.Pool;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.SeekableInput;
import org.apache.avro.io.DatumReader;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Iterator;

/** Provides a {@link FormatReaderFactory} for Avro records. */
public class AvroBulkFormat implements FormatReaderFactory {

    private static final long serialVersionUID = 1L;

    protected final RowType projectedRowType;

    public AvroBulkFormat(RowType projectedRowType) {
        this.projectedRowType = projectedRowType;
    }

    @Override
    public RecordReader<InternalRow> createReader(FormatReaderFactory.Context context)
            throws IOException {
        return new AvroReader(context.fileIO(), context.filePath(), context.fileSize());
    }

    private class AvroReader implements RecordReader<InternalRow> {

        private final FileIO fileIO;
        private final DataFileReader<InternalRow> reader;

        private final long end;
        private final Pool<Object> pool;
        private final Path filePath;
        private long currentRowPosition;

        private AvroReader(FileIO fileIO, Path path, long fileSize) throws IOException {
            this.fileIO = fileIO;
            this.end = fileSize;
            this.reader = createReaderFromPath(path, end);
            this.reader.sync(0);
            this.pool = new Pool<>(1);
            this.pool.add(new Object());
            this.filePath = path;
            this.currentRowPosition = 0;
        }

        private DataFileReader<InternalRow> createReaderFromPath(Path path, long fileSize)
                throws IOException {
            DatumReader<InternalRow> datumReader = new AvroRowDatumReader(projectedRowType);
            SeekableInput in =
                    new SeekableInputStreamWrapper(fileIO.newInputStream(path), fileSize);
            try {
                return (DataFileReader<InternalRow>) DataFileReader.openReader(in, datumReader);
            } catch (Throwable e) {
                IOUtils.closeQuietly(in);
                throw e;
            }
        }

        @Nullable
        @Override
        public RecordIterator<InternalRow> readBatch() throws IOException {
            Object ticket;
            try {
                ticket = pool.pollEntry();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(
                        "Interrupted while waiting for the previous batch to be consumed", e);
            }

            if (!readNextBlock()) {
                pool.recycler().recycle(ticket);
                return null;
            }

            long rowPosition = currentRowPosition;
            currentRowPosition += reader.getBlockCount();
            Iterator<InternalRow> iterator = new AvroBlockIterator(reader.getBlockCount(), reader);
            return new IteratorResultIterator<>(
                    iterator, () -> pool.recycler().recycle(ticket), filePath, rowPosition);
        }

        private boolean readNextBlock() throws IOException {
            // read the next block with reader,
            // returns true if a block is read and false if we reach the end of this split
            return reader.hasNext() && !reader.pastSync(end);
        }

        @Override
        public void close() throws IOException {
            reader.close();
        }
    }

    private static class AvroBlockIterator implements Iterator<InternalRow> {

        private long numRecordsRemaining;
        private final DataFileReader<InternalRow> reader;

        private AvroBlockIterator(long numRecordsRemaining, DataFileReader<InternalRow> reader) {
            this.numRecordsRemaining = numRecordsRemaining;
            this.reader = reader;
        }

        @Override
        public boolean hasNext() {
            return numRecordsRemaining > 0;
        }

        @Override
        public InternalRow next() {
            try {
                numRecordsRemaining--;
                // reader.next merely deserialize bytes in memory to java objects
                // and will not read from file
                // Do not reuse object, manifest file assumes no object reuse
                return reader.next(null);
            } catch (IOException e) {
                throw new RuntimeException(
                        "Encountered exception when reading from avro format file", e);
            }
        }
    }
}
