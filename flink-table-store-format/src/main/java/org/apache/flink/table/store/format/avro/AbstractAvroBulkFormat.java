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

package org.apache.flink.table.store.format.avro;

import org.apache.flink.table.store.data.InternalRow;
import org.apache.flink.table.store.format.FormatReaderFactory;
import org.apache.flink.table.store.fs.FileIO;
import org.apache.flink.table.store.fs.Path;
import org.apache.flink.table.store.reader.RecordReader;
import org.apache.flink.table.store.utils.IOUtils;
import org.apache.flink.table.store.utils.IteratorResultIterator;
import org.apache.flink.table.store.utils.Pool;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.SeekableInput;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DatumReader;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Iterator;
import java.util.function.Function;

/**
 * Provides a {@link FormatReaderFactory} for Avro records.
 *
 * <p>NOTE: Copied from Flink.
 */
public abstract class AbstractAvroBulkFormat<A> implements FormatReaderFactory {

    private static final long serialVersionUID = 1L;

    protected final Schema readerSchema;

    protected AbstractAvroBulkFormat(Schema readerSchema) {
        this.readerSchema = readerSchema;
    }

    @Override
    public AvroReader createReader(FileIO fileIO, Path file) throws IOException {
        return createReader(fileIO, file, createReusedAvroRecord(), createConverter());
    }

    private AvroReader createReader(
            FileIO fileIO, Path file, A reuse, Function<A, InternalRow> converter)
            throws IOException {
        return new AvroReader(fileIO, file, 0, fileIO.getFileSize(file), -1, 0, reuse, converter);
    }

    protected abstract A createReusedAvroRecord();

    protected abstract Function<A, InternalRow> createConverter();

    private class AvroReader implements RecordReader<InternalRow> {

        private final FileIO fileIO;
        private final DataFileReader<A> reader;
        private final Function<A, InternalRow> converter;

        private final long end;
        private final Pool<A> pool;

        private long currentRecordsToSkip;

        private AvroReader(
                FileIO fileIO,
                Path path,
                long offset,
                long end,
                long blockStart,
                long recordsToSkip,
                A reuse,
                Function<A, InternalRow> converter)
                throws IOException {
            this.fileIO = fileIO;
            this.reader = createReaderFromPath(path);
            if (blockStart >= 0) {
                reader.seek(blockStart);
            } else {
                reader.sync(offset);
            }
            for (int i = 0; i < recordsToSkip; i++) {
                reader.next(reuse);
            }
            this.converter = converter;

            this.end = end;
            this.pool = new Pool<>(1);
            this.pool.add(reuse);

            this.currentRecordsToSkip = recordsToSkip;
        }

        private DataFileReader<A> createReaderFromPath(Path path) throws IOException {
            DatumReader<A> datumReader = new GenericDatumReader<>(null, readerSchema);
            SeekableInput in =
                    new SeekableInputStreamWrapper(
                            fileIO.newInputStream(path), fileIO.getFileSize(path));
            try {
                return (DataFileReader<A>) DataFileReader.openReader(in, datumReader);
            } catch (Throwable e) {
                IOUtils.closeQuietly(in);
                throw e;
            }
        }

        @Nullable
        @Override
        public RecordIterator<InternalRow> readBatch() throws IOException {
            A reuse;
            try {
                reuse = pool.pollEntry();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(
                        "Interrupted while waiting for the previous batch to be consumed", e);
            }

            if (!readNextBlock()) {
                pool.recycler().recycle(reuse);
                return null;
            }

            Iterator<InternalRow> iterator =
                    new AvroBlockIterator(
                            reader.getBlockCount() - currentRecordsToSkip,
                            reader,
                            reuse,
                            converter);
            currentRecordsToSkip = 0;
            return new IteratorResultIterator<>(iterator, () -> pool.recycler().recycle(reuse));
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

    private class AvroBlockIterator implements Iterator<InternalRow> {

        private long numRecordsRemaining;
        private final DataFileReader<A> reader;
        private final A reuse;
        private final Function<A, InternalRow> converter;

        private AvroBlockIterator(
                long numRecordsRemaining,
                DataFileReader<A> reader,
                A reuse,
                Function<A, InternalRow> converter) {
            this.numRecordsRemaining = numRecordsRemaining;
            this.reader = reader;
            this.reuse = reuse;
            this.converter = converter;
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
                return converter.apply(reader.next(reuse));
            } catch (IOException e) {
                throw new RuntimeException(
                        "Encountered exception when reading from avro format file", e);
            }
        }
    }
}
