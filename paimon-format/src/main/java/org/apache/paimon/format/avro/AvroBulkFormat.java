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
import org.apache.paimon.fs.ByteArraySeekableStream;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.reader.FileRecordReader;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.IOUtils;
import org.apache.paimon.utils.IteratorResultIterator;
import org.apache.paimon.utils.IteratorWithException;
import org.apache.paimon.utils.Pool;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.SeekableInput;
import org.apache.avro.io.DatumReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.function.Supplier;

/** Provides a {@link FormatReaderFactory} for Avro records. */
public class AvroBulkFormat implements FormatReaderFactory {

    private static final Logger LOG = LoggerFactory.getLogger(AvroBulkFormat.class);

    private static final String CACHE_DIR =
            System.getProperty("java.io.tmpdir") + "/paimon_avro_cache";

    protected final RowType projectedRowType;

    public AvroBulkFormat(RowType projectedRowType) {
        this.projectedRowType = projectedRowType;
    }

    @Override
    public FileRecordReader<InternalRow> createReader(FormatReaderFactory.Context context)
            throws IOException {
        return new AvroReader(context.fileIO(), context.filePath(), context.fileSize());
    }

    private class AvroReader implements FileRecordReader<InternalRow> {

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

            // LOG.info("createReaderFromPath for {}, {}", path, fileSize);
            // long startTime = System.currentTimeMillis();

            // byte[] content = new byte[(int) fileSize];
            // File localCacheFile = getLocalCacheFile(path);
            // if (localCacheFile.exists() && localCacheFile.length() == fileSize) {
            //     LOG.info("Using local cache for {}: {}", path, localCacheFile);
            //     try (InputStream inputStream =
            //             new LocalFileIO.LocalSeekableInputStream(localCacheFile)) {
            //         IOUtils.readFully(inputStream, content);
            //     }
            //     LOG.info(
            //             "Loaded local cache for {}: {}, duration: {}ms",
            //             path,
            //             localCacheFile,
            //             System.currentTimeMillis() - startTime);
            // } else {
            //     LOG.info("Downloading remote file to local cache for {}: {}", path,
            // localCacheFile);
            //     try (InputStream inputStream = fileIO.newInputStream(path)) {
            //         IOUtils.readFully(inputStream, content);
            //     }
            //     LOG.info(
            //             "createReaderFromPath read byteArray for {}, bytes: {}, duration: {}ms",
            //             path,
            //             content.length,
            //             System.currentTimeMillis() - startTime);
            //     saveToLocalCache(localCacheFile, content);
            //     LOG.info(
            //             "Downloaded remote file to local cache for {}: {}, duration: {}ms",
            //             path,
            //             localCacheFile,
            //             System.currentTimeMillis() - startTime);
            // }

            SeekableInput in;
            if (fileSize > Integer.MAX_VALUE) {
                in = new SeekableInputStreamWrapper(fileIO.newInputStream(path), fileSize);
            } else {
                byte[] content = new byte[(int) fileSize];
                try (InputStream inputStream = fileIO.newInputStream(path)) {
                    IOUtils.readFully(inputStream, content);
                }
                in = new SeekableInputStreamWrapper(new ByteArraySeekableStream(content), fileSize);
            }
            try {
                return (DataFileReader<InternalRow>) DataFileReader.openReader(in, datumReader);
            } catch (Throwable e) {
                IOUtils.closeQuietly(in);
                throw e;
            }
        }

        private File getLocalCacheFile(Path path) {
            if (path.toUri().getScheme() == null) {
                // already a local file
                return new File(path.toString());
            }
            String fsPath = path.toString().split("://")[1];
            return new File(CACHE_DIR, fsPath);
        }

        private void saveToLocalCache(File localFile, byte[] content) {
            try {
                if (!localFile.getParentFile().exists()) {
                    localFile.getParentFile().mkdirs();
                }
                File tempFile = new File(localFile.getPath() + ".tmp." + System.nanoTime());
                try (FileOutputStream out = new FileOutputStream(tempFile)) {
                    out.write(content);
                }
                if (!tempFile.renameTo(localFile)) {
                    tempFile.delete();
                }
            } catch (IOException e) {
                LOG.warn("Failed to save local cache for {}: {}", localFile, e.getMessage());
            }
        }

        @Nullable
        @Override
        public IteratorResultIterator readBatch() throws IOException {

            // LOG.info("readBatch started for {}", filePath);

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

            //            LOG.info("readBatch for {} found block of {}", reader.getBlockCount());

            IteratorWithException<InternalRow, IOException> iterator =
                    new AvroBlockIterator(reader.getBlockCount(), reader);
            IteratorResultIterator out =
                    new IteratorResultIterator(
                            iterator, () -> pool.recycler().recycle(ticket), filePath, rowPosition);

            // LOG.info("readBatch finished for {}", filePath);

            return out;
        }

        private boolean readNextBlock() throws IOException {
            // read the next block with reader,
            // returns true if a block is read and false if we reach the end of this split
            return replaceAvroRuntimeException(reader::hasNext) && !reader.pastSync(end);
        }

        @Override
        public void close() throws IOException {
            reader.close();
        }
    }

    private static class AvroBlockIterator
            implements IteratorWithException<InternalRow, IOException> {

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
        public InternalRow next() throws IOException {
            numRecordsRemaining--;
            // reader.next merely deserialize bytes in memory to java objects
            // and will not read from file
            // Do not reuse object, manifest file assumes no object reuse
            return replaceAvroRuntimeException(reader::next);
        }
    }

    private static <T> T replaceAvroRuntimeException(Supplier<T> supplier) throws IOException {
        try {
            return supplier.get();
        } catch (AvroRuntimeException e) {
            if (e.getCause() != null && e.getCause() instanceof IOException) {
                throw (IOException) e.getCause();
            }
            throw e;
        }
    }
}
