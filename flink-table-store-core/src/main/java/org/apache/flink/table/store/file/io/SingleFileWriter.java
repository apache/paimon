/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.table.store.file.io;

import org.apache.flink.api.common.serialization.BulkWriter;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.store.file.utils.FileUtils;
import org.apache.flink.util.IOUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.function.Function;

/**
 * A {@link FileWriter} to produce a single file.
 *
 * @param <T> type of records to write.
 * @param <R> type of result to produce after writing a file.
 */
public abstract class SingleFileWriter<T, R> implements FileWriter<T, R> {

    private static final Logger LOG = LoggerFactory.getLogger(SingleFileWriter.class);

    private final BulkWriter.Factory<RowData> factory;
    protected final Path path;
    private final Function<T, RowData> converter;

    private long recordCount;
    private long length;
    protected boolean closed;

    private BulkWriter<RowData> writer;
    private FSDataOutputStream out;

    public SingleFileWriter(
            BulkWriter.Factory<RowData> factory, Path path, Function<T, RowData> converter) {
        this.factory = factory;
        this.path = path;
        this.converter = converter;

        this.recordCount = 0;
        this.length = 0;
        this.closed = false;

        this.writer = null;
        this.out = null;
    }

    public Path path() {
        return path;
    }

    @Override
    public void write(T record) throws IOException {
        writeImpl(record);
    }

    protected RowData writeImpl(T record) throws IOException {
        if (closed) {
            throw new RuntimeException("Writer has already closed!");
        }

        try {
            if (writer == null) {
                writer = createWriter();
            }
            RowData rowData = converter.apply(record);
            writer.addElement(rowData);
            recordCount++;
            return rowData;
        } catch (Throwable e) {
            LOG.warn("Exception occurs when writing file " + path + ". Cleaning up.", e);
            abort();
            throw e;
        }
    }

    @Override
    public long recordCount() {
        return recordCount;
    }

    @Override
    public long length() throws IOException {
        if (closed) {
            return length;
        } else {
            return out.getPos();
        }
    }

    @Override
    public void abort() {
        IOUtils.closeQuietly(out);
        FileUtils.deleteOrWarn(path);
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("Closing file " + path);
        }

        try {
            if (writer != null) {
                writer.flush();
                writer.finish();
            }

            if (out != null) {
                out.flush();
                length = out.getPos();
                out.close();
            }
        } catch (IOException e) {
            LOG.warn("Exception occurs when closing file " + path + ". Cleaning up.", e);
            abort();
            throw e;
        } finally {
            closed = true;
        }
    }

    private BulkWriter<RowData> createWriter() throws IOException {
        FileSystem fs = path.getFileSystem();
        out = fs.create(path, FileSystem.WriteMode.NO_OVERWRITE);
        try {
            return factory.create(out);
        } catch (Throwable e) {
            LOG.warn(
                    "Failed to open the bulk writer, closing the output stream and throw the error.",
                    e);
            IOUtils.closeQuietly(out);
            throw e;
        }
    }
}
