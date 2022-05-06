/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.store.file.utils;

import org.apache.flink.api.common.serialization.BulkWriter;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.data.RowData;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

/**
 * A utility class to write a list of objects into several files, each with a size limit.
 *
 * @param <R> record type
 * @param <F> file meta type
 */
public abstract class RollingFile<R, F> {

    private static final Logger LOG = LoggerFactory.getLogger(RollingFile.class);

    private final long suggestedFileSize;

    public RollingFile(long suggestedFileSize) {
        this.suggestedFileSize = suggestedFileSize;
    }

    /** Create the path for a new file. */
    protected abstract Path newPath();

    /** Create a new object writer. Called per file. */
    protected abstract BulkWriter<RowData> newWriter(FSDataOutputStream out) throws IOException;

    /**
     * Called before writing a record into file. Per-record calculation can be performed here.
     *
     * @param record record to write
     * @return serialized record
     */
    protected abstract RowData toRowData(R record);

    /** Called before closing the current file. Per-file calculation can be performed here. */
    protected abstract F collectFile(Path path) throws IOException;

    public void write(Iterator<R> iterator, List<F> result, List<Path> filesToCleanUp)
            throws IOException {
        Writer writer = null;
        Path currentPath = null;

        while (iterator.hasNext()) {
            if (writer == null) {
                // create new rolling file
                currentPath = newPath();
                filesToCleanUp.add(currentPath);
                writer = new Writer(currentPath);
            }

            RowData serialized = toRowData(iterator.next());
            writer.write(serialized);

            if (writer.exceedsSuggestedFileSize()) {
                // exceeds suggested size, close current file
                writer.finish();
                result.add(collectFile(currentPath));
                writer = null;
            }
        }

        // finish last file
        if (writer != null) {
            writer.finish();
            result.add(collectFile(currentPath));
        }
    }

    private class Writer {
        private final FSDataOutputStream out;
        private final BulkWriter<RowData> writer;

        private Writer(Path path) throws IOException {
            this.out = path.getFileSystem().create(path, FileSystem.WriteMode.NO_OVERWRITE);
            this.writer = newWriter(out);

            if (LOG.isDebugEnabled()) {
                LOG.debug("Create new rolling file " + path);
            }
        }

        private void write(RowData record) throws IOException {
            writer.addElement(record);
        }

        private boolean exceedsSuggestedFileSize() throws IOException {
            // NOTE: this method is inaccurate for formats buffering changes in memory
            return out.getPos() >= suggestedFileSize;
        }

        private void finish() throws IOException {
            writer.finish();
            out.close();
        }
    }
}
