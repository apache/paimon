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

/** A utility class to write a list of objects into several files, each with a size limit. */
public class RollingFile {

    /**
     * Provide logic for serializing records and producing file metas.
     *
     * @param <R> record type
     * @param <F> file meta type
     */
    public interface Context<R, F> {

        /** Create the path for a new file. */
        Path newPath();

        /** Create a new object writer. */
        BulkWriter<RowData> newWriter(FSDataOutputStream out) throws IOException;

        /**
         * Called before writing a record into file. Per-record calculation can be performed here.
         *
         * @param record record to write
         * @return serialized record
         */
        RowData serialize(R record);

        /** Called before closing the current file. Per-file calculation can be performed here. */
        F collectFile(Path path) throws IOException;
    }

    public static <R, F> void write(
            Iterator<R> iterator,
            long suggestedFileSize,
            Context<R, F> context,
            List<F> result,
            List<Path> filesToCleanUp)
            throws IOException {
        RollingFile rollingFile = null;
        Path currentPath = null;

        while (iterator.hasNext()) {
            if (rollingFile == null) {
                // create new rolling file
                currentPath = context.newPath();
                filesToCleanUp.add(currentPath);
                rollingFile = new RollingFile(currentPath, suggestedFileSize, context);
            }

            RowData serialized = context.serialize(iterator.next());
            rollingFile.write(serialized);

            if (rollingFile.exceedsSuggestedFileSize()) {
                // exceeds suggested size, close current file
                rollingFile.finish();
                result.add(context.collectFile(currentPath));
                rollingFile = null;
            }
        }

        // finish last file
        if (rollingFile != null) {
            rollingFile.finish();
            result.add(context.collectFile(currentPath));
        }
    }

    private static final Logger LOG = LoggerFactory.getLogger(RollingFile.class);

    private final long suggestedFileSize;
    private final FSDataOutputStream out;
    private final BulkWriter<RowData> writer;

    private RollingFile(Path path, long suggestedFileSize, Context<?, ?> context)
            throws IOException {
        this.suggestedFileSize = suggestedFileSize;
        this.out = path.getFileSystem().create(path, FileSystem.WriteMode.NO_OVERWRITE);
        this.writer = context.newWriter(out);

        if (LOG.isDebugEnabled()) {
            LOG.debug("Create new rolling file " + path.toString());
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
