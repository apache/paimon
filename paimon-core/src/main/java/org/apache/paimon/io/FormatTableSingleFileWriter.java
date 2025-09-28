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

package org.apache.paimon.io;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.format.FormatWriter;
import org.apache.paimon.format.FormatWriterFactory;
import org.apache.paimon.format.SupportsDirectWrite;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.TwoPhaseOutputStream;
import org.apache.paimon.utils.IOUtils;

import org.apache.paimon.shade.guava30.com.google.common.collect.Lists;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;

/** Format table's writer to produce a single file. */
public class FormatTableSingleFileWriter {

    private static final Logger LOG = LoggerFactory.getLogger(FormatTableSingleFileWriter.class);

    protected final FileIO fileIO;
    protected final Path path;

    private FormatWriter writer;
    private PositionOutputStream out;
    private TwoPhaseOutputStream.Committer committer;

    protected long outputBytes;
    protected boolean closed;

    public FormatTableSingleFileWriter(
            FileIO fileIO, FormatWriterFactory factory, Path path, String compression) {
        this.fileIO = fileIO;
        this.path = path;

        try {
            if (factory instanceof SupportsDirectWrite) {
                throw new UnsupportedOperationException("Does not support SupportsDirectWrite.");
            } else {
                out = fileIO.newTwoPhaseOutputStream(path, false);
                writer = factory.create(out, compression);
            }
        } catch (IOException e) {
            LOG.warn(
                    "Failed to open the bulk writer, closing the output stream and throw the error.",
                    e);
            if (out != null) {
                abort();
            }
            throw new UncheckedIOException(e);
        }

        this.closed = false;
    }

    public Path path() {
        return path;
    }

    public void write(InternalRow record) throws IOException {
        if (closed) {
            throw new RuntimeException("Writer has already closed!");
        }

        try {
            writer.addElement(record);
        } catch (Throwable e) {
            LOG.warn("Exception occurs when writing file {}. Cleaning up.", path, e);
            abort();
            throw e;
        }
    }

    public boolean reachTargetSize(boolean suggestedCheck, long targetSize) throws IOException {
        return writer.reachTargetSize(suggestedCheck, targetSize);
    }

    public void abort() {
        if (writer != null) {
            IOUtils.closeQuietly(writer);
            writer = null;
        }
        if (out != null) {
            try {
                committer = ((TwoPhaseOutputStream) out).closeForCommit();
            } catch (Throwable e) {
                LOG.warn("Exception occurs when close for commit out {}", committer, e);
            }
            out = null;
        }
        if (committer != null) {
            try {
                committer.discard();
            } catch (Throwable e) {
                LOG.warn("Exception occurs when close out {}", committer, e);
            }
        }
        fileIO.deleteQuietly(path);
    }

    public List<TwoPhaseOutputStream.Committer> committers() {
        if (!closed) {
            throw new RuntimeException("Writer should be closed before getting committer!");
        }
        return Lists.newArrayList(committer);
    }

    public FileWriterAbortExecutor abortExecutor() {
        if (!closed) {
            throw new RuntimeException("Writer should be closed!");
        }

        return new FileWriterAbortExecutor(fileIO, path);
    }

    public void close() throws IOException {
        if (closed) {
            return;
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("Closing file {}", path);
        }

        try {
            if (writer != null) {
                writer.close();
                writer = null;
            }
            if (out != null) {
                out.flush();
                outputBytes = out.getPos();
                committer = ((TwoPhaseOutputStream) out).closeForCommit();
                out = null;
            }
        } catch (IOException e) {
            LOG.warn("Exception occurs when closing file {}. Cleaning up.", path, e);
            abort();
            throw e;
        } finally {
            closed = true;
        }
    }
}
