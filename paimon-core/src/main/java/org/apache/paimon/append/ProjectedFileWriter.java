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

package org.apache.paimon.append;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.io.BundleRecords;
import org.apache.paimon.io.FileWriter;
import org.apache.paimon.io.ProjectableBundleRecords;
import org.apache.paimon.io.ReplayableBundleRecords;
import org.apache.paimon.utils.ProjectedRow;

import java.io.IOException;
import java.util.Arrays;

/**
 * A delegating {@link FileWriter} which applies a field projection to each incoming {@link
 * InternalRow} before forwarding it to the underlying writer.
 *
 * <p>This is useful when the physical file schema is a subset of the logical write schema. The
 * projection is evaluated via {@link ProjectedRow} to avoid object allocations.
 */
public class ProjectedFileWriter<T extends FileWriter<InternalRow, R>, R>
        implements FileWriter<InternalRow, R> {

    private final T writer;
    private final int[] projection;
    private final ProjectedRow projectedRow;

    public ProjectedFileWriter(T writer, int[] projection) {
        this.writer = writer;
        this.projection = Arrays.copyOf(projection, projection.length);
        this.projectedRow = ProjectedRow.from(this.projection);
    }

    @Override
    public void write(InternalRow record) throws IOException {
        projectedRow.replaceRow(record);
        writer.write(projectedRow);
    }

    public void writeBundle(BundleRecords bundle) throws IOException {
        if (writer instanceof BundlePassThroughWriter
                && ((BundlePassThroughWriter) writer).supportsBundlePassThrough()
                && bundle instanceof ReplayableBundleRecords) {
            ReplayableBundleRecords projectedBundle =
                    bundle instanceof ProjectableBundleRecords
                            ? ((ProjectableBundleRecords) bundle).project(projection)
                            : new ProjectedBundleRecords(
                                    (ReplayableBundleRecords) bundle, projection);
            ((BundlePassThroughWriter) writer).writeReplayableBundle(projectedBundle);
            return;
        }

        for (InternalRow row : bundle) {
            write(row);
        }
    }

    @Override
    public long recordCount() {
        return writer.recordCount();
    }

    @Override
    public void abort() {
        writer.abort();
    }

    @Override
    public R result() throws IOException {
        return writer.result();
    }

    @Override
    public void close() throws IOException {
        writer.close();
    }

    public T writer() {
        return writer;
    }
}
