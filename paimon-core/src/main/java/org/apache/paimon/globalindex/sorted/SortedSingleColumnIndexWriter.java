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

package org.apache.paimon.globalindex.sorted;

import org.apache.paimon.globalindex.GlobalIndexSingleColumnWriter;
import org.apache.paimon.globalindex.ResultEntry;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.paimon.utils.Preconditions.checkState;

/** Rotates single-column index writers after a configured number of logical records. */
public final class SortedSingleColumnIndexWriter implements Closeable {

    private final long recordsPerRange;
    private final Factory factory;
    private final List<List<ResultEntry>> resultGroups = new ArrayList<>();

    @Nullable private GlobalIndexSingleColumnWriter currentWriter;
    private long currentRecordCount;
    private boolean finished;

    public SortedSingleColumnIndexWriter(long recordsPerRange, Factory factory) {
        checkArgument(recordsPerRange > 0, "Records per range must be positive.");
        this.recordsPerRange = recordsPerRange;
        this.factory = factory;
    }

    public void write(@Nullable Object value, long localRowId) throws IOException {
        checkState(!finished, "Cannot write after the sorted index writer is finished.");
        if (currentWriter != null && currentRecordCount >= recordsPerRange) {
            finishCurrentWriter();
        }
        if (currentWriter == null) {
            currentWriter = factory.create();
        }
        currentWriter.write(value, localRowId);
        currentRecordCount++;
    }

    public List<List<ResultEntry>> finish() {
        checkState(!finished, "Sorted index writer is already finished.");
        if (currentWriter != null) {
            finishCurrentWriter();
        }
        finished = true;
        List<List<ResultEntry>> copy = new ArrayList<>(resultGroups.size());
        for (List<ResultEntry> group : resultGroups) {
            copy.add(Collections.unmodifiableList(new ArrayList<>(group)));
        }
        return Collections.unmodifiableList(copy);
    }

    @Override
    public void close() throws IOException {
        finished = true;
        currentRecordCount = 0;
        GlobalIndexSingleColumnWriter writer = currentWriter;
        currentWriter = null;
        if (writer instanceof AutoCloseable) {
            try {
                ((AutoCloseable) writer).close();
            } catch (IOException e) {
                throw e;
            } catch (Exception e) {
                throw new IOException("Failed to close the active sorted index writer.", e);
            }
        }
    }

    private void finishCurrentWriter() {
        resultGroups.add(currentWriter.finish());
        currentWriter = null;
        currentRecordCount = 0;
    }

    /** Creates one underlying algorithm writer. */
    @FunctionalInterface
    public interface Factory {
        GlobalIndexSingleColumnWriter create() throws IOException;
    }
}
