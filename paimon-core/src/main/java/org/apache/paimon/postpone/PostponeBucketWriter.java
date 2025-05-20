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

package org.apache.paimon.postpone;

import org.apache.paimon.KeyValue;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.io.KeyValueFileWriterFactory;
import org.apache.paimon.io.RollingFileWriter;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.utils.CommitIncrement;
import org.apache.paimon.utils.RecordWriter;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/** {@link RecordWriter} for {@code bucket = -2} tables. */
public class PostponeBucketWriter implements RecordWriter<KeyValue> {

    private final KeyValueFileWriterFactory writerFactory;
    private final List<DataFileMeta> files;

    private RollingFileWriter<KeyValue, DataFileMeta> writer;

    public PostponeBucketWriter(
            KeyValueFileWriterFactory writerFactory, @Nullable CommitIncrement restoreIncrement) {
        this.writerFactory = writerFactory;
        this.files = new ArrayList<>();
        if (restoreIncrement != null) {
            files.addAll(restoreIncrement.newFilesIncrement().newFiles());
        }

        this.writer = null;
    }

    @Override
    public void write(KeyValue record) throws Exception {
        if (writer == null) {
            writer = writerFactory.createRollingMergeTreeFileWriter(0, FileSource.APPEND);
        }
        writer.write(record);
    }

    @Override
    public void compact(boolean fullCompaction) throws Exception {}

    @Override
    public void addNewFiles(List<DataFileMeta> files) {
        this.files.addAll(files);
    }

    @Override
    public Collection<DataFileMeta> dataFiles() {
        return new ArrayList<>(files);
    }

    @Override
    public long maxSequenceNumber() {
        // see comments in the constructor of PostponeBucketFileStoreWrite
        return 0;
    }

    @Override
    public CommitIncrement prepareCommit(boolean waitCompaction) throws Exception {
        if (writer != null) {
            writer.close();
            files.addAll(writer.result());
            writer = null;
        }

        List<DataFileMeta> result = new ArrayList<>(files);
        files.clear();
        return new CommitIncrement(
                new DataIncrement(result, Collections.emptyList(), Collections.emptyList()),
                CompactIncrement.emptyIncrement(),
                null);
    }

    @Override
    public boolean compactNotCompleted() {
        return false;
    }

    @Override
    public void sync() throws Exception {}

    @Override
    public void withInsertOnly(boolean insertOnly) {}

    @Override
    public void close() throws Exception {
        if (writer != null) {
            writer.abort();
            writer = null;
        }
    }
}
