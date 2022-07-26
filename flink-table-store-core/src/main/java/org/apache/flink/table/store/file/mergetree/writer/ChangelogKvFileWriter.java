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

package org.apache.flink.table.store.file.mergetree.writer;

import org.apache.flink.core.fs.Path;
import org.apache.flink.table.store.file.KeyValue;
import org.apache.flink.table.store.file.data.DataFileMeta;
import org.apache.flink.table.store.file.mergetree.compact.ChangelogConsumer;
import org.apache.flink.table.store.file.utils.FileUtils;
import org.apache.flink.table.store.file.writer.FileWriter;
import org.apache.flink.table.store.file.writer.Metric;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;

/** */
public class ChangelogKvFileWriter
        implements FileWriter<KeyValue, DataFileMeta>, ChangelogConsumer {

    private final KvFileWriter kvFileWriter;
    private final FileWriter<KeyValue, Metric> changelogFileWriter;
    private final Path changelogFile;

    private boolean closed = false;

    public ChangelogKvFileWriter(
            KvFileWriter kvFileWriter,
            FileWriter.Factory<KeyValue, Metric> writerFactory,
            Path changelogFile) {
        this.kvFileWriter = kvFileWriter;
        this.changelogFile = changelogFile;
        try {
            this.changelogFileWriter = writerFactory.create(changelogFile);
        } catch (IOException e) {
            FileUtils.deleteOrWarn(changelogFile);
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void write(KeyValue record) throws IOException {
        kvFileWriter.write(record);
    }

    @Override
    public long recordCount() {
        return kvFileWriter.recordCount();
    }

    @Override
    public long length() throws IOException {
        return kvFileWriter.length();
    }

    @Override
    public DataFileMeta result() throws IOException {
        DataFileMeta result = kvFileWriter.result();
        Preconditions.checkArgument(FileUtils.exists(changelogFile));
        List<String> extraFiles = new ArrayList<>(result.extraFiles());
        extraFiles.add(changelogFile.getName());
        result = result.copy(extraFiles);
        return result;
    }

    @Override
    public void consume(KeyValue keyValue) {
        try {
            changelogFileWriter.write(keyValue);

            // update metric for kv writer, this is to avoid the problem of no min/max key when kv
            // writer has no data.
            kvFileWriter.updateMetric(keyValue);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void abort() {
        IOUtils.closeQuietly(this);
        kvFileWriter.abort();
        FileUtils.deleteOrWarn(changelogFile);
    }

    @Override
    public void close() throws IOException {
        if (!closed) {
            kvFileWriter.close();
            IOUtils.closeQuietly(changelogFileWriter);
            closed = true;
        }
    }
}
