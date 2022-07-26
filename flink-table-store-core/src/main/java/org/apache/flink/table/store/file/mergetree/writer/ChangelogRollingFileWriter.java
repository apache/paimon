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

package org.apache.flink.table.store.file.mergetree.writer;

import org.apache.flink.table.store.file.KeyValue;
import org.apache.flink.table.store.file.data.DataFileMeta;
import org.apache.flink.table.store.file.mergetree.compact.ChangelogConsumer;
import org.apache.flink.table.store.file.writer.RollingFileWriter;

import java.io.IOException;
import java.util.Iterator;
import java.util.function.Supplier;

/** */
public class ChangelogRollingFileWriter extends RollingFileWriter<KeyValue, DataFileMeta>
        implements ChangelogConsumer {

    public ChangelogRollingFileWriter(
            Supplier<ChangelogKvFileWriter> writerFactory, long targetFileSize) {
        super(writerFactory, targetFileSize);
    }

    @Override
    public void consume(KeyValue keyValue) {
        if (currentWriter == null) {
            // TODO data file writer should support empty file
            openCurrentWriter();
        }
        ((ChangelogKvFileWriter) currentWriter).consume(keyValue);
    }

    @Override
    public void write(Iterator<KeyValue> records) throws IOException {
        while (records.hasNext()) {
            write(records.next());
        }
    }
}
