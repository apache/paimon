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

package org.apache.paimon.spark;

import org.apache.paimon.disk.IOManager;
import org.apache.paimon.spark.util.SparkRowUtils;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageSerializer;
import org.apache.paimon.types.RowType;

import org.apache.spark.sql.Row;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/** An util class for {@link BatchTableWrite}. */
public class SparkTableWrite implements AutoCloseable {

    private final BatchTableWrite write;
    private final IOManager ioManager;

    private final RowType rowType;
    private final int rowKindColIdx;

    public SparkTableWrite(BatchWriteBuilder writeBuilder, RowType rowType, int rowKindColIdx) {
        this.write = writeBuilder.newWrite();
        this.rowType = rowType;
        this.rowKindColIdx = rowKindColIdx;
        this.ioManager = SparkUtils.createIOManager();
        write.withIOManager(ioManager);
    }

    public void write(Row row) throws Exception {
        write.write(toPaimonRow(row));
    }

    public void write(Row row, int bucket) throws Exception {
        write.write(toPaimonRow(row), bucket);
    }

    public Iterator<byte[]> finish() throws Exception {
        CommitMessageSerializer serializer = new CommitMessageSerializer();
        List<byte[]> commitMessages = new ArrayList<>();
        for (CommitMessage message : write.prepareCommit()) {
            commitMessages.add(serializer.serialize(message));
        }
        return commitMessages.iterator();
    }

    @Override
    public void close() throws Exception {
        write.close();
        ioManager.close();
    }

    private SparkRow toPaimonRow(Row row) {
        return new SparkRow(rowType, row, SparkRowUtils.getRowKind(row, rowKindColIdx));
    }
}
