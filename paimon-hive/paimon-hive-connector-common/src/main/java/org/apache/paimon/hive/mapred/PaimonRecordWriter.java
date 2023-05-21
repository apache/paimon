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

package org.apache.paimon.hive.mapred;

import org.apache.paimon.hive.RowDataContainer;
import org.apache.paimon.table.sink.BatchTableWrite;

import org.apache.paimon.shade.guava30.com.google.common.collect.Maps;

import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

class PaimonRecordWriter
        implements FileSinkOperator.RecordWriter,
                org.apache.hadoop.mapred.RecordWriter<NullWritable, RowDataContainer> {
    private static final Logger LOG = LoggerFactory.getLogger(PaimonRecordWriter.class);

    // Each task generates a PaimonRecordWriter use Map<TaskAttemptID, Map<String,
    // PaimonRecordWriter>>
    // is used to support multiple table writing for one task in the future
    private static final Map<TaskAttemptID, Map<String, PaimonRecordWriter>> writers =
            Maps.newConcurrentMap();

    private BatchTableWrite batchTableWrite;

    public PaimonRecordWriter(
            BatchTableWrite batchTableWrite, TaskAttemptID taskAttemptID, String tableName) {
        this.batchTableWrite = batchTableWrite;
        writers.putIfAbsent(taskAttemptID, Maps.newConcurrentMap());
        writers.get(taskAttemptID).put(tableName, this);
    }

    static Map<String, PaimonRecordWriter> removeWriters(TaskAttemptID taskAttemptID) {
        return writers.remove(taskAttemptID);
    }

    static Map<String, PaimonRecordWriter> getWriters(TaskAttemptID taskAttemptID) {
        return writers.get(taskAttemptID);
    }

    public void write(Writable row) throws IOException {
        try {
            batchTableWrite.write(((RowDataContainer) row).get());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void write(NullWritable key, RowDataContainer value) throws IOException {
        write(value);
    }

    @Override
    public void close(boolean abort) {
        if (abort) {
            try {
                batchTableWrite.close();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public void close(Reporter reporter) {
        close(false);
    }

    public BatchTableWrite batchTableWrite() {
        return batchTableWrite;
    }
}
