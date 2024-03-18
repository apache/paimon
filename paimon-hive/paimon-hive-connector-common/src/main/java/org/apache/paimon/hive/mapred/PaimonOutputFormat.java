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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.hive.RowDataContainer;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.util.Progressable;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

import static org.apache.paimon.hive.utils.HiveUtils.createFileStoreTable;

/** {@link OutputFormat} for table split. */
public class PaimonOutputFormat
        implements OutputFormat<NullWritable, RowDataContainer>,
                HiveOutputFormat<NullWritable, RowDataContainer> {

    private static final String TASK_ATTEMPT_ID_KEY = "mapreduce.task.attempt.id";

    @Override
    public RecordWriter<NullWritable, RowDataContainer> getRecordWriter(
            FileSystem fileSystem, JobConf jobConf, String s, Progressable progressable)
            throws IOException {
        return writer(jobConf);
    }

    @Override
    public void checkOutputSpecs(FileSystem fileSystem, JobConf jobConf) throws IOException {}

    @Override
    public FileSinkOperator.RecordWriter getHiveRecordWriter(
            JobConf jobConf,
            Path path,
            Class<? extends Writable> aClass,
            boolean b,
            Properties properties,
            Progressable progressable)
            throws IOException {
        return writer(jobConf);
    }

    private static PaimonRecordWriter writer(JobConf jobConf) {
        TaskAttemptID taskAttemptID = TezUtil.taskAttemptWrapper(jobConf);

        FileStoreTable table = createFileStoreTable(jobConf);
        // force write-only = true
        Map<String, String> newOptions =
                Collections.singletonMap(CoreOptions.WRITE_ONLY.key(), Boolean.TRUE.toString());
        FileStoreTable copy = table.copy(newOptions);
        BatchWriteBuilder batchWriteBuilder = copy.newBatchWriteBuilder();
        BatchTableWrite batchTableWrite = batchWriteBuilder.newWrite();

        return new PaimonRecordWriter(batchTableWrite, taskAttemptID, copy.name());
    }
}
