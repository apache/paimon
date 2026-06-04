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
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.hive.RowDataContainer;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.types.DataField;
import org.apache.paimon.utils.PartitionPathUtils;
import org.apache.paimon.utils.TypeUtils;

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
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
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
        FileStoreTable table = forWriteOnly(createFileStoreTable(jobConf));
        PaimonRecordWriter inner = writer(jobConf, table);

        GenericRow staticPartitionRow = buildStaticPartitionRow(path, table.schema());
        return staticPartitionRow == null
                ? inner
                : new PartitionedRecordWriter(
                        inner, staticPartitionRow, table.schema().fields().size());
    }

    private static PaimonRecordWriter writer(JobConf jobConf) {
        return writer(jobConf, forWriteOnly(createFileStoreTable(jobConf)));
    }

    private static PaimonRecordWriter writer(JobConf jobConf, FileStoreTable table) {
        TaskAttemptID taskAttemptID = TezUtil.taskAttemptWrapper(jobConf);
        BatchWriteBuilder batchWriteBuilder = table.newBatchWriteBuilder();
        BatchTableWrite batchTableWrite = batchWriteBuilder.newWrite();
        return new PaimonRecordWriter(batchTableWrite, taskAttemptID, table.name());
    }

    private static FileStoreTable forWriteOnly(FileStoreTable table) {
        Map<String, String> newOptions =
                Collections.singletonMap(CoreOptions.WRITE_ONLY.key(), Boolean.TRUE.toString());
        return table.copy(newOptions);
    }

    static GenericRow buildStaticPartitionRow(Path path, TableSchema schema) {
        List<String> partitionKeys = schema.partitionKeys();
        if (partitionKeys.isEmpty()) {
            return null;
        }

        LinkedHashMap<String, String> spec =
                PartitionPathUtils.extractPartitionSpecFromPath(
                        new org.apache.paimon.fs.Path(path.toString()));
        if (spec.isEmpty()) {
            return null;
        }

        assertPartitionKeysAreSchemaTail(schema.fields(), partitionKeys);

        GenericRow row = new GenericRow(partitionKeys.size());
        List<DataField> fields = schema.fields();
        for (int i = 0; i < partitionKeys.size(); i++) {
            String key = partitionKeys.get(i);
            String raw = lookupCaseInsensitive(spec, key);
            if (raw == null) {
                throw new IllegalArgumentException(
                        "Mixed static and dynamic partition writes are not supported for managed "
                                + "Paimon Hive tables. Partition key '"
                                + key
                                + "' has no value in the static partition path "
                                + path
                                + ". Provide values for every partition key in the INSERT.");
            }
            DataField field = findField(fields, key);
            row.setField(i, TypeUtils.castFromString(raw, field.type()));
        }
        return row;
    }

    private static void assertPartitionKeysAreSchemaTail(
            List<DataField> fields, List<String> partitionKeys) {
        int n = partitionKeys.size();
        int start = fields.size() - n;
        if (start < 0) {
            throw new IllegalArgumentException(
                    "Table schema has "
                            + fields.size()
                            + " columns but the schema declares "
                            + n
                            + " partition keys. Static partition write requires partition keys "
                            + "to be a trailing slice of the schema.");
        }
        for (int i = 0; i < n; i++) {
            String expected = partitionKeys.get(i);
            String actual = fields.get(start + i).name();
            if (!actual.equalsIgnoreCase(expected)) {
                List<String> schemaNames = new ArrayList<>(fields.size());
                for (DataField f : fields) {
                    schemaNames.add(f.name());
                }
                throw new IllegalArgumentException(
                        "Static partition write requires partition keys to be the trailing "
                                + "columns of the schema in declared order. Expected column at "
                                + "position "
                                + (start + i)
                                + " to be '"
                                + expected
                                + "', found '"
                                + actual
                                + "'. Schema: "
                                + schemaNames
                                + ", partition keys: "
                                + partitionKeys
                                + ".");
            }
        }
    }

    private static String lookupCaseInsensitive(Map<String, String> spec, String key) {
        for (Map.Entry<String, String> e : spec.entrySet()) {
            if (e.getKey().equalsIgnoreCase(key)) {
                return e.getValue();
            }
        }
        return null;
    }

    private static DataField findField(List<DataField> fields, String name) {
        for (DataField f : fields) {
            if (f.name().equalsIgnoreCase(name)) {
                return f;
            }
        }
        throw new IllegalStateException("Partition column not found in schema: " + name);
    }
}
