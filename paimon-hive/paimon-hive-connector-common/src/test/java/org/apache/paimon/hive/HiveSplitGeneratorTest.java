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

package org.apache.paimon.hive;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryRowWriter;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.hive.utils.HiveSplitGenerator;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.AppendOnlyFileStoreTable;
import org.apache.paimon.table.CatalogEnvironment;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.VarCharType;
import org.apache.paimon.utils.TraceableFileIO;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.mapred.JobConf;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/** IT cases for {@link HiveSplitGenerator}. */
public class HiveSplitGeneratorTest {

    private static final List<DataField> SCHEMA_FIELDS =
            Arrays.asList(
                    new DataField(0, "id", new IntType()),
                    new DataField(1, "col", VarCharType.STRING_TYPE),
                    new DataField(2, "pt", VarCharType.STRING_TYPE));

    private static final TableSchema TABLE_SCHEMA =
            new TableSchema(
                    0,
                    SCHEMA_FIELDS,
                    2,
                    Collections.emptyList(),
                    Collections.singletonList("id"),
                    Collections.emptyMap(),
                    "");

    @TempDir java.nio.file.Path tempDir;

    protected Path tablePath;
    protected FileIO fileIO;
    protected String commitUser;
    protected final Options tableConfig = new Options();

    @BeforeEach
    public void before() throws Exception {
        tablePath = new Path(TraceableFileIO.SCHEME + "://" + tempDir.toString());
        fileIO = FileIO.get(tablePath, CatalogContext.create(new Options()));
        commitUser = UUID.randomUUID().toString();
        tableConfig.set(CoreOptions.PATH, tablePath.toString());
        tableConfig.set(CoreOptions.BUCKET, 1);
    }

    @Test
    public void testPackSplitsForNonBucketTable() throws Exception {
        JobConf jobConf = new JobConf();
        jobConf.set(HiveConf.ConfVars.MAPREDMAXSPLITSIZE.varname, "268435456"); // 256MB
        jobConf.set(HiveConf.ConfVars.MAPREDMINSPLITSIZE.varname, "268435456"); // 256MB

        FileStoreTable table = createFileStoreTable(TABLE_SCHEMA);

        List<DataSplit> dataSplits = new ArrayList<>();
        dataSplits.add(newDataSplit(4, 0, 12582912L)); // 12MB
        dataSplits.add(newDataSplit(2, 0, 12582912L));
        dataSplits.add(newDataSplit(3, 0, 12582912L));
        List<DataSplit> packed = HiveSplitGenerator.packSplits(table, jobConf, dataSplits, 0);

        assertThat(packed.size()).isEqualTo(1);
        int totalFiles = 0;
        for (DataSplit dataSplit : packed) {
            totalFiles += dataSplit.dataFiles().size();
        }
        assertThat(totalFiles).isEqualTo(9);
    }

    @Test
    public void testPackSplitsForBucketTable() throws Exception {
        JobConf jobConf = new JobConf();
        jobConf.set(HiveConf.ConfVars.MAPREDMAXSPLITSIZE.varname, "268435456");
        jobConf.set(HiveConf.ConfVars.MAPREDMINSPLITSIZE.varname, "268435456");

        FileStoreTable table = createFileStoreTable(TABLE_SCHEMA);

        List<DataSplit> dataSplits = new ArrayList<>();
        dataSplits.add(newDataSplit(4, 0, 12582912L));
        dataSplits.add(newDataSplit(2, 1, 12582912L));
        dataSplits.add(newDataSplit(1, 1, 12582912L));
        dataSplits.add(newDataSplit(3, 2, 12582912L));
        List<DataSplit> packed = HiveSplitGenerator.packSplits(table, jobConf, dataSplits, 0);

        assertThat(packed.size()).isEqualTo(3);
        int totalFiles = 0;
        for (DataSplit dataSplit : packed) {
            totalFiles += dataSplit.dataFiles().size();
        }
        assertThat(totalFiles).isEqualTo(10);
    }

    private FileStoreTable createFileStoreTable(TableSchema tableSchema) throws Exception {
        SchemaManager schemaManager = new SchemaManager(fileIO, tablePath);
        schemaManager.commit(tableSchema);

        return new AppendOnlyFileStoreTable(
                fileIO, tablePath, tableSchema, CatalogEnvironment.empty()) {

            @Override
            public SchemaManager schemaManager() {
                return schemaManager;
            }
        };
    }

    private DataSplit newDataSplit(int numFiles, int bucket, long fileSize) {
        List<DataFileMeta> dataFiles = new ArrayList<>();

        for (int i = 0; i < numFiles; i++) {
            DataFileMeta fileMeta =
                    DataFileMeta.create(
                            "test-file-" + i + ".parquet",
                            fileSize,
                            100L,
                            createBinaryRow(1),
                            createBinaryRow(100),
                            null,
                            null,
                            0L,
                            0L,
                            0,
                            0,
                            Collections.emptyList(),
                            null,
                            null,
                            FileSource.APPEND,
                            null,
                            null,
                            null,
                            null);
            dataFiles.add(fileMeta);
        }

        DataSplit.Builder builder = DataSplit.builder();
        builder.withSnapshot(1)
                .withPartition(BinaryRow.EMPTY_ROW)
                .withBucket(bucket)
                .withBucketPath("bucket-" + bucket + "/")
                .rawConvertible(true)
                .withDataFiles(dataFiles);
        return builder.build();
    }

    private BinaryRow createBinaryRow(int value) {
        BinaryRow row = new BinaryRow(1);
        BinaryRowWriter writer = new BinaryRowWriter(row);
        writer.writeInt(0, value);
        writer.complete();
        return row;
    }
}
