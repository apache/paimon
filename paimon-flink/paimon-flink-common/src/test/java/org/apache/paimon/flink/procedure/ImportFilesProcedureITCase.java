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

package org.apache.paimon.flink.procedure;

import org.apache.paimon.data.GenericRow;
import org.apache.paimon.flink.CatalogITCaseBase;
import org.apache.paimon.fs.Path;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.Split;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** SQL integration tests for {@link ImportFilesProcedure}. */
class ImportFilesProcedureITCase extends CatalogITCaseBase {

    @Test
    void testImportIntoPartition() throws Exception {
        Path location = prepareFiles();
        sql(
                "CREATE TABLE T (id INT, dt STRING, `hour` INT) PARTITIONED BY (dt, `hour`) "
                        + "WITH ('bucket'='-1', 'file.format'='parquet')");

        assertThat(
                        sql("CALL sys.import_files(`table` => 'default.T', location => '"
                                        + location
                                        + "', `partition` => 'hour=12,dt=p1')")
                                .get(0)
                                .getField(0))
                .isEqualTo(1L);
        FileStoreTable table = paimonTable("T");
        DataSplit split = (DataSplit) table.newScan().plan().splits().get(0);
        DataFileMeta file = split.dataFiles().get(0);
        assertThat(file.externalPath()).isPresent();
        assertThat(table.fileIO().exists(new Path(file.externalPath().get()))).isTrue();
        assertThat(file.rowCount()).isEqualTo(2L);
        assertThat(sql("SELECT * FROM T ORDER BY id").toString())
                .isEqualTo("[+I[1, p1, 12], +I[2, p1, 12]]");
        assertThat(sql("SELECT COUNT(*) FROM T").get(0).getField(0)).isEqualTo(2L);
        sql("ALTER TABLE T ADD added INT");
        assertThat(sql("SELECT COUNT(*) FROM T WHERE added IS NULL").get(0).getField(0))
                .isEqualTo(2L);
    }

    @Test
    void testImportIntoUnpartitionedTable() throws Exception {
        Path location = prepareFiles();
        sql("CREATE TABLE T (id INT) WITH ('bucket'='-1', 'file.format'='parquet')");
        assertThat(sql("CALL sys.import_files('default.T', '" + location + "')").get(0).getField(0))
                .isEqualTo(1L);
        assertThat(sql("SELECT COUNT(*) FROM T").get(0).getField(0)).isEqualTo(2L);
    }

    private Path prepareFiles() throws Exception {
        sql("CREATE TABLE S (id INT) WITH ('bucket'='-1', 'file.format'='parquet')");
        FileStoreTable source = paimonTable("S");
        BatchWriteBuilder builder = source.newBatchWriteBuilder();
        try (BatchTableWrite write = builder.newWrite();
                BatchTableCommit commit = builder.newCommit()) {
            write.write(GenericRow.of(1));
            write.write(GenericRow.of(2));
            commit.commit(write.prepareCommit());
        }
        Path location = new Path(path, "external");
        source.fileIO().mkdirs(location);
        for (Split split : source.newScan().plan().splits()) {
            DataSplit dataSplit = (DataSplit) split;
            for (DataFileMeta file : dataSplit.dataFiles()) {
                Path sourcePath =
                        source.store()
                                .pathFactory()
                                .createDataFilePathFactory(
                                        dataSplit.partition(), dataSplit.bucket())
                                .toPath(file);
                source.fileIO().copyFile(sourcePath, new Path(location, file.fileName()), true);
            }
        }
        return location;
    }
}
