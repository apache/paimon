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

package org.apache.paimon.flink.action;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.FileSystemCatalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.fs.FileStatus;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.TableCommitImpl;
import org.apache.paimon.table.sink.TableWriteImpl;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/** IT cases for {@link RemoveUnexistingFilesAction}. */
public class RemoveUnexistingFilesActionITCase extends ActionITCaseBase {

    @ParameterizedTest
    @ValueSource(ints = {-1, 3})
    public void testAction(int bucket) throws Exception {
        int numPartitions = 2;
        int numFiles = 10;
        int[] numDeletes = new int[numPartitions];
        prepareRandomlyDeletedTable(warehouse, bucket, numFiles, numDeletes);

        RemoveUnexistingFilesAction action =
                createAction(
                                RemoveUnexistingFilesAction.class,
                                "remove_unexisting_files",
                                "--warehouse",
                                warehouse,
                                "--database",
                                "mydb",
                                "--table",
                                "t",
                                "--dry_run",
                                "true")
                        .withParallelism(2);
        int[] actual = new int[numPartitions];
        Pattern pattern = Pattern.compile("pt=(\\d+?)/");
        try (CloseableIterator<String> it = action.buildDataStream().executeAndCollect()) {
            while (it.hasNext()) {
                String path = it.next();
                Matcher matcher = pattern.matcher(path);
                if (matcher.find()) {
                    actual[Integer.parseInt(matcher.group(1))]++;
                }
            }
        }
        assertThat(actual).isEqualTo(numDeletes);

        action =
                createAction(
                                RemoveUnexistingFilesAction.class,
                                "remove_unexisting_files",
                                "--warehouse",
                                warehouse,
                                "--database",
                                "mydb",
                                "--table",
                                "t")
                        .withParallelism(2);
        action.run();
        TableEnvironment tEnv = tableEnvironmentBuilder().batchMode().build();
        tEnv.executeSql(
                "CREATE CATALOG mycat WITH (\n"
                        + "  'type' = 'paimon',\n"
                        + "  'warehouse' = '"
                        + warehouse
                        + "'\n"
                        + ")");
        tEnv.executeSql("USE CATALOG mycat");
        try (CloseableIterator<Row> it =
                tEnv.executeSql("SELECT pt, CAST(COUNT(*) AS INT) FROM mydb.t GROUP BY pt")
                        .collect()) {
            while (it.hasNext()) {
                Row row = it.next();
                assertThat(row.getField(1)).isEqualTo(numFiles - numDeletes[(int) row.getField(0)]);
            }
        }
    }

    public static FileStoreTable prepareRandomlyDeletedTable(
            String warehouse, int bucket, int numFiles, int[] numDeletes) throws Exception {
        RowType rowType =
                RowType.of(
                        new DataType[] {DataTypes.INT(), DataTypes.INT(), DataTypes.BIGINT()},
                        new String[] {"pt", "id", "v"});
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.BUCKET.key(), String.valueOf(bucket));
        options.put(CoreOptions.WRITE_ONLY.key(), "true");
        if (bucket > 0) {
            options.put(CoreOptions.BUCKET_KEY.key(), "id");
        }
        FileStoreTable table =
                createPaimonTable(warehouse, rowType, Collections.singletonList("pt"), options);

        String commitUser = UUID.randomUUID().toString();
        TableWriteImpl<?> write = table.newWrite(commitUser);
        TableCommitImpl commit = table.newCommit(commitUser);

        ThreadLocalRandom random = ThreadLocalRandom.current();
        int numPartitions = numDeletes.length;
        for (int i = 0; i < numPartitions; i++) {
            numDeletes[i] = random.nextInt(0, numFiles + 1);
        }

        int identifier = 0;
        for (int i = 0; i < numPartitions; i++) {
            for (int j = 0; j < numFiles; j++) {
                write.write(GenericRow.of(i, random.nextInt(), random.nextLong()));
                identifier++;
                commit.commit(identifier, write.prepareCommit(false, identifier));
            }
        }

        write.close();
        commit.close();

        for (int i = 0; i < numPartitions; i++) {
            LocalFileIO fileIO = LocalFileIO.create();
            List<Path> paths = new ArrayList<>();
            for (int j = 0; j < Math.max(1, bucket); j++) {
                Path path = new Path(table.location(), "pt=" + i + "/bucket-" + j);
                paths.addAll(
                        Arrays.stream(fileIO.listStatus(path))
                                .map(FileStatus::getPath)
                                .collect(Collectors.toList()));
            }
            Collections.shuffle(paths);
            for (int j = 0; j < numDeletes[i]; j++) {
                fileIO.deleteQuietly(paths.get(j));
            }
        }

        return table;
    }

    private static FileStoreTable createPaimonTable(
            String warehouse,
            RowType rowType,
            List<String> partitionKeys,
            Map<String, String> customOptions)
            throws Exception {
        LocalFileIO fileIO = LocalFileIO.create();
        Path path = new Path(warehouse);

        Schema schema =
                new Schema(
                        rowType.getFields(),
                        partitionKeys,
                        Collections.emptyList(),
                        customOptions,
                        "");

        try (FileSystemCatalog paimonCatalog = new FileSystemCatalog(fileIO, path)) {
            paimonCatalog.createDatabase("mydb", false);
            Identifier paimonIdentifier = Identifier.create("mydb", "t");
            paimonCatalog.createTable(paimonIdentifier, schema, false);
            return (FileStoreTable) paimonCatalog.getTable(paimonIdentifier);
        }
    }
}
