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

package org.apache.paimon.operation;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.SchemaUtils;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.sink.TableCommitImpl;
import org.apache.paimon.table.sink.TableWriteImpl;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Utils for {@link OrphanFilesClean}. */
public class OrphanFilesCleanTest {

    @Rule public TemporaryFolder tempDir = new TemporaryFolder();

    @Test
    public void testOlderThanMillis() {
        // normal olderThan
        OrphanFilesClean.olderThanMillis(null);
        OrphanFilesClean.olderThanMillis("2024-12-21 23:00:00");

        // non normal olderThan
        assertThatThrownBy(() -> OrphanFilesClean.olderThanMillis("3024-12-21 23:00:00"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(
                        "The arg olderThan must be less than now, because dataFiles that are currently being written and not referenced by snapshots will be mistakenly cleaned up.");
    }

    @Test
    public void testListPaimonFileDirsWithEmptyPartition() throws Exception {
        Path tablePath = new Path(tempDir.newFolder().toURI());
        FileIO fileIO = LocalFileIO.create();
        RowType rowType =
                RowType.of(
                        new DataType[] {
                            DataTypes.INT(), DataTypes.INT(), DataTypes.STRING(), DataTypes.STRING()
                        },
                        new String[] {"pk", "part1", "part2", "value"});
        FileStoreTable table = createFileStoreTable(fileIO, tablePath, rowType, new Options());
        String commitUser = UUID.randomUUID().toString();
        try (TableWriteImpl<?> write = table.newWrite(commitUser);
                TableCommitImpl commit = table.newCommit(commitUser)) {
            write.write(
                    GenericRow.ofKind(
                            RowKind.INSERT,
                            1,
                            0,
                            BinaryString.fromString("a"),
                            BinaryString.fromString("v1")));
            commit.commit(0, write.prepareCommit(true, 0));
        }

        Path emptyPartitionPath = new Path(tablePath, "part1=0/part2=b");
        fileIO.mkdirs(emptyPartitionPath);
        Path emptyNonLeafPartitionPath = new Path(tablePath, "part1=1");
        fileIO.mkdirs(emptyNonLeafPartitionPath);

        java.lang.reflect.Method method =
                LocalOrphanFilesClean.class.getSuperclass().getDeclaredMethod("listPaimonFileDirs");
        method.setAccessible(true);
        @SuppressWarnings("unchecked")
        List<Path> dirs = (List<Path>) method.invoke(new LocalOrphanFilesClean(table));

        assertThat(dirs)
                .as(
                        "Empty partition (no bucket subdirs) is listed by listPaimonFileDirs for empty-dir cleanup")
                .contains(emptyPartitionPath);
        assertThat(dirs)
                .as(
                        "Empty non-leaf partition dir (e.g. part1=1 with no part2) is listed by listPaimonFileDirs")
                .contains(emptyNonLeafPartitionPath);
    }

    @Test
    public void testDeleteNonEmptyDir() throws Exception {
        Path dir = new Path(tempDir.newFolder().toURI().toString(), "part1=0");
        FileIO fileIO = LocalFileIO.create();
        fileIO.mkdirs(dir);
        Path file = new Path(dir, "data.dat");
        fileIO.writeFile(file, "x", true);

        assertThat(fileIO.exists(dir)).isTrue();
        assertThatThrownBy(() -> fileIO.delete(dir, false))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("not empty");
        assertThat(fileIO.exists(dir)).isTrue();
    }

    private FileStoreTable createFileStoreTable(
            FileIO fileIO, Path tablePath, RowType rowType, Options conf) throws Exception {
        conf.set(CoreOptions.PATH, tablePath.toString());
        conf.set(CoreOptions.BUCKET, 2);
        TableSchema tableSchema =
                SchemaUtils.forceCommit(
                        new SchemaManager(fileIO, tablePath),
                        new Schema(
                                rowType.getFields(),
                                Arrays.asList("part1", "part2"),
                                Arrays.asList("pk", "part1", "part2"),
                                conf.toMap(),
                                ""));
        return FileStoreTableFactory.create(fileIO, tablePath, tableSchema);
    }
}
