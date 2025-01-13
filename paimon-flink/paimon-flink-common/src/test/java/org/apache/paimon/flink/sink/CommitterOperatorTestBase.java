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

package org.apache.paimon.flink.sink;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.options.Options;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.reader.RecordReaderIterator;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.source.TableRead;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.CloseableIterator;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;

/** Base test class for {@link CommitterOperatorTest}. */
public abstract class CommitterOperatorTestBase {

    private static final RowType ROW_TYPE =
            RowType.of(
                    new DataType[] {DataTypes.INT(), DataTypes.BIGINT()}, new String[] {"a", "b"});

    @TempDir public java.nio.file.Path tempDir;
    protected Path tablePath;

    @BeforeEach
    public void before() {
        tablePath = new Path(tempDir.toString());
    }

    protected void assertResults(FileStoreTable table, String... expected) {
        TableRead read = table.newReadBuilder().newRead();
        List<String> actual = new ArrayList<>();
        table.newReadBuilder()
                .newScan()
                .plan()
                .splits()
                .forEach(
                        s -> {
                            try {
                                RecordReader<InternalRow> recordReader = read.createReader(s);
                                CloseableIterator<InternalRow> it =
                                        new RecordReaderIterator<>(recordReader);
                                while (it.hasNext()) {
                                    InternalRow row = it.next();
                                    actual.add(row.getInt(0) + ", " + row.getLong(1));
                                }
                                it.close();
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        });
        Collections.sort(actual);
        assertThat(actual).isEqualTo(Arrays.asList(expected));
    }

    protected FileStoreTable createFileStoreTable() throws Exception {
        return createFileStoreTable(options -> {}, Collections.emptyList());
    }

    protected FileStoreTable createFileStoreTable(Consumer<Options> setOptions) throws Exception {
        return createFileStoreTable(setOptions, Collections.emptyList());
    }

    protected FileStoreTable createFileStoreTable(
            Consumer<Options> setOptions, List<String> partitionKeys) throws Exception {
        Options conf = new Options();
        conf.set(CoreOptions.PATH, tablePath.toString());
        conf.setString("bucket", "1");
        conf.setString("bucket-key", "a");
        setOptions.accept(conf);
        SchemaManager schemaManager = new SchemaManager(LocalFileIO.create(), tablePath);
        schemaManager.createTable(
                new Schema(
                        ROW_TYPE.getFields(),
                        partitionKeys,
                        Collections.emptyList(),
                        conf.toMap(),
                        ""));
        return FileStoreTableFactory.create(LocalFileIO.create(), conf);
    }
}
