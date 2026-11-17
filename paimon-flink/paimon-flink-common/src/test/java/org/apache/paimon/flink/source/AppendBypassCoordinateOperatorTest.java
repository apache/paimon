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

package org.apache.paimon.flink.source;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.append.AppendCompactTask;
import org.apache.paimon.flink.sink.Committable;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.schema.FileSystemSchemaManager;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.RowType;

import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.types.Either;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThatCode;

/** Tests for {@link AppendBypassCoordinateOperator}. */
public class AppendBypassCoordinateOperatorTest {

    @TempDir java.nio.file.Path temp;

    @Test
    public void testCloseWithoutOpen() throws Exception {
        try (OneInputStreamOperatorTestHarness<Committable, Either<Committable, AppendCompactTask>>
                harness =
                        new OneInputStreamOperatorTestHarness<>(
                                new AppendBypassCoordinateOperatorFactory<>(createTable()))) {
            harness.setup();

            // Flink closes operators that were never opened when a task is cancelled during
            // restore.
            assertThatCode(() -> harness.getOperator().close()).doesNotThrowAnyException();
        }
    }

    private FileStoreTable createTable() throws Exception {
        Path tablePath = new Path(temp.toUri().toString());
        new FileSystemSchemaManager(LocalFileIO.create(), tablePath)
                .createTable(
                        new Schema(
                                RowType.of(new IntType()).getFields(),
                                Collections.emptyList(),
                                Collections.emptyList(),
                                Collections.singletonMap(CoreOptions.BUCKET.key(), "-1"),
                                ""));
        return FileStoreTableFactory.create(LocalFileIO.create(), tablePath);
    }
}
