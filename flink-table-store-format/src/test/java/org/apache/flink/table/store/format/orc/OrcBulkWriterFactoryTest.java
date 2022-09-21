/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.store.format.orc;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.local.LocalDataOutputStream;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;

import org.apache.hadoop.fs.Path;
import org.apache.orc.MemoryManager;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

/** Test for {@link OrcBulkWriterFactory}. */
public class OrcBulkWriterFactoryTest {
    @TempDir File tempDir;

    @Test
    public void testNotOverrideInMemoryManager() throws IOException {
        OrcFileFormat orcFileFormat = new OrcFileFormat(new Configuration());
        OrcBulkWriterFactory<RowData> factory =
                (OrcBulkWriterFactory<RowData>)
                        orcFileFormat.createWriterFactory(
                                RowType.of(new VarCharType(), new IntType()));

        TestMemoryManager memoryManager = new TestMemoryManager();
        factory.getWriterOptions().memory(memoryManager);

        factory.create(new LocalDataOutputStream(new File(tempDir, UUID.randomUUID().toString())));
        factory.create(new LocalDataOutputStream(new File(tempDir, UUID.randomUUID().toString())));

        List<Path> addedWriterPath = memoryManager.getAddedWriterPath();
        assertEquals(2, addedWriterPath.size());
        assertNotEquals(addedWriterPath.get(1), addedWriterPath.get(0));
    }

    private static class TestMemoryManager implements MemoryManager {
        private final List<Path> addedWriterPath = new ArrayList<>();

        @Override
        public void addWriter(Path path, long requestedAllocation, Callback callback) {
            addedWriterPath.add(path);
        }

        public List<Path> getAddedWriterPath() {
            return addedWriterPath;
        }

        @Override
        public void removeWriter(Path path) {}

        @Override
        public void addedRow(int rows) {}
    }
}
