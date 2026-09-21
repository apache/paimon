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

package org.apache.paimon.table.object;

import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.source.ReadBuilder;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link ObjectTableImpl}. */
public class ObjectTableTest {

    @TempDir java.nio.file.Path tempPath;
    private ObjectTable table;

    @BeforeEach
    public void beforeEach() throws Exception {
        FileIO fileIO = LocalFileIO.create();
        Path objectPath = new Path(tempPath.toUri());
        fileIO.writeFile(new Path(objectPath, "drop.txt"), "drop", false);
        fileIO.writeFile(new Path(objectPath, "dir/keep.txt"), "keep", false);

        table =
                ObjectTable.builder()
                        .identifier(Identifier.create("default", "objects"))
                        .fileIO(fileIO)
                        .location(objectPath.toString())
                        .build();
    }

    @Test
    public void testReadWithoutFilter() throws Exception {
        assertThat(readPaths(table.newReadBuilder()))
                .containsExactlyInAnyOrder("drop.txt", "dir/keep.txt");
    }

    @Test
    public void testReadWithFilterAndProjection() throws Exception {
        Predicate predicate =
                new PredicateBuilder(ObjectTable.SCHEMA)
                        .equal(1, BinaryString.fromString("keep.txt"));
        ReadBuilder readBuilder =
                table.newReadBuilder().withFilter(predicate).withProjection(new int[] {0});

        assertThat(readPaths(readBuilder)).containsExactly("dir/keep.txt");
    }

    @Test
    public void testReadWithNoMatches() throws Exception {
        Predicate predicate =
                new PredicateBuilder(ObjectTable.SCHEMA)
                        .equal(1, BinaryString.fromString("missing.txt"));

        assertThat(readPaths(table.newReadBuilder().withFilter(predicate))).isEmpty();
    }

    private List<String> readPaths(ReadBuilder readBuilder) throws Exception {
        List<String> paths = new ArrayList<>();
        try (RecordReader<InternalRow> reader =
                readBuilder.newRead().createReader(readBuilder.newScan().plan())) {
            reader.forEachRemaining(row -> paths.add(row.getString(0).toString()));
        }
        return paths;
    }
}
