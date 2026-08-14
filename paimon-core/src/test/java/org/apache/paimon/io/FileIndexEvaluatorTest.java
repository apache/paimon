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

package org.apache.paimon.io;

import org.apache.paimon.deletionvectors.BitmapDeletionVector;
import org.apache.paimon.fileindex.FileIndexFormat;
import org.apache.paimon.fileindex.FileIndexResult;
import org.apache.paimon.fileindex.FileIndexWriter;
import org.apache.paimon.fileindex.bitmap.BitmapFileIndex;
import org.apache.paimon.fileindex.bitmap.BitmapFileIndexFactory;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.stats.SimpleStats;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link FileIndexEvaluator}. */
public class FileIndexEvaluatorTest {

    @Test
    public void testDataFilterIntersectsDeletionVector() throws Exception {
        BitmapDeletionVector deletionVector = new BitmapDeletionVector();
        deletionVector.delete(0);

        FileIndexResult result =
                FileIndexEvaluator.evaluate(
                        null,
                        tableSchema(),
                        Collections.singletonList(
                                new PredicateBuilder(
                                                RowType.of(
                                                        new DataType[] {DataTypes.INT()},
                                                        new String[] {"a"}))
                                        .equal(0, 1)),
                        null,
                        null,
                        null,
                        DataFileMeta.forAppend(
                                "file",
                                0,
                                2,
                                SimpleStats.EMPTY_STATS,
                                0,
                                0,
                                0,
                                Collections.emptyList(),
                                embeddedBitmapIndex(),
                                null,
                                null,
                                null,
                                null,
                                null),
                        deletionVector);

        assertThat(result).isSameAs(FileIndexResult.SKIP);
    }

    private static TableSchema tableSchema() {
        return new TableSchema(
                1,
                Collections.singletonList(new DataField(0, "a", DataTypes.INT())),
                0,
                Collections.emptyList(),
                Collections.emptyList(),
                Collections.emptyMap(),
                null);
    }

    private static byte[] embeddedBitmapIndex() throws IOException {
        BitmapFileIndex bitmapFileIndex = new BitmapFileIndex(DataTypes.INT(), new Options());
        FileIndexWriter indexWriter = bitmapFileIndex.createWriter();
        indexWriter.write(1);
        indexWriter.write(2);

        Map<String, Map<String, byte[]>> indexes = new HashMap<>();
        indexes.put(
                "a",
                Collections.singletonMap(
                        BitmapFileIndexFactory.BITMAP_INDEX, indexWriter.serializedBytes()));

        ByteArrayOutputStream output = new ByteArrayOutputStream();
        try (FileIndexFormat.Writer writer = FileIndexFormat.createWriter(output)) {
            writer.writeColumnIndexes(indexes);
        }
        return output.toByteArray();
    }
}
