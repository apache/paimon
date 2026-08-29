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

package org.apache.paimon.format.orc;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.format.FormatReaderContext;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.OrcConf;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Test positional schema evolution in {@link OrcReaderFactory}. */
class OrcPositionalSchemaEvolutionTest {

    @TempDir File folder;

    @Test
    void testPositionalSchemaEvolution() throws Exception {
        Path path = writeOrcFile();
        RowType readType = RowType.builder().field("id", DataTypes.INT()).build();
        Configuration conf = new Configuration(false);
        OrcConf.FORCE_POSITIONAL_EVOLUTION.setBoolean(conf, true);

        OrcReaderFactory factory =
                new OrcReaderFactory(conf, readType, Collections.emptyList(), 1024, false, false);
        LocalFileIO fileIO = new LocalFileIO();
        List<Integer> values = new ArrayList<>();
        try (RecordReader<InternalRow> reader =
                factory.createReader(
                        new FormatReaderContext(
                                fileIO, path, fileIO.getFileSize(path), null, null))) {
            reader.forEachRemaining(row -> values.add(row.isNullAt(0) ? null : row.getInt(0)));
        }

        assertThat(values).containsExactly(42);
    }

    private Path writeOrcFile() throws Exception {
        Path path = new Path(folder.getPath(), "legacy.orc");
        TypeDescription schema = TypeDescription.fromString("struct<_col0:int>");
        try (Writer writer =
                OrcFile.createWriter(
                        new org.apache.hadoop.fs.Path(path.toString()),
                        OrcFile.writerOptions(new Configuration(false)).setSchema(schema))) {
            VectorizedRowBatch batch = schema.createRowBatch();
            ((LongColumnVector) batch.cols[0]).vector[0] = 42;
            batch.size = 1;
            writer.addRowBatch(batch);
        }
        return path;
    }
}
