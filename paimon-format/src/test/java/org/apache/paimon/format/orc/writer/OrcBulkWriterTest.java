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

package org.apache.paimon.format.orc.writer;

import org.apache.paimon.format.FileFormat;
import org.apache.paimon.format.FormatWriter;
import org.apache.paimon.format.FormatWriterFactory;
import org.apache.paimon.format.orc.OrcFileFormat;
import org.apache.paimon.format.orc.OrcWriterFactory;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.options.Options;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;

import static org.apache.paimon.format.OrcOptions.ORC_WRITE_BATCH_SIZE;

class OrcBulkWriterTest {

    @Test
    void testRowBatch(@TempDir java.nio.file.Path tempDir) throws IOException {
        Options options = new Options();
        options.set(ORC_WRITE_BATCH_SIZE, 1);
        FileFormat orc = FileFormat.getFileFormat(options, "orc");
        Assertions.assertThat(orc).isInstanceOf(OrcFileFormat.class);

        RowType rowType =
                RowType.builder()
                        .field("a", DataTypes.INT())
                        .field("b", DataTypes.STRING())
                        .build();
        FormatWriterFactory writerFactory = orc.createWriterFactory(rowType);
        Assertions.assertThat(writerFactory).isInstanceOf(OrcWriterFactory.class);

        Path path = new Path(tempDir.toUri().toString(), "1.orc");
        PositionOutputStream out = LocalFileIO.create().newOutputStream(path, false);
        FormatWriter formatWriter = writerFactory.create(out, null);

        Assertions.assertThat(formatWriter).isInstanceOf(OrcBulkWriter.class);

        OrcBulkWriter orcBulkWriter = (OrcBulkWriter) formatWriter;
        Assertions.assertThat(orcBulkWriter.getRowBatch().getMaxSize()).isEqualTo(1);
    }
}
