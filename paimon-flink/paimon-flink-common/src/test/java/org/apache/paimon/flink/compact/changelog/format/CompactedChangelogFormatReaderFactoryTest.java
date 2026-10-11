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

package org.apache.paimon.flink.compact.changelog.format;

import org.apache.paimon.format.FormatReaderFactory;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.io.DataFileRecordReader;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.RoaringBitmap32;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.annotation.Nullable;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link CompactedChangelogFormatReaderFactory}. */
class CompactedChangelogFormatReaderFactoryTest {

    @TempDir java.nio.file.Path tempDir;

    @Test
    void testReaderCreationExceptionIsNotReportedAsMissingFile() throws Exception {
        FileIO fileIO = LocalFileIO.create();
        Path partitionPath = new Path(tempDir.toUri());
        Path realPath =
                new Path(partitionPath, "bucket-74/compacted-changelog-test$74-1.cc-parquet");
        fileIO.mkdirs(realPath.getParent());
        try (PositionOutputStream out = fileIO.newOutputStream(realPath, false)) {
            out.write(new byte[] {1, 2});
        }

        Path virtualPath =
                new Path(partitionPath, "bucket-75/compacted-changelog-test$74-1-1-1.cc-parquet");
        FormatReaderFactory failingReaderFactory =
                context -> {
                    assertThat(context.fileIO().exists(context.filePath())).isTrue();
                    throw new IOException("simulated transient read failure");
                };

        FormatReaderFactory.Context context = context(fileIO, virtualPath);
        CompactedChangelogFormatReaderFactory readerFactory =
                new CompactedChangelogFormatReaderFactory(failingReaderFactory);

        assertThat(fileIO.exists(realPath)).isTrue();
        assertThat(fileIO.exists(virtualPath)).isFalse();
        assertThatThrownBy(
                        () ->
                                new DataFileRecordReader(
                                        RowType.of(DataTypes.INT()),
                                        readerFactory,
                                        context,
                                        false,
                                        false,
                                        null,
                                        null,
                                        null,
                                        false,
                                        null,
                                        0,
                                        Collections.emptyMap()))
                .isInstanceOf(IOException.class)
                .isNotInstanceOf(FileNotFoundException.class)
                .hasRootCauseMessage("simulated transient read failure");
    }

    private static FormatReaderFactory.Context context(FileIO fileIO, Path filePath) {
        return new FormatReaderFactory.Context() {
            @Override
            public FileIO fileIO() {
                return fileIO;
            }

            @Override
            public Path filePath() {
                return filePath;
            }

            @Override
            public long fileSize() {
                return 1;
            }

            @Nullable
            @Override
            public RoaringBitmap32 selection() {
                return null;
            }
        };
    }
}
