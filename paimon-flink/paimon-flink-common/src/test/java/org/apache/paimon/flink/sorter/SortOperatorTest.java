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

package org.apache.paimon.flink.sorter;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.VarCharType;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.file.Path;
import java.util.Arrays;

/** Test for {@link SortOperator}. */
public class SortOperatorTest {

    @TempDir private Path tempDir;

    RowType keyRowType =
            new RowType(Arrays.asList(new DataField(0, "a", new VarCharType(), "Someone's desc.")));

    RowType valueRowType =
            new RowType(
                    Arrays.asList(
                            new DataField(0, "b", new VarCharType(), "Someone's desc."),
                            new DataField(1, "c`", new BigIntType())));

    @Test
    public void testCloseSortOprator() throws Exception {
        SortOperator sortOperator =
                new SortOperator(
                        keyRowType,
                        valueRowType,
                        MemorySize.parse("10 mb").getBytes(),
                        (int) MemorySize.parse("16 kb").getBytes()) {
                    @Override
                    protected Configuration jobConfiguration() {
                        Configuration configuration = new Configuration();
                        configuration.set(CoreOptions.TMP_DIRS, tempDir.toString());
                        return configuration;
                    }
                };

        char[] data = new char[1024];
        for (int i = 0; i < 1024; i++) {
            data[i] = (char) ('a' + i % 26);
        }

        sortOperator.initBuffer();

        for (int i = 0; i < 10000; i++) {
            sortOperator.processElement(
                    new StreamRecord<>(
                            GenericRow.of(
                                    BinaryString.fromString(String.valueOf(data)),
                                    BinaryString.fromString(String.valueOf(data)),
                                    (long) i)));
        }

        try {
            sortOperator.endInput();
        } catch (NullPointerException e) {
            // ignore null point exception
        }

        sortOperator.close();

        assertNoDataFile(tempDir.toFile());
    }

    private void assertNoDataFile(File fileDir) {
        Assertions.assertThat(fileDir.isDirectory()).isTrue();

        for (File file : fileDir.listFiles()) {
            assertNoDataFile(file);
        }
    }
}
