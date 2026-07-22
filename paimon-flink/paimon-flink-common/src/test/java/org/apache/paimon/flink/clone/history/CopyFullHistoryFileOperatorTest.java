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

package org.apache.paimon.flink.clone.history;

import org.apache.paimon.clone.FullHistoryCopyPlan;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link CopyFullHistoryFileOperator}. */
public class CopyFullHistoryFileOperatorTest {

    @TempDir private java.nio.file.Path tempDir;

    @Test
    public void testDuplicateTargetIsCopiedOnlyOnce() throws Exception {
        java.nio.file.Path source = tempDir.resolve("source");
        java.nio.file.Path target = tempDir.resolve("target");
        byte[] content = "content".getBytes(StandardCharsets.UTF_8);
        Files.write(source, content);

        CountingLocalFileIO targetFileIO = new CountingLocalFileIO();
        CopyFullHistoryFileOperator operator =
                new CopyFullHistoryFileOperator(LocalFileIO.create(), targetFileIO);
        FullHistoryCopyPlan.FileCopy copy =
                new FullHistoryCopyPlan.FileCopy(
                        new Path(source.toString()),
                        new Path(target.toString()),
                        FullHistoryCopyPlan.FileKind.DATA,
                        content.length);

        try (KeyedOneInputStreamOperatorTestHarness<String, FullHistoryCopyPlan.FileCopy, Boolean>
                harness = createHarness(operator)) {
            harness.open();
            harness.processElement(new StreamRecord<>(copy));
            int existsCallsAfterCopy = targetFileIO.existsCalls;

            harness.processElement(new StreamRecord<>(copy));

            assertThat(targetFileIO.existsCalls).isEqualTo(existsCallsAfterCopy);
            assertThat(Files.readAllBytes(target)).isEqualTo(content);
        }
    }

    @Test
    public void testDuplicateTargetWithDifferentSourceFails() throws Exception {
        java.nio.file.Path firstSource = tempDir.resolve("source-1");
        java.nio.file.Path secondSource = tempDir.resolve("source-2");
        java.nio.file.Path target = tempDir.resolve("target");
        Files.write(firstSource, new byte[] {1});
        Files.write(secondSource, new byte[] {2});

        CopyFullHistoryFileOperator operator =
                new CopyFullHistoryFileOperator(LocalFileIO.create(), LocalFileIO.create());
        FullHistoryCopyPlan.FileCopy first = copy(firstSource, target);
        FullHistoryCopyPlan.FileCopy second = copy(secondSource, target);

        try (KeyedOneInputStreamOperatorTestHarness<String, FullHistoryCopyPlan.FileCopy, Boolean>
                harness = createHarness(operator)) {
            harness.open();
            harness.processElement(new StreamRecord<>(first));

            assertThatThrownBy(() -> harness.processElement(new StreamRecord<>(second)))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("Conflicting clone payloads map to target");
        }
    }

    private static FullHistoryCopyPlan.FileCopy copy(
            java.nio.file.Path source, java.nio.file.Path target) {
        return new FullHistoryCopyPlan.FileCopy(
                new Path(source.toString()),
                new Path(target.toString()),
                FullHistoryCopyPlan.FileKind.DATA,
                1L);
    }

    private static KeyedOneInputStreamOperatorTestHarness<
                    String, FullHistoryCopyPlan.FileCopy, Boolean>
            createHarness(CopyFullHistoryFileOperator operator) throws Exception {
        return new KeyedOneInputStreamOperatorTestHarness<>(
                operator,
                file -> file.target().toString(),
                BasicTypeInfo.STRING_TYPE_INFO,
                1,
                1,
                0);
    }

    private static class CountingLocalFileIO extends LocalFileIO {

        private static final long serialVersionUID = 1L;

        private int existsCalls;

        @Override
        public boolean exists(Path path) throws IOException {
            existsCalls++;
            return super.exists(path);
        }
    }
}
