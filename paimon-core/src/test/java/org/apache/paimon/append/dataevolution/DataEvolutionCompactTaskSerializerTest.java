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

package org.apache.paimon.append.dataevolution;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataFileMetaSerializer;
import org.apache.paimon.io.DataInputDeserializer;
import org.apache.paimon.io.DataOutputSerializer;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.utils.Range;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.paimon.stats.SimpleStats.EMPTY_STATS;
import static org.apache.paimon.utils.SerializationUtils.serializeBinaryRow;
import static org.assertj.core.api.Assertions.assertThat;

/** Task serialization keeps clustering provenance without changing ordinary compaction defaults. */
class DataEvolutionCompactTaskSerializerTest {

    @Test
    void testOutputLevelsSurviveMixedTaskList() throws Exception {
        DataEvolutionCompactTaskSerializer serializer = new DataEvolutionCompactTaskSerializer();
        List<DataEvolutionCompactTask> tasks =
                Arrays.asList(
                        new DataEvolutionNormalCompactTask(
                                BinaryRow.EMPTY_ROW,
                                files(),
                                Collections.singletonList(new Range(0, 2)),
                                2),
                        new DataEvolutionNormalCompactTask(BinaryRow.EMPTY_ROW, files()),
                        new DataEvolutionBlobCompactTask(BinaryRow.EMPTY_ROW, files()));
        DataOutputSerializer out = new DataOutputSerializer(128);
        serializer.serializeList(tasks, out);
        List<DataEvolutionCompactTask> restored =
                serializer.deserializeList(
                        serializer.getVersion(), new DataInputDeserializer(out.getCopyOfBuffer()));
        assertThat(restored).containsExactlyElementsOf(tasks);
        assertThat(((DataEvolutionNormalCompactTask) restored.get(0)).outputLevel()).isEqualTo(2);
        assertThat(((DataEvolutionNormalCompactTask) restored.get(1)).outputLevel()).isZero();
    }

    @Test
    void testVersionFourDefaultsToUnsortedOutput() throws Exception {
        DataEvolutionCompactTaskSerializer serializer = new DataEvolutionCompactTaskSerializer();
        DataOutputSerializer out = new DataOutputSerializer(128);
        List<DataFileMeta> files = files();
        serializeBinaryRow(BinaryRow.EMPTY_ROW, out);
        new DataFileMetaSerializer().serializeList(files, out);
        out.writeInt(DataEvolutionCompactTask.TaskType.NORMAL.code());
        out.writeInt(1);
        out.writeLong(0);
        out.writeLong(2);
        DataEvolutionNormalCompactTask restored =
                (DataEvolutionNormalCompactTask) serializer.deserialize(4, out.getCopyOfBuffer());
        assertThat(restored.outputLevel()).isZero();
        assertThat(restored.protectedRanges()).containsExactly(new Range(0, 2));
        assertThat(restored.compactBefore()).containsExactlyElementsOf(files);
    }

    private static List<DataFileMeta> files() {
        return Collections.singletonList(
                DataFileMeta.forAppend(
                        "normal.parquet",
                        100,
                        3,
                        EMPTY_STATS,
                        10,
                        10,
                        0,
                        Collections.emptyList(),
                        null,
                        FileSource.APPEND,
                        null,
                        null,
                        0L,
                        null));
    }
}
