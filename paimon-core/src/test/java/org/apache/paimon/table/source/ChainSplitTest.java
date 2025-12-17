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

package org.apache.paimon.table.source;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryRowWriter;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataFileTestDataGenerator;
import org.apache.paimon.utils.InstantiationUtil;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link ChainSplit}. */
public class ChainSplitTest {

    @Test
    public void testChainSplitSerde() throws IOException, ClassNotFoundException {
        BinaryRow logicalPartition = new BinaryRow(1);
        BinaryRowWriter writer = new BinaryRowWriter(logicalPartition);
        writer.writeString(0, BinaryString.fromString("20251202"));
        DataFileTestDataGenerator gen = DataFileTestDataGenerator.builder().build();
        List<DataFileMeta> dataFiles = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            dataFiles.add(gen.next().meta);
        }
        Map<String, String> fileBucketPathMapping = new HashMap<>();
        Map<String, String> fileBranchMapping = new HashMap<>();
        for (DataFileMeta dataFile : dataFiles) {
            fileBucketPathMapping.put(dataFile.fileName(), "dt=20251202/bucket_0");
            fileBranchMapping.put(dataFile.fileName(), "branch_0");
        }
        ChainSplit split =
                new ChainSplit(
                        logicalPartition, dataFiles, fileBucketPathMapping, fileBranchMapping);
        byte[] bytes = InstantiationUtil.serializeObject(split);
        ChainSplit newSplit =
                InstantiationUtil.deserializeObject(bytes, ChainSplit.class.getClassLoader());
        assertThat(fileBucketPathMapping).isEqualTo(newSplit.fileBucketPathMapping());
        assertThat(fileBranchMapping).isEqualTo(newSplit.fileBranchMapping());
        assertThat(newSplit).isEqualTo(split);
    }
}
