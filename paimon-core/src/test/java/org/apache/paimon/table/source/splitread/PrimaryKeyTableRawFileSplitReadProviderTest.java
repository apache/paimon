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

package org.apache.paimon.table.source.splitread;

import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.table.source.DataSplit;

import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.apache.paimon.data.BinaryRow.EMPTY_ROW;
import static org.apache.paimon.io.DataFileTestUtils.newFile;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link PrimaryKeyTableRawFileSplitReadProvider}. */
public class PrimaryKeyTableRawFileSplitReadProviderTest {

    @Test
    public void testBypassRawReadWhenKeyValueSequenceNumberEnabled() {
        PrimaryKeyTableRawFileSplitReadProvider provider =
                new PrimaryKeyTableRawFileSplitReadProvider(() -> null, read -> {});

        DataFileMeta file = newFile("data-0.orc", 1, 0, 100, 100, 0L);
        DataSplit split =
                DataSplit.builder()
                        .withPartition(EMPTY_ROW)
                        .withBucket(0)
                        .withBucketPath("bucket-0")
                        .withDataFiles(Collections.singletonList(file))
                        .rawConvertible(true)
                        .build();

        assertThat(provider.match(split, new SplitReadProvider.Context(false))).isTrue();
        assertThat(provider.match(split, new SplitReadProvider.Context(false, true))).isFalse();
    }
}
