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

package org.apache.paimon.operation;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.options.Options;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;

/** Tests the full-page boundary without changing either sorter's minimum allocation. */
class FileEntryMergeBufferSmallBufferTest {

    @ParameterizedTest
    @CsvSource({"0, false", "256, false", "383, false", "384, true", "448, true"})
    void testFullPageCapacity(int bufferKibiBytes, boolean supported) {
        Options options = new Options();
        options.set(CoreOptions.PAGE_SIZE, MemorySize.ofKibiBytes(64));
        options.set(CoreOptions.SORT_SPILL_BUFFER_SIZE, MemorySize.ofKibiBytes(bufferKibiBytes));
        CoreOptions coreOptions = new CoreOptions(options);
        assertThat(FileEntryMergeBuffer.supportsBufferSize(coreOptions)).isEqualTo(supported);
        if (!supported) {
            IOManager io = mock(IOManager.class);
            assertThatThrownBy(() -> new FileEntryMergeBuffer(coreOptions, io))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("at least six sort-buffer pages");
            verifyNoInteractions(io);
        }
    }
}
