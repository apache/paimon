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

package org.apache.paimon.memory;

import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link MemorySliceOutput}. */
public class MemorySliceOutputTest {

    @Test
    public void testWriteAfterZeroInitialCapacity() throws InterruptedException {
        MemorySliceOutput out = new MemorySliceOutput(0);
        AtomicReference<Throwable> failure = new AtomicReference<>();

        // Growth doubled from the segment size, which never leaves zero, so the first write
        // spun on the CPU. JUnit's @Timeout in its default same-thread mode only reports after
        // the method returns, so the write runs on a daemon thread and the test asserts that it
        // finished; the daemon flag keeps a regression from holding the surefire fork open.
        Thread writer =
                new Thread(
                        () -> {
                            try {
                                out.writeByte(5);
                                out.writeBytes(new byte[] {1, 2, 3}, 0, 3);
                            } catch (Throwable t) {
                                failure.set(t);
                            }
                        });
        writer.setDaemon(true);
        writer.start();
        writer.join(10_000);

        assertThat(writer.isAlive()).as("write did not terminate").isFalse();
        assertThat(failure.get()).isNull();
        assertThat(out.size()).isEqualTo(4);
        assertThat(out.toSlice().readByte(0)).isEqualTo((byte) 5);
        assertThat(out.toSlice().readByte(3)).isEqualTo((byte) 3);
    }
}
