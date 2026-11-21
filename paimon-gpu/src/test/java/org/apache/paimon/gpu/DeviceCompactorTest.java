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

package org.apache.paimon.gpu;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.SpecialFields;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.concurrent.CancellationException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests that {@link DeviceCompactor} starts no device work for a cancelled caller or after it is
 * closed, and that the rewriters' statistics stay bounded. No device is used.
 */
public class DeviceCompactorTest {

    @Test
    public void testClosedCompactorTakesNoBatch() {
        DeviceCompactor device = new DeviceCompactor(layout());
        device.close();

        assertThatThrownBy(() -> compact(device)).isInstanceOf(CancellationException.class);
    }

    @Test
    public void testInterruptedCallerTakesNoBatch() {
        DeviceCompactor device = new DeviceCompactor(layout());
        Thread.currentThread().interrupt();
        try {
            assertThatThrownBy(() -> compact(device)).isInstanceOf(InterruptedException.class);
        } finally {
            Thread.interrupted();
            device.close();
        }
    }

    @Test
    public void testStatsCountEveryCpuCompactionButKeepFewReasons() {
        GpuMergeTreeCompactRewriter.Stats stats = new GpuMergeTreeCompactRewriter.Stats();
        for (int i = 0; i < 1000; i++) {
            stats.cpu("reason " + i);
        }

        assertThat(stats.toString())
                .contains("cpu compactions=1000 [reason 0, reason 1, reason 2]")
                .doesNotContain("reason 3");
    }

    private static void compact(DeviceCompactor device) throws InterruptedException {
        device.compact(
                Collections.singletonList("/nowhere.parquet"), 1, 0, "/out.parquet", true, 0);
    }

    private static KvLayout layout() {
        RowType key =
                new RowType(
                        Collections.singletonList(
                                new DataField(
                                        SpecialFields.KEY_FIELD_ID_START,
                                        SpecialFields.KEY_FIELD_PREFIX + "k",
                                        DataTypes.BIGINT().notNull())));
        RowType value =
                new RowType(
                        Collections.singletonList(
                                new DataField(0, "k", DataTypes.BIGINT().notNull())));
        Options options = new Options();
        options.set("file.format", "parquet");
        options.set("file.compression", "zstd");
        return KvLayout.of(key, value, new CoreOptions(options), new StringBuilder());
    }
}
