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

package org.apache.paimon.globalindex;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link GlobalIndexQueryContext}. */
public class GlobalIndexQueryContextTest {

    @Test
    public void testExactBudgetIsAccepted() {
        GlobalIndexQueryContext context = new GlobalIndexQueryContext(3);

        context.reserveDecodedRowIds(1);
        context.reserveDecodedRowIds(2);

        assertThat(context.decodedRowIds()).isEqualTo(3);
        assertThatThrownBy(() -> context.reserveDecodedRowIds(1))
                .isInstanceOf(GlobalIndexLookupDeclinedException.class);
        assertThat(context.decodedRowIds()).isEqualTo(3);
    }

    @Test
    public void testConcurrentReservationsDoNotExceedBudget() throws Exception {
        int budget = 50;
        GlobalIndexQueryContext context = new GlobalIndexQueryContext(budget);
        ExecutorService executor = Executors.newFixedThreadPool(8);
        try {
            List<Callable<Boolean>> reservations = new ArrayList<>();
            for (int i = 0; i < budget * 2; i++) {
                reservations.add(
                        () -> {
                            try {
                                context.reserveDecodedRowIds(1);
                                return true;
                            } catch (GlobalIndexLookupDeclinedException e) {
                                return false;
                            }
                        });
            }

            int accepted = 0;
            for (Future<Boolean> result : executor.invokeAll(reservations)) {
                if (result.get()) {
                    accepted++;
                }
            }

            assertThat(accepted).isEqualTo(budget);
            assertThat(context.decodedRowIds()).isEqualTo(budget);
        } finally {
            executor.shutdownNow();
        }
    }
}
