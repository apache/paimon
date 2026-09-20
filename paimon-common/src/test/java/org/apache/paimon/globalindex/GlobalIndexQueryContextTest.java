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

    @Test
    public void testForkHasIndependentBudget() {
        GlobalIndexQueryContext template = new GlobalIndexQueryContext(2);
        GlobalIndexQueryContext first = template.fork();
        GlobalIndexQueryContext second = template.fork();

        first.reserveDecodedRowIds(2);
        second.reserveDecodedRowIds(2);

        assertThat(template.decodedRowIds()).isZero();
        assertThat(first.decodedRowIds()).isEqualTo(2);
        assertThat(second.decodedRowIds()).isEqualTo(2);
    }

    @Test
    public void testForksShareQueryBudget() {
        GlobalIndexQueryContext template =
                new GlobalIndexQueryContext(3, 4, Long.MAX_VALUE, Long.MAX_VALUE);
        GlobalIndexQueryContext first = template.fork();
        GlobalIndexQueryContext second = template.fork();

        first.reserveDecodedRowIds(3);
        second.reserveDecodedRowIds(1);

        assertThat(template.totalDecodedRowIds()).isEqualTo(4);
        assertThatThrownBy(() -> second.reserveDecodedRowIds(1))
                .isInstanceOf(GlobalIndexLookupDeclinedException.class);
        assertThat(template.totalDecodedRowIds()).isEqualTo(4);
    }

    @Test
    public void testDeclinedFieldKeepsConsumedQueryWorkCharged() {
        GlobalIndexQueryContext template = new GlobalIndexQueryContext(2, 3, 4, 6);
        GlobalIndexQueryContext broad = template.fork();
        GlobalIndexQueryContext selective = template.fork();

        broad.reserveDecodedRowIds(2);
        broad.reserveReadBytes(4);
        assertThatThrownBy(() -> broad.reserveDecodedRowIds(1))
                .isInstanceOf(GlobalIndexLookupDeclinedException.class);

        assertThat(template.totalDecodedRowIds()).isEqualTo(2);
        assertThat(template.totalReadBytes()).isEqualTo(4);
        selective.reserveDecodedRowIds(1);
        selective.reserveReadBytes(2);
        assertThat(template.totalDecodedRowIds()).isEqualTo(3);
        assertThat(template.totalReadBytes()).isEqualTo(6);
    }

    @Test
    public void testUnlimitedFieldLimitStillHonorsQueryLimit() {
        GlobalIndexQueryContext template =
                new GlobalIndexQueryContext(Long.MAX_VALUE, 1, Long.MAX_VALUE, Long.MAX_VALUE);
        GlobalIndexQueryContext field = template.fork();

        field.reserveDecodedRowIds(1);
        assertThatThrownBy(() -> field.reserveDecodedRowIds(1))
                .isInstanceOf(GlobalIndexLookupDeclinedException.class);
    }

    @Test
    public void testUnlimitedReservationsDoNotAccumulate() {
        for (GlobalIndexQueryContext context :
                new GlobalIndexQueryContext[] {
                    GlobalIndexQueryContext.unlimited(),
                    new GlobalIndexQueryContext(
                            Long.MAX_VALUE, Long.MAX_VALUE, Long.MAX_VALUE, Long.MAX_VALUE)
                }) {
            context.reserveReadBytes(Long.MAX_VALUE);
            context.reserveReadBytes(Long.MAX_VALUE);
            context.reserveDecodedRowIds(Long.MAX_VALUE);
            context.reserveDecodedRowIds(Long.MAX_VALUE);
            assertThat(context.readBytes()).isZero();
            assertThat(context.decodedRowIds()).isZero();
            assertThatThrownBy(() -> context.reserveReadBytes(-1))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    public void testConcurrentForksDoNotExceedQueryBudget() throws Exception {
        GlobalIndexQueryContext template = new GlobalIndexQueryContext(10, 50, 10, 50);
        ExecutorService executor = Executors.newFixedThreadPool(8);
        try {
            List<Callable<Integer>> reservations = new ArrayList<>();
            for (int i = 0; i < 8; i++) {
                GlobalIndexQueryContext field = template.fork();
                reservations.add(
                        () -> {
                            int accepted = 0;
                            for (int attempt = 0; attempt < 10; attempt++) {
                                try {
                                    field.reserveReadBytes(1);
                                    accepted++;
                                } catch (GlobalIndexLookupDeclinedException e) {
                                    break;
                                }
                            }
                            assertThat(field.readBytes())
                                    .isEqualTo(accepted)
                                    .isLessThanOrEqualTo(10);
                            return accepted;
                        });
            }
            int accepted = 0;
            for (Future<Integer> result : executor.invokeAll(reservations)) {
                accepted += result.get();
            }
            assertThat(accepted).isEqualTo(50);
            assertThat(template.totalReadBytes()).isEqualTo(50);
        } finally {
            executor.shutdownNow();
        }
    }
}
