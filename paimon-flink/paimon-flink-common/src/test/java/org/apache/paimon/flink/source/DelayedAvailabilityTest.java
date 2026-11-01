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

package org.apache.paimon.flink.source;

import org.apache.flink.core.testutils.ManuallyTriggeredScheduledExecutorService;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link DelayedAvailability}. */
public class DelayedAvailabilityTest {

    @Test
    public void testAvailableUntilDelayed() {
        try (DelayedAvailability availability =
                new DelayedAvailability(new ManuallyTriggeredScheduledExecutorService())) {
            assertThat(availability.future()).isDone();
        }
    }

    @Test
    public void testDelayCompletesWhenTimerFires() {
        ManuallyTriggeredScheduledExecutorService timer =
                new ManuallyTriggeredScheduledExecutorService();
        try (DelayedAvailability availability = new DelayedAvailability(timer)) {
            availability.delay(1_000L);

            assertThat(availability.future()).isNotDone();
            ScheduledFuture<?> wakeUp = timer.getAllNonPeriodicScheduledTask().get(0);
            assertThat(wakeUp.getDelay(TimeUnit.MILLISECONDS)).isEqualTo(1_000L);

            timer.triggerNonPeriodicScheduledTasks();

            assertThat(availability.future()).isDone();
        }
    }

    @Test
    public void testConcurrentDelaysDoNotBlockEachOther() {
        ManuallyTriggeredScheduledExecutorService timer =
                new ManuallyTriggeredScheduledExecutorService();
        try (DelayedAvailability first = new DelayedAvailability(timer);
                DelayedAvailability second = new DelayedAvailability(timer)) {
            first.delay(1_000L);
            second.delay(1_000L);

            // each wait is a queued timer task rather than a thread sleeping through the interval
            assertThat(timer.getAllNonPeriodicScheduledTask()).hasSize(2);
            assertThat(first.future()).isNotDone();
            assertThat(second.future()).isNotDone();

            timer.triggerNonPeriodicScheduledTasks();

            assertThat(first.future()).isDone();
            assertThat(second.future()).isDone();
        }
    }

    @Test
    public void testCloseCancelsThePendingDelay() {
        ManuallyTriggeredScheduledExecutorService timer =
                new ManuallyTriggeredScheduledExecutorService();
        try (DelayedAvailability availability = new DelayedAvailability(timer)) {
            availability.delay(1_000L);
            ScheduledFuture<?> wakeUp = timer.getAllNonPeriodicScheduledTask().get(0);

            availability.close();

            assertThat(wakeUp.isCancelled()).isTrue();
            assertThat(availability.future()).isDone();
        }
    }

    @Test
    public void testOwnedTimerCompletesTheDelay() throws Exception {
        try (DelayedAvailability availability =
                new DelayedAvailability("delayed-availability-test")) {
            availability.delay(10L);
            availability.future().get(10, TimeUnit.SECONDS);
        }
    }
}
