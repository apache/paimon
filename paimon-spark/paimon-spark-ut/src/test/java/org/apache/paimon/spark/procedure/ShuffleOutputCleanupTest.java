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

package org.apache.paimon.spark.procedure;

import org.apache.spark.MapOutputTrackerMaster;
import org.apache.spark.ShuffleStatus;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkException;
import org.apache.spark.scheduler.MapStatus;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.lang.reflect.InvocationTargetException;
import java.util.stream.Stream;

import scala.collection.Iterator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

/** Tests for the shuffle output cleanup used by {@link RemoveOrphanBlobsProcedureTest}. */
class ShuffleOutputCleanupTest {

    @Test
    void testClearRegisteredShuffleOutputs() {
        MapOutputTrackerMaster tracker =
                new MapOutputTrackerMaster(new SparkConf(false), null, true);
        try {
            registerOutput(tracker, 0);
            ShuffleStatus status = tracker.shuffleStatuses().get(0).get();
            assertThat(status.numAvailableMapOutputs()).isEqualTo(1);
            long epoch = tracker.getEpoch();

            clearShuffleOutputs(tracker, 0);

            assertThat(status.numAvailableMapOutputs()).isZero();
            assertThat(tracker.containsShuffle(0)).isTrue();
            assertThat(tracker.getEpoch()).isEqualTo(epoch + 1);
        } finally {
            tracker.stop();
        }
    }

    @Test
    void testConcurrentUnregistrationDoesNotSkipRemainingShuffles() {
        MapOutputTrackerMaster tracker =
                new MapOutputTrackerMaster(new SparkConf(false), null, true);
        try {
            registerOutput(tracker, 0);
            registerOutput(tracker, 1);
            Iterator<?> ids = tracker.shuffleStatuses().keysIterator();
            int[] capturedIds = {(Integer) ids.next(), (Integer) ids.next()};
            // Force removal between enumeration and cleanup, without relying on GC timing.
            tracker.unregisterShuffle(capturedIds[0]);
            long epoch = tracker.getEpoch();

            clearShuffleOutputs(tracker, capturedIds);

            assertThat(tracker.containsShuffle(capturedIds[0])).isFalse();
            assertThat(tracker.containsShuffle(capturedIds[1])).isTrue();
            assertThat(tracker.shuffleStatuses().get(capturedIds[1]).get().numAvailableMapOutputs())
                    .isZero();
            assertThat(tracker.getEpoch()).isEqualTo(epoch + 1);
        } finally {
            tracker.stop();
        }
    }

    @ParameterizedTest
    @MethodSource("unrelatedFailures")
    void testUnrelatedFailuresPropagate(Exception failure) {
        assertThatThrownBy(() -> clearShuffleOutputs(new ThrowingTracker(failure), 0))
                .isInstanceOf(InvocationTargetException.class)
                .hasCauseReference(failure);
    }

    private static Stream<Exception> unrelatedFailures() {
        return Stream.of(
                new SparkException("Unrelated failure"),
                new SparkException((String) null),
                new SparkException(
                        "unregisterAllMapAndMergeOutput called for nonexistent shuffle ID 1."),
                new SparkException("unregisterMapOutput called for nonexistent shuffle ID 0."),
                new IllegalStateException(
                        "unregisterAllMapAndMergeOutput called for nonexistent shuffle ID 0."));
    }

    @Test
    void testMissingReflectionMethodPropagates() {
        assertThatThrownBy(() -> clearShuffleOutputs(new Object(), 0))
                .isInstanceOf(NoSuchMethodException.class);
    }

    private static void clearShuffleOutputs(Object tracker, int... shuffleIds) {
        RemoveOrphanBlobsProcedureTest$.MODULE$.clearShuffleOutputs(tracker, shuffleIds);
    }

    private static void registerOutput(MapOutputTrackerMaster tracker, int id) {
        tracker.registerShuffle(id, 1, 1);
        tracker.registerMapOutput(id, 0, mock(MapStatus.class));
    }

    /** Public so that the cleanup helper can invoke the method through reflection. */
    public static class ThrowingTracker {
        private final Exception failure;

        ThrowingTracker(Exception failure) {
            this.failure = failure;
        }

        public void unregisterAllMapAndMergeOutput(int id) throws Exception {
            throw failure;
        }
    }
}
