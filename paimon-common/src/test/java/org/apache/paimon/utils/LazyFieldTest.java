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

package org.apache.paimon.utils;

import org.junit.jupiter.api.Test;

import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link LazyField}. */
public class LazyFieldTest {

    @Test
    void testLazyEvaluation() {
        AtomicInteger counter = new AtomicInteger(0);
        LazyField<String> lazyField =
                new LazyField<>(
                        () -> {
                            counter.incrementAndGet();
                            return "hello";
                        });

        assertThat(lazyField.initialized()).isFalse();
        assertThat(counter.get()).isEqualTo(0);

        assertThat(lazyField.get()).isEqualTo("hello");
        assertThat(lazyField.initialized()).isTrue();
        assertThat(counter.get()).isEqualTo(1);

        // second call should not re-evaluate
        assertThat(lazyField.get()).isEqualTo("hello");
        assertThat(counter.get()).isEqualTo(1);
    }

    @Test
    void testSupplierReleasedAfterGet() throws Exception {
        LazyField<String> lazyField = new LazyField<>(() -> "hello");

        java.lang.reflect.Field supplierField = LazyField.class.getDeclaredField("supplier");
        supplierField.setAccessible(true);

        // before evaluation, supplier is held
        assertThat(supplierField.get(lazyField)).isNotNull();

        lazyField.get();

        // after evaluation, supplier is released for GC
        assertThat(supplierField.get(lazyField)).isNull();
    }

    @Test
    void testChainedLazyFieldsReleaseClosure() throws Exception {
        // Simulates the GlobalIndexResult.or() chain scenario:
        // K1 = lazy(() -> merge(r1, r2))
        // K2 = lazy(() -> merge(K1.get(), r3))
        // After K2.get(), K1's supplier should be released.

        final byte[] resource1 = new byte[512 * 1024];
        final byte[] resource2 = new byte[512 * 1024];

        // K1 captures resource1
        LazyField<String> k1 = new LazyField<>(() -> "r1=" + resource1.length);
        // K2 captures K1 and resource2
        LazyField<String> k2 = new LazyField<>(() -> k1.get() + ",r2=" + resource2.length);

        // trigger full chain evaluation
        String result = k2.get();
        assertThat(result).isEqualTo("r1=524288,r2=524288");

        // verify both suppliers are nulled after chain evaluation
        java.lang.reflect.Field supplierField = LazyField.class.getDeclaredField("supplier");
        supplierField.setAccessible(true);
        assertThat(supplierField.get(k1)).isNull();
        assertThat(supplierField.get(k2)).isNull();
    }

    @Test
    void testIntermediateBitmapsReclaimableAfterChainEvaluation() {
        // Simulates the real GlobalIndexResult.or() chain with RoaringNavigableMap64:
        // Each or() creates a NEW intermediate bitmap via RoaringNavigableMap64.or(x1, x2).
        // Before the fix, all intermediate bitmaps are retained forever.
        // After the fix, intermediate bitmaps become reclaimable by GC.

        final int chainLength = 10;
        List<WeakReference<RoaringNavigableMap64>> intermediateRefs = new ArrayList<>();

        // Build a chain: K0 = leaf, K1 = or(K0, leaf), K2 = or(K1, leaf), ...
        // This mirrors UnionGlobalIndexReader#union() with N readers.
        LazyField<RoaringNavigableMap64> current =
                new LazyField<>(
                        () -> {
                            RoaringNavigableMap64 bitmap = new RoaringNavigableMap64();
                            bitmap.add(0L);
                            return bitmap;
                        });

        for (int i = 1; i <= chainLength; i++) {
            final LazyField<RoaringNavigableMap64> prev = current;
            final long value = i;
            current =
                    new LazyField<>(
                            () -> {
                                // This simulates RoaringNavigableMap64.or(x1, x2) creating a new
                                // object
                                RoaringNavigableMap64 left = prev.get();
                                RoaringNavigableMap64 right = new RoaringNavigableMap64();
                                right.add(value);
                                RoaringNavigableMap64 merged =
                                        RoaringNavigableMap64.or(left, right);
                                // Track the intermediate result with a weak reference
                                intermediateRefs.add(new WeakReference<>(merged));
                                return merged;
                            });
        }

        // Trigger the full chain evaluation
        RoaringNavigableMap64 finalResult = current.get();

        // Final result should contain all values 0..chainLength
        for (long i = 0; i <= chainLength; i++) {
            assertThat(finalResult.contains(i)).isTrue();
        }

        // After evaluation with the fix (supplier=null), intermediate bitmaps
        // are no longer strongly referenced and can be reclaimed by GC.
        // Without the fix, the supplier chain holds them all alive.
        System.gc();

        int reclaimedCount = 0;
        for (WeakReference<RoaringNavigableMap64> ref : intermediateRefs) {
            if (ref.get() == null) {
                reclaimedCount++;
            }
        }

        // The last merged bitmap is the finalResult (held by local variable),
        // so 9 out of 10 intermediate bitmaps should be reclaimed.
        assertThat(reclaimedCount).isEqualTo(chainLength - 1);
    }
}
