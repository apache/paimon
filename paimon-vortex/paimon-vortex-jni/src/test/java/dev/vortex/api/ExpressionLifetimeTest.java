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

package dev.vortex.api;

import dev.vortex.jni.NativeLoader;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@link Expression#column} releases the intermediate root it builds, so that pointer is freed
 * outside any {@link Expression} wrapper. Closing the returned expression must therefore free its
 * own pointer once and never the root's again: a second free of either would abort the JVM inside
 * this test rather than fail it.
 *
 * <p>That the child stays valid after the root is freed is covered end to end by the round-trip
 * tests in {@code VortexPredicateConverterTest}, which read rows through a scan built from
 * expressions this reader has already released.
 */
class ExpressionLifetimeTest {

    @BeforeAll
    static void loadNativeLibrary() {
        // Report a missing library as a test failure rather than an ExceptionInInitializerError
        // from the static initializer of NativeExpression.
        NativeLoader.loadJni();
    }

    @Test
    void closingAColumnFreesItsPointerExactlyOnce() {
        Expression column = Expression.column("col_a");
        assertThat(column.nativePointer()).isNotZero();

        column.close();
        assertThat(column.nativePointer()).isZero();

        // Idempotent: the second close must not reach the native side.
        column.close();
        assertThat(column.nativePointer()).isZero();
    }
}
