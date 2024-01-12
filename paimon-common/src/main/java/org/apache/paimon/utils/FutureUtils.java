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

import javax.annotation.Nullable;

import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;

/** A collection of utilities that expand the usage of {@link CompletableFuture}. */
public class FutureUtils {

    /**
     * Returns an exceptionally completed {@link CompletableFuture}.
     *
     * @param cause to complete the future with
     * @param <T> type of the future
     * @return An exceptionally completed CompletableFuture
     */
    public static <T> CompletableFuture<T> completedExceptionally(Throwable cause) {
        CompletableFuture<T> result = new CompletableFuture<>();
        result.completeExceptionally(cause);

        return result;
    }

    /**
     * Forwards the value from the source future to the target future.
     *
     * @param source future to forward the value from
     * @param target future to forward the value to
     * @param <T> type of the value
     */
    public static <T> void forward(CompletableFuture<T> source, CompletableFuture<T> target) {
        source.whenComplete(forwardTo(target));
    }

    private static <T> BiConsumer<T, Throwable> forwardTo(CompletableFuture<T> target) {
        return (value, throwable) -> doForward(value, throwable, target);
    }

    /**
     * Completes the given future with either the given value or throwable, depending on which
     * parameter is not null.
     *
     * @param value value with which the future should be completed
     * @param throwable throwable with which the future should be completed exceptionally
     * @param target future to complete
     * @param <T> completed future
     */
    public static <T> void doForward(
            @Nullable T value, @Nullable Throwable throwable, CompletableFuture<T> target) {
        if (throwable != null) {
            target.completeExceptionally(throwable);
        } else {
            target.complete(value);
        }
    }
}
