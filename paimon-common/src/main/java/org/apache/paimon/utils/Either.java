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

import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.function.Consumer;

/**
 * A functional programming utility class that represents a value of one of two possible types. This
 * is a discriminated union type that can hold either a Left value (typically representing an error
 * or failure case) or a Right value (typically representing a success or correct result).
 *
 * <p>The Either type is commonly used in functional programming to handle operations that can
 * either succeed or fail, providing a type-safe alternative to exceptions or null values.
 *
 * <p>Usage examples:
 *
 * <pre>{@code
 * // Create a Left (error case)
 * Either<String, Integer> error = Either.left("Invalid input");
 *
 * // Create a Right (success case)
 * Either<String, Integer> success = Either.right(42);
 *
 * // Check and handle the result
 * if (result.isRight()) {
 *     Integer value = result.getRight();
 *     // Process successful result
 * } else {
 *     String error = result.getLeft();
 *     // Handle error case
 * }
 * }</pre>
 *
 * @param <L> the type of the Left value (typically error/failure)
 * @param <R> the type of the Right value (typically success/result)
 */
public abstract class Either<L, R> {

    /** Private constructor to prevent direct instantiation. */
    private Either() {}

    /**
     * Static factory method to create a Left-sided Either instance. Typically represents a failure
     * or error.
     *
     * @param value The left-side value.
     * @param <L> The type of the left value.
     * @param <R> The type of the right value.
     * @return A new Left instance.
     */
    public static <L, R> Either<L, R> left(L value) {
        return new Left<>(value);
    }

    /**
     * Static factory method to create a Right-sided Either instance. Typically represents a
     * successful or correct result.
     *
     * @param value The right-side value.
     * @param <L> The type of the left value.
     * @param <R> The type of the right value.
     * @return A new Right instance.
     */
    public static <L, R> Either<L, R> right(R value) {
        return new Right<>(value);
    }

    /**
     * Checks if this instance is a Left.
     *
     * @return true if Left, false otherwise.
     */
    public abstract boolean isLeft();

    /**
     * Checks if this instance is a Right.
     *
     * @return true if Right, false otherwise.
     */
    public abstract boolean isRight();

    /**
     * Returns the left value if this is a Left instance.
     *
     * @return The left value.
     * @throws NoSuchElementException if this is a Right instance.
     */
    public abstract L getLeft();

    /**
     * Returns the right value if this is a Right instance.
     *
     * @return The right value.
     * @throws NoSuchElementException if this is a Left instance.
     */
    public abstract R getRight();

    /**
     * If this is a Left, performs the given action on its value.
     *
     * @param action The consumer function to execute.
     */
    public abstract void ifLeft(Consumer<L> action);

    /**
     * If this is a Right, performs the given action on its value.
     *
     * @param action The consumer function to execute.
     */
    public abstract void ifRight(Consumer<R> action);

    /** Private static inner class representing the Left state of Either. */
    private static final class Left<L, R> extends Either<L, R> {
        private final L value;

        private Left(L value) {
            this.value = Objects.requireNonNull(value, "Left value cannot be null");
        }

        @Override
        public boolean isLeft() {
            return true;
        }

        @Override
        public boolean isRight() {
            return false;
        }

        @Override
        public L getLeft() {
            return value;
        }

        @Override
        public R getRight() {
            throw new NoSuchElementException("Cannot call getRight() on a Left instance");
        }

        @Override
        public void ifLeft(Consumer<L> action) {
            action.accept(value);
        }

        @Override
        public void ifRight(Consumer<R> action) {
            // Do nothing
        }

        @Override
        public String toString() {
            return "Left(" + value + ")";
        }
    }

    /** Private static inner class representing the Right state of Either. */
    private static final class Right<L, R> extends Either<L, R> {
        private final R value;

        private Right(R value) {
            this.value = Objects.requireNonNull(value, "Right value cannot be null");
        }

        @Override
        public boolean isLeft() {
            return false;
        }

        @Override
        public boolean isRight() {
            return true;
        }

        @Override
        public L getLeft() {
            throw new NoSuchElementException("Cannot call getLeft() on a Right instance");
        }

        @Override
        public R getRight() {
            return value;
        }

        @Override
        public void ifLeft(Consumer<L> action) {
            // Do nothing
        }

        @Override
        public void ifRight(Consumer<R> action) {
            action.accept(value);
        }

        @Override
        public String toString() {
            return "Right(" + value + ")";
        }
    }
}
