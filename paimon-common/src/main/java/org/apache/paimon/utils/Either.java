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

import java.io.Serializable;

/** Either class, A or B. Reference to Flink. */
public abstract class Either<L, R> implements Serializable {

    public static <L, R> Either<L, R> left(L value) {
        return new Left<>(value);
    }

    public static <L, R> Either<L, R> right(R value) {
        return new Right<>(value);
    }

    public abstract L left() throws IllegalStateException;

    public abstract R right() throws IllegalStateException;

    public final boolean isLeft() {
        return this.getClass() == Left.class;
    }

    public final boolean isRight() {
        return this.getClass() == Right.class;
    }

    /** A Left value. */
    public static class Left<L, R> extends Either<L, R> {
        private L value;

        public Left(L value) {
            this.value = value;
        }

        public L left() {
            return this.value;
        }

        public R right() {
            throw new IllegalStateException("Cannot retrieve right value on a left");
        }

        public void setValue(L value) {
            this.value = value;
        }

        public boolean equals(Object object) {
            if (object instanceof Left) {
                Left<?, ?> other = (Left<?, ?>) object;
                return this.value.equals(other.value);
            } else {
                return false;
            }
        }

        public int hashCode() {
            return this.value.hashCode();
        }

        public String toString() {
            return "left(" + this.value.toString() + ")";
        }

        public static <L, R> Left<L, R> of(L left) {
            return new Left<L, R>(left);
        }
    }

    /** A Right value. */
    public static class Right<L, R> extends Either<L, R> {
        private R value;

        public Right(R value) {
            this.value = value;
        }

        public L left() {
            throw new IllegalStateException("Cannot retrieve left value on a right");
        }

        public R right() {
            return this.value;
        }

        public void setValue(R value) {
            this.value = value;
        }

        public boolean equals(Object object) {
            if (object instanceof Right) {
                Right<?, ?> other = (Right<?, ?>) object;
                return this.value.equals(other.value);
            } else {
                return false;
            }
        }

        public int hashCode() {
            return this.value.hashCode();
        }

        public String toString() {
            return "right(" + this.value.toString() + ")";
        }

        public static <L, R> Right<L, R> of(R right) {
            return new Right<>(right);
        }
    }
}
