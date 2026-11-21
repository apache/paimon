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

package org.apache.paimon.data;

import javax.annotation.Nullable;

/** Scoped observations at the synchronous native Blob write and commit boundaries. */
public final class BlobIngestObservations {

    private static final ThreadLocal<State> CURRENT = new ThreadLocal<>();
    private static final Scope NO_OP = () -> {};

    private BlobIngestObservations() {}

    public static Scope observe(@Nullable Observer observer) {
        State previous = CURRENT.get();
        if (observer == null) {
            if (previous == null) {
                return NO_OP;
            }
            CURRENT.remove();
        } else {
            CURRENT.set(new State(observer));
        }
        return () -> {
            if (previous == null) {
                CURRENT.remove();
            } else {
                CURRENT.set(previous);
            }
        };
    }

    public static boolean enabled() {
        return CURRENT.get() != null;
    }

    public static boolean inBlob() {
        State state = CURRENT.get();
        return state != null && state.blobDepth != 0;
    }

    public static Scope enterBlob() {
        State state = CURRENT.get();
        if (state == null) {
            return NO_OP;
        }
        state.blobDepth++;
        return state.leaveBlob;
    }

    public static void record(String stage, String outcome, long value) {
        State state = CURRENT.get();
        if (state == null) {
            return;
        }
        if ("commit_publish".equals(stage)) {
            if ("unknown".equals(outcome)) {
                state.unknown += value;
            } else if ("reconciled".equals(outcome)) {
                state.reconciled += value;
            } else if ("committed".equals(outcome)) {
                state.committed += value;
            }
        }
        try {
            state.observer.record(stage, outcome, value);
        } catch (RuntimeException ignored) {
            // A reporter cannot change the result of a write or commit.
        }
    }

    /** Local publication evidence used to classify an ambiguous logical commit. */
    public static long total(String stage, String outcome) {
        State state = CURRENT.get();
        if (state == null || !"commit_publish".equals(stage)) {
            return 0;
        }
        switch (outcome) {
            case "unknown":
                return state.unknown;
            case "reconciled":
                return state.reconciled;
            case "committed":
                return state.committed;
            default:
                return 0;
        }
    }

    /** Receives bounded labels and numeric values for the current operation. */
    @FunctionalInterface
    public interface Observer {
        void record(String stage, String outcome, long value);
    }

    /** Restores the calling thread's observation context. */
    @FunctionalInterface
    public interface Scope extends AutoCloseable {
        @Override
        void close();
    }

    private static final class State {
        private final Observer observer;
        private int blobDepth;
        private final Scope leaveBlob = () -> blobDepth--;
        private long unknown;
        private long reconciled;
        private long committed;

        private State(Observer observer) {
            this.observer = observer;
        }
    }
}
