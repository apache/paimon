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

package org.apache.paimon.operation;

import org.apache.paimon.annotation.Public;
import org.apache.paimon.utils.ExceptionUtils;

import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.concurrent.Callable;

/** A table lock covering an operation and its publication. */
@Public
public interface Lock extends AutoCloseable {

    /**
     * Acquire a scope to release on the acquiring thread. Clean up partial acquisition on failure.
     */
    AutoCloseable acquire() throws Exception;

    /**
     * Run an operation under the lock. Acquisition failures are distinguished from operation
     * failures; interruption stops acquisition. Releasing a scope must not replace the operation's
     * result or exception.
     */
    default <T> T runWithLock(Callable<T> callable) throws Exception {
        AutoCloseable scope;
        try {
            scope = acquire();
        } catch (Exception e) {
            if (ExceptionUtils.findThrowable(e, InterruptedException.class).isPresent()) {
                Thread.currentThread().interrupt();
                throw e;
            }
            throw new AcquisitionException(e);
        }

        Throwable failure = null;
        try {
            return callable.call();
        } catch (Exception | Error e) {
            failure = e;
            throw e;
        } finally {
            release(scope, failure);
        }
    }

    /** Release a scope without losing the outcome of its operation or partial acquisition. */
    static void release(AutoCloseable scope, @Nullable Throwable failure) {
        try {
            scope.close();
        } catch (Exception e) {
            if (ExceptionUtils.findThrowable(e, InterruptedException.class).isPresent()) {
                Thread.currentThread().interrupt();
            }
            if (failure != null && failure != e) {
                failure.addSuppressed(e);
            }
            LoggerFactory.getLogger(Lock.class).warn("Failed to release table lock.", e);
        }
    }

    /** Fail before publication when a held lease has expired or renewal has failed. */
    default void ensureValid() {}

    static Lock empty() {
        return new EmptyLock();
    }

    /** Distinguishes failure to acquire a scope from failure of its operation or publication. */
    class AcquisitionException extends Exception {
        private AcquisitionException(Exception cause) {
            super("Failed to acquire table lock.", cause);
        }
    }

    /** An operation with no external lock configured. */
    class EmptyLock implements Lock {
        @Override
        public AutoCloseable acquire() {
            return () -> {};
        }

        @Override
        public <T> T runWithLock(Callable<T> callable) throws Exception {
            return callable.call();
        }

        @Override
        public void close() {}
    }

    /** Reuse the current scope for nested publication on the same thread. */
    static Lock reentrant(Lock lock) {
        return lock instanceof EmptyLock || lock instanceof ReentrantLock
                ? lock
                : new ReentrantLock(lock);
    }

    /** Keeps the outer operation's scope until preparation and publication both finish. */
    class ReentrantLock implements Lock {
        private final Lock delegate;
        private final ThreadLocal<Boolean> held = ThreadLocal.withInitial(() -> false);

        private ReentrantLock(Lock delegate) {
            this.delegate = delegate;
        }

        @Override
        public AutoCloseable acquire() throws Exception {
            if (held.get()) {
                delegate.ensureValid();
                return () -> {};
            }
            AutoCloseable scope = delegate.acquire();
            try {
                delegate.ensureValid();
                held.set(true);
                return () -> {
                    held.remove();
                    scope.close();
                };
            } catch (Exception | Error e) {
                release(scope, e);
                throw e;
            }
        }

        @Override
        public <T> T runWithLock(Callable<T> callable) throws Exception {
            if (held.get()) {
                delegate.ensureValid();
                return callable.call();
            }
            return Lock.super.runWithLock(callable);
        }

        @Override
        public void ensureValid() {
            delegate.ensureValid();
        }

        @Override
        public void close() throws Exception {
            delegate.close();
        }
    }
}
