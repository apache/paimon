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

package org.apache.paimon.elasticsearch.index.util;

import org.elasticsearch.logging.Level;
import org.elasticsearch.logging.Logger;

import java.util.function.Supplier;

/**
 * Bridges the Elasticsearch {@link Logger} interface to an SLF4J {@link org.slf4j.Logger}, so that
 * ES vector index code logs appear in the Paimon logging framework.
 */
public class Slf4jBridgeLogger implements Logger {

    private final org.slf4j.Logger delegate;

    public Slf4jBridgeLogger(org.slf4j.Logger delegate) {
        this.delegate = delegate;
    }

    @Override
    public String getName() {
        return delegate.getName();
    }

    @Override
    public boolean isFatalEnabled() {
        return delegate.isErrorEnabled();
    }

    @Override
    public boolean isErrorEnabled() {
        return delegate.isErrorEnabled();
    }

    @Override
    public boolean isWarnEnabled() {
        return delegate.isWarnEnabled();
    }

    @Override
    public boolean isInfoEnabled() {
        return delegate.isInfoEnabled();
    }

    @Override
    public boolean isDebugEnabled() {
        return delegate.isDebugEnabled();
    }

    @Override
    public boolean isTraceEnabled() {
        return delegate.isTraceEnabled();
    }

    @Override
    public boolean isEnabled(Level level) {
        switch (level) {
            case FATAL:
            case ERROR:
                return delegate.isErrorEnabled();
            case WARN:
                return delegate.isWarnEnabled();
            case INFO:
                return delegate.isInfoEnabled();
            case DEBUG:
                return delegate.isDebugEnabled();
            case TRACE:
            case ALL:
                return delegate.isTraceEnabled();
            case OFF:
            default:
                return false;
        }
    }

    // ---- generic log dispatchers ----

    @Override
    public void log(Level level, String msg) {
        switch (level) {
            case FATAL:
            case ERROR:
                delegate.error(msg);
                break;
            case WARN:
                delegate.warn(msg);
                break;
            case INFO:
                delegate.info(msg);
                break;
            case DEBUG:
                delegate.debug(msg);
                break;
            case TRACE:
            case ALL:
                delegate.trace(msg);
                break;
            default:
                break;
        }
    }

    @Override
    public void log(Level level, Supplier<String> msgSupplier, Throwable thrown) {
        if (isEnabled(level)) {
            logWithThrowable(level, msgSupplier.get(), thrown);
        }
    }

    private void logWithThrowable(Level level, String msg, Throwable thrown) {
        switch (level) {
            case FATAL:
            case ERROR:
                delegate.error(msg, thrown);
                break;
            case WARN:
                delegate.warn(msg, thrown);
                break;
            case INFO:
                delegate.info(msg, thrown);
                break;
            case DEBUG:
                delegate.debug(msg, thrown);
                break;
            case TRACE:
            case ALL:
                delegate.trace(msg, thrown);
                break;
            default:
                break;
        }
    }

    private void logWithParams(Level level, String msg, Object... params) {
        switch (level) {
            case FATAL:
            case ERROR:
                delegate.error(msg, params);
                break;
            case WARN:
                delegate.warn(msg, params);
                break;
            case INFO:
                delegate.info(msg, params);
                break;
            case DEBUG:
                delegate.debug(msg, params);
                break;
            case TRACE:
            case ALL:
                delegate.trace(msg, params);
                break;
            default:
                break;
        }
    }

    private void logSupplier(Level level, Supplier<String> msgSupplier) {
        if (isEnabled(level)) {
            log(level, msgSupplier.get());
        }
    }

    // ---- fatal (mapped to error) ----

    @Override
    public void fatal(Supplier<String> msgSupplier) {
        logSupplier(Level.FATAL, msgSupplier);
    }

    @Override
    public void fatal(Supplier<String> msgSupplier, Throwable thrown) {
        log(Level.FATAL, msgSupplier, thrown);
    }

    @Override
    public void fatal(String msg) {
        log(Level.FATAL, msg);
    }

    @Override
    public void fatal(String msg, Throwable thrown) {
        logWithThrowable(Level.FATAL, msg, thrown);
    }

    @Override
    public void fatal(String msg, Object... params) {
        logWithParams(Level.FATAL, msg, params);
    }

    // ---- error ----

    @Override
    public void error(Supplier<String> msgSupplier) {
        logSupplier(Level.ERROR, msgSupplier);
    }

    @Override
    public void error(Supplier<String> msgSupplier, Throwable thrown) {
        log(Level.ERROR, msgSupplier, thrown);
    }

    @Override
    public void error(String msg) {
        log(Level.ERROR, msg);
    }

    @Override
    public void error(String msg, Throwable thrown) {
        logWithThrowable(Level.ERROR, msg, thrown);
    }

    @Override
    public void error(String msg, Object... params) {
        logWithParams(Level.ERROR, msg, params);
    }

    // ---- warn ----

    @Override
    public void warn(Supplier<String> msgSupplier) {
        logSupplier(Level.WARN, msgSupplier);
    }

    @Override
    public void warn(Supplier<String> msgSupplier, Throwable thrown) {
        log(Level.WARN, msgSupplier, thrown);
    }

    @Override
    public void warn(String msg) {
        log(Level.WARN, msg);
    }

    @Override
    public void warn(String msg, Throwable thrown) {
        logWithThrowable(Level.WARN, msg, thrown);
    }

    @Override
    public void warn(String msg, Object... params) {
        logWithParams(Level.WARN, msg, params);
    }

    // ---- info ----

    @Override
    public void info(Supplier<String> msgSupplier) {
        logSupplier(Level.INFO, msgSupplier);
    }

    @Override
    public void info(Supplier<String> msgSupplier, Throwable thrown) {
        log(Level.INFO, msgSupplier, thrown);
    }

    @Override
    public void info(String msg) {
        log(Level.INFO, msg);
    }

    @Override
    public void info(String msg, Throwable thrown) {
        logWithThrowable(Level.INFO, msg, thrown);
    }

    @Override
    public void info(String msg, Object... params) {
        logWithParams(Level.INFO, msg, params);
    }

    // ---- debug ----

    @Override
    public void debug(Supplier<String> msgSupplier) {
        logSupplier(Level.DEBUG, msgSupplier);
    }

    @Override
    public void debug(Supplier<String> msgSupplier, Throwable thrown) {
        log(Level.DEBUG, msgSupplier, thrown);
    }

    @Override
    public void debug(String msg) {
        log(Level.DEBUG, msg);
    }

    @Override
    public void debug(String msg, Throwable thrown) {
        logWithThrowable(Level.DEBUG, msg, thrown);
    }

    @Override
    public void debug(String msg, Object... params) {
        logWithParams(Level.DEBUG, msg, params);
    }

    // ---- trace ----

    @Override
    public void trace(Supplier<String> msgSupplier) {
        logSupplier(Level.TRACE, msgSupplier);
    }

    @Override
    public void trace(Supplier<String> msgSupplier, Throwable thrown) {
        log(Level.TRACE, msgSupplier, thrown);
    }

    @Override
    public void trace(String msg) {
        log(Level.TRACE, msg);
    }

    @Override
    public void trace(String msg, Throwable thrown) {
        logWithThrowable(Level.TRACE, msg, thrown);
    }

    @Override
    public void trace(String msg, Object... params) {
        logWithParams(Level.TRACE, msg, params);
    }
}
