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

package org.apache.paimon.fs;

import java.io.IOException;

/** Base stream for wrappers that forward two-phase writes to another FileIO. */
abstract class ForwardingTwoPhaseOutputStream extends TwoPhaseOutputStream {

    private final TwoPhaseOutputStream delegate;

    ForwardingTwoPhaseOutputStream(TwoPhaseOutputStream delegate) {
        this.delegate = delegate;
    }

    protected abstract Committer wrapCommitter(Committer committer);

    protected <T> T invoke(IOCallable<T> callable) throws IOException {
        return callable.call();
    }

    @Override
    public Committer closeForCommit() throws IOException {
        return wrapCommitter(invoke(delegate::closeForCommit));
    }

    @Override
    public long getPos() throws IOException {
        return invoke(delegate::getPos);
    }

    @Override
    public void write(int value) throws IOException {
        invoke(
                () -> {
                    delegate.write(value);
                    return null;
                });
    }

    @Override
    public void write(byte[] bytes) throws IOException {
        invoke(
                () -> {
                    delegate.write(bytes);
                    return null;
                });
    }

    @Override
    public void write(byte[] bytes, int offset, int length) throws IOException {
        invoke(
                () -> {
                    delegate.write(bytes, offset, length);
                    return null;
                });
    }

    @Override
    public void flush() throws IOException {
        invoke(
                () -> {
                    delegate.flush();
                    return null;
                });
    }

    @Override
    public void close() throws IOException {
        invoke(
                () -> {
                    delegate.close();
                    return null;
                });
    }

    @FunctionalInterface
    protected interface IOCallable<T> {
        T call() throws IOException;
    }
}
