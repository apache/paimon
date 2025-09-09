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

package org.apache.paimon.io;

import org.apache.paimon.fs.CommittablePositionOutputStream;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * A wrapper class that contains both the file results and the committers from RollingFileWriter.
 *
 * @param <R> the type of the file results
 */
public class RollingFileResult<R> {

    private final List<R> results;
    private final List<CommittablePositionOutputStream.Committer> committers;

    public RollingFileResult(
            List<R> results, List<CommittablePositionOutputStream.Committer> committers) {
        this.results =
                Collections.unmodifiableList(
                        new ArrayList<>(
                                Objects.requireNonNull(results, "results must not be null")));
        this.committers =
                Collections.unmodifiableList(
                        new ArrayList<>(
                                Objects.requireNonNull(committers, "committers must not be null")));
    }

    /** Create a RollingFileResult without committers for regular files. */
    public static <R> RollingFileResult<R> forRegularFiles(List<R> results) {
        return new RollingFileResult<>(results, Collections.emptyList());
    }

    /** Get the file results. */
    public List<R> getResults() {
        return results;
    }

    /** Get the committers. */
    public List<CommittablePositionOutputStream.Committer> getCommitters() {
        return committers;
    }

    /** Check if this result has any committers. */
    public boolean hasCommitters() {
        return !committers.isEmpty();
    }

    /** Get the number of files written. */
    public int getFileCount() {
        return results.size();
    }

    /** Get the number of committers. */
    public int getCommitterCount() {
        return committers.size();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RollingFileResult<?> that = (RollingFileResult<?>) o;
        return Objects.equals(results, that.results) && Objects.equals(committers, that.committers);
    }

    @Override
    public int hashCode() {
        return Objects.hash(results, committers);
    }

    @Override
    public String toString() {
        return "RollingFileResult{"
                + "fileCount="
                + getFileCount()
                + ", committerCount="
                + getCommitterCount()
                + '}';
    }
}
