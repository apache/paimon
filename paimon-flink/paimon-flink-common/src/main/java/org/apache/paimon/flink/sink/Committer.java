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

package org.apache.paimon.flink.sink;

import org.apache.paimon.manifest.ManifestCommittable;

import java.io.IOException;
import java.util.List;

/**
 * The {@code Committer} is responsible for creating and committing an aggregated committable, which
 * we call committable (see {@link #combine}).
 *
 * <p>The {@code Committer} runs with parallelism equal to 1.
 */
public interface Committer extends AutoCloseable {

    /** Find out which global committables need to be retried when recovering from the failure. */
    List<ManifestCommittable> filterRecoveredCommittables(
            List<ManifestCommittable> globalCommittables) throws IOException;

    /** Compute an aggregated committable from a list of committables. */
    ManifestCommittable combine(long checkpointId, long watermark, List<Committable> committables)
            throws IOException;

    /** Commits the given {@link ManifestCommittable}. */
    void commit(List<ManifestCommittable> globalCommittables)
            throws IOException, InterruptedException;
}
