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

package org.apache.paimon.table.sink;

import org.apache.paimon.Snapshot;
import org.apache.paimon.manifest.ManifestCommittable;
import org.apache.paimon.manifest.ManifestEntry;

import java.util.List;

/**
 * This callback will be called after a list of {@link ManifestCommittable}s is committed.
 *
 * <p>As a usage example, we can extract what partitions are created from the {@link
 * ManifestCommittable}s and add these partitions into Hive metastore.
 *
 * <p>NOTE: To guarantee that this callback is called, if a failure occurs right after the commit,
 * this callback might be called multiple times. Please make sure that your implementation is
 * idempotent. That is, your callback can be called multiple times and still have the desired
 * effect.
 */
public interface CommitCallback extends AutoCloseable {

    void call(List<ManifestEntry> committedEntries, Snapshot snapshot);

    void retry(ManifestCommittable committable);
}
