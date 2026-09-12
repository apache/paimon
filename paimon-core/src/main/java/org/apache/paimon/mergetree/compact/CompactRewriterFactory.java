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

package org.apache.paimon.mergetree.compact;

import org.apache.paimon.data.BinaryRow;

/**
 * Creates a compaction rewriter for one primary-key partition and bucket.
 *
 * <p>The supplied rewriter is Paimon's implementation selected for the table's merge engine,
 * changelog producer, and deletion-vector options. A factory may return it unchanged, or wrap it to
 * delegate compactions that its implementation does not support. A replacement must preserve the
 * same records, sequence numbers, changelogs, deletion vectors, record expiration, and file
 * metadata contracts.
 *
 * <p>After a successful call, the returned rewriter owns the supplied rewriter and must close it
 * when closed. If creation fails or returns null, Paimon closes the supplied rewriter. The factory
 * must release any other resources it allocated before failing. Each call must return a rewriter
 * owned exclusively by that bucket; it is closed by Paimon's compaction manager.
 */
@FunctionalInterface
public interface CompactRewriterFactory {

    /**
     * Creates a rewriter before the bucket starts compacting. The partition is an independent copy
     * that may be retained. Capture table schema, options, and file access in the factory as
     * needed.
     */
    CompactRewriter create(BinaryRow partition, int bucket, CompactRewriter defaultRewriter);
}
