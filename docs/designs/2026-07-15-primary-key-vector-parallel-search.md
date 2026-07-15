<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Primary-Key Vector Parallel Search Design

## Problem Statement

`PrimaryKeyVectorRead` searches bucket splits serially, and
`PrimaryKeyVectorBucketSearch` waits for each ANN segment before starting the next one. A query
over multiple buckets or segments therefore accumulates their local-search latency.

## Chosen Approach

Use the existing global-index read executor and `global-index.thread-num` to compose bucket,
segment, and uncovered-file searches with `CompletableFuture`. Submit only leaf searches to the
executor and wait once after all searches have been started. This avoids blocking an executor task
while it waits for more work submitted to the same executor.

## Design Details

### Asynchronous Search

- Add asynchronous ANN segment search while retaining synchronous wrappers for compatibility.
- Compose all segment futures within a bucket instead of joining each segment immediately.
- Compose all bucket futures in `PrimaryKeyVectorRead` and preserve the existing deterministic
  global Top-K merge.
- Submit FULL and DETAIL exact searches for uncovered files as independent leaf tasks.

### Resource and Error Handling

- Attach index-reader closure directly to the future returned by the index reader.
- Close exact-search readers within their leaf tasks.
- Do not submit additional work from completion callbacks.
- Preserve the existing top-level `Failed to search primary-key vector index.` error contract.

### Compatibility

- Keep JDK 8 syntax and existing synchronous search entry points.
- Do not add a new option or executor.
- Keep bucket preparation, deletion-vector loading, residual-filter evaluation, reranking, and
  result ordering unchanged.

### Verification

- Prove multiple ANN segment searches can be in flight together.
- Prove multiple bucket searches are composed concurrently and merge deterministically.
- Prove FULL and DETAIL exact fallback searches uncovered files concurrently.
- Verify exceptional completion closes readers and is propagated.
- Verify `global-index.thread-num=1` completes without deadlock.

## Open Questions

None.

## Out of Scope

- Fair scheduling across buckets when a semaphore-limited executor blocks submissions.
- Parallel bucket metadata, deletion-vector, residual-filter, or rerank reads.
- Changes to `GlobalIndexReadThreadPool` or its executor wrappers.
