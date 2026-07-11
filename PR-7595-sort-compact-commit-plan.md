# PR 7595 Sort Compact COMPACT Commit Plan

## Context

This plan is based on the review comment in apache/paimon#7595:

> Sort compaction should not be modeled as an OVERWRITE commit with extra base-snapshot handling. SortCompactAction should produce a COMPACT commit so Commit can run the normal compact validation and conflict checks.

The plan targets current apache/paimon master at `ce92c8a19`.

## Summary

- Refactor Flink `SortCompactAction` and Spark `CompactProcedure` sort compact paths so they produce `CommitKind.COMPACT`, not `OVERWRITE`.
- Keep validation centralized in the existing commit protocol.
- Do not add overwrite base-snapshot APIs or special overwrite conflict logic.
- Preserve current supported mode boundaries: Flink sort compact supports bucket-unaware and hash-dynamic paths; Spark sort compact remains bucket-unaware append-only only.

## Key Changes

- Add an internal core helper to convert sort compact write results into compact commit messages.
- Helper inputs:
  - table
  - base snapshot id
  - compact input `DataSplit`s
  - written `CommitMessage`s
- Helper behavior:
  - Group old files from planned splits by partition/bucket as `compactBefore`.
  - Move newly written data files from `newFilesIncrement.newFiles()` to `compactAfter`.
  - Preserve index-file changes by moving written index increments into the compact increment.
  - For deletion-vector tables, mirror existing `AppendCompactTask` DV cleanup so removed old files also remove old DV index entries.
  - Emit `CommitMessageImpl(..., DataIncrement.emptyIncrement(), CompactIncrement(...))`.
- Keep this helper internal; no public `FileStoreCommit`, `InnerTableCommit`, or `BatchWriteBuilder` API changes.

## Flink Implementation

- In `SortCompactAction`, capture the base snapshot and plan before building the source:
  - Use `table.newSnapshotReader()` with the same partition predicate.
  - Keep the returned snapshot id and `dataSplits`.
  - For empty tables or no splits, keep sort compact as a no-op job with no commit.
- Pin the Flink source to the captured snapshot by using a copied table with:
  - `write-only=true`
  - `scan.snapshot-id=<baseSnapshotId>` when a base snapshot exists
- Remove `.overwrite()` from the sort compact sink path.
- Extend `SortCompactSinkBuilder` to build dedicated sort compact sinks for:
  - bucket-unaware append table
  - hash-dynamic table
- Add a `SortCompactCommitter` or equivalent internal committer wrapper for this topology only:
  - Rewrite written append committables through the shared helper before calling `TableCommitImpl.commitMultiple`.
  - Do not affect normal append, overwrite, or existing compact sinks.

## Spark Implementation

- In `sortCompactUnAwareBucketTable`, keep the existing `SnapshotReader` planning and packed split logic, but stop using `writer.writeBuilder().withOverwrite()`.
- Write sorted rows using `PaimonSparkWriter.apply(table).writeOnly()` so the write stage only creates new files.
- Convert returned write commit messages into compact commit messages with the shared helper, using the same planned base snapshot/data splits.
- Commit rewritten messages with `writer.commit(...)`, producing a `COMPACT` snapshot.
- Preserve current restrictions: Spark sort compact remains bucket-unaware append-only only, and Data Evolution remains unsupported.

## Tests

- Core unit test for the helper:
  - Rewrites append messages into compact messages with expected `compactBefore`, `compactAfter`, and empty data increment.
  - Handles multiple partitions/buckets.
  - Covers deletion-vector index cleanup if feasible with existing test utilities.
- Flink IT tests:
  - Update or add assertions in `SortCompactActionForAppendTableITCase` that sort compact latest snapshot kind is `COMPACT`.
  - Add the same assertion for `SortCompactActionForDynamicBucketITCase`.
  - Add a no-data case confirming no overwrite snapshot is produced.
- Spark tests:
  - Change existing `CompactProcedureTestBase` assertion for sort compact with partition filter from `OVERWRITE` to `COMPACT`.
  - Add or extend sort compact tests to assert latest snapshot kind is `COMPACT`.
- Regression scenario:
  - Simulate sort compact from snapshot S0.
  - Append new data at S1 before compact commit.
  - Commit compact messages from S0.
  - Assert S1 data remains visible and the sort compact snapshot is `COMPACT`.

## Verification

Run targeted tests first:

```shell
mvn -pl paimon-core -Pfast-build -Dtest=SortCompactCommitMessageRewriterTest test
mvn -pl paimon-flink/paimon-flink-common -Pfast-build -Dtest=SortCompactActionForAppendTableITCase test
mvn -pl paimon-flink/paimon-flink-common -Pfast-build -Dtest=SortCompactActionForDynamicBucketITCase test
mvn -pl paimon-spark/paimon-spark-ut -am -Pfast-build -DfailIfNoTests=false \
  -DwildcardSuites=org.apache.paimon.spark.procedure.CompactProcedureTest \
  -Dtest=none test
```

Final compile check for touched modules:

```shell
mvn -pl paimon-core,paimon-flink/paimon-flink-common,paimon-spark/paimon-spark-common \
  -DskipTests compile
```

## Assumptions

- Scope is Flink plus Spark sort compact.
- No overwrite commit protocol changes are allowed for this fix.
- Existing unsupported modes remain unsupported; this PR only changes currently supported sort compact paths.
